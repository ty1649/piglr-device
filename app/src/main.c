/*
INSERT INFORMATION HERE

TERRY YU 2026
 */

#include <stdio.h>
#include <string.h>

#include <zephyr/kernel.h>
#include <zephyr/logging/log.h>
#include <dk_buttons_and_leds.h>

#include <modem/nrf_modem_lib.h>
#include <nrf_modem_at.h>
#include <modem/lte_lc.h>

#include <net/mqtt_helper.h>
#include <date_time.h>
#include <modem/modem_key_mgmt.h>

LOG_MODULE_REGISTER(Amulet_MQTT_GoldenLoop, LOG_LEVEL_INF);

/* ===== Cadence ===== */
#define POLL_INTERVAL_S   (5 * 60)
#define POLL_WINDOW_S     15
#define GNSS_INTERVAL_S   (3 * 60 * 60)

/* ===== Subscription IDs ===== */
#define SUBSCRIBE_TOPIC_ID 1234

/* ===== Misc ===== */
#define IMEI_LEN 15
#define CGSN_RESPONSE_LENGTH (IMEI_LEN + 6 + 1) /* \r\nOK\r\n + \0 */
#define CLIENT_ID_LEN (sizeof("nrf-") + IMEI_LEN) /* includes \0 */

#define TOPIC_BUF_LEN 128

/* ===== Your CA cert included as a header blob ===== */
static const unsigned char ca_certificate[] = {
#include "ca-cert.pem"
};

/* ===== Semaphores / state flags ===== */
static K_SEM_DEFINE(lte_connected, 0, 1);
static K_SEM_DEFINE(mqtt_conn_sem, 0, 1);
static K_SEM_DEFINE(mqtt_sub_sem, 0, 1);

static atomic_t mqtt_connected;

/* If user presses bump while disconnected/offline, queue it for next cycle */
static atomic_t bump_send_queued;

static uint8_t client_id[CLIENT_ID_LEN];

/* ===== Topics (built at runtime) ===== */
static char topic_bump_me[TOPIC_BUF_LEN];
static char topic_loc_peer[TOPIC_BUF_LEN];
static char topic_loc_me[TOPIC_BUF_LEN];
static char topic_bump_peer[TOPIC_BUF_LEN];
static char topic_status_me[TOPIC_BUF_LEN];

/* ===== Schedules / counters (v1: RAM only) ===== */
static int64_t next_gnss_due_utc_s; /* epoch seconds, set after first time sync */
static uint32_t loc_seq;
static uint32_t bump_id;

/* ===== Forward decls ===== */
static void subscribe_topics(void);
static int publish_text_qos1_nonretained(const char *topic, const uint8_t *data, size_t len);
static int publish_location_stub(void);
static int ensure_time_valid(int timeout_s);
static int64_t now_utc_s(void);

/* ===== LTE handler ===== */
static const char *reg_status_str(enum lte_lc_nw_reg_status s)
{
    switch (s) {
    case LTE_LC_NW_REG_NOT_REGISTERED:       return "NOT_REGISTERED";
    case LTE_LC_NW_REG_REGISTERED_HOME:      return "REGISTERED_HOME";
    case LTE_LC_NW_REG_SEARCHING:            return "SEARCHING";
    case LTE_LC_NW_REG_REGISTRATION_DENIED:  return "DENIED";
    case LTE_LC_NW_REG_UNKNOWN:              return "UNKNOWN";
    case LTE_LC_NW_REG_REGISTERED_ROAMING:   return "ROAMING";
    case LTE_LC_NW_REG_UICC_FAIL:            return "UICC_FAIL";
    default:                                 return "??";
    }
}

static void lte_handler(const struct lte_lc_evt *const evt)
{
    switch (evt->type) {
    case LTE_LC_EVT_NW_REG_STATUS:
        LOG_INF("NW_REG_STATUS: %s (%d)",
            reg_status_str(evt->nw_reg_status),
            evt->nw_reg_status);

        if ((evt->nw_reg_status == LTE_LC_NW_REG_REGISTERED_HOME) ||
            (evt->nw_reg_status == LTE_LC_NW_REG_REGISTERED_ROAMING)) {
            k_sem_give(&lte_connected);
        }
        break;

    case LTE_LC_EVT_RRC_UPDATE:
        LOG_INF("RRC mode: %s",
            evt->rrc_mode == LTE_LC_RRC_MODE_CONNECTED ? "Connected" : "Idle");
        break;

    default:
        break;
    }
}

/* ===== Provision CA into modem ===== */
static int certificate_provision(void)
{
    int err;
    bool exists = false;

    err = modem_key_mgmt_exists(CONFIG_MQTT_HELPER_SEC_TAG,
                    MODEM_KEY_MGMT_CRED_TYPE_CA_CHAIN,
                    &exists);
    if (err) {
        LOG_ERR("modem_key_mgmt_exists err=%d", err);
        return err;
    }

    if (exists) {
        err = modem_key_mgmt_cmp(CONFIG_MQTT_HELPER_SEC_TAG,
                     MODEM_KEY_MGMT_CRED_TYPE_CA_CHAIN,
                     ca_certificate,
                     sizeof(ca_certificate));
        LOG_INF("CA credentials: %s", err ? "Mismatch (will rewrite)" : "Match");
        if (!err) {
            return 0;
        }
    }

    err = modem_key_mgmt_write(CONFIG_MQTT_HELPER_SEC_TAG,
                   MODEM_KEY_MGMT_CRED_TYPE_CA_CHAIN,
                   ca_certificate,
                   sizeof(ca_certificate));
    if (err) {
        LOG_ERR("modem_key_mgmt_write(CA) err=%d", err);
        return err;
    }

    LOG_INF("Provisioned CA certificate into modem (sec_tag=%d)", CONFIG_MQTT_HELPER_SEC_TAG);
    return 0;
}

/* ===== Modem bring-up + LTE connect (initial) ===== */
static int modem_configure_and_connect_lte(void)
{
    int err;

    LOG_INF("Initializing modem library...");
    err = nrf_modem_lib_init();
    if (err) {
        LOG_ERR("nrf_modem_lib_init err=%d", err);
        return err;
    }

    /* Provision CA cert while offline (good practice) */
    err = certificate_provision();
    if (err) {
        LOG_ERR("certificate_provision err=%d", err);
        return err;
    }

    LOG_INF("Connecting to LTE...");
    k_sem_reset(&lte_connected);
    err = lte_lc_connect_async(lte_handler);
    if (err) {
        LOG_ERR("lte_lc_connect_async err=%d", err);
        return err;
    }

    if (k_sem_take(&lte_connected, K_MINUTES(3)) != 0) {
        LOG_ERR("LTE registration timeout");
        return -ETIMEDOUT;
    }
    LOG_INF("LTE connected");
    dk_set_led_on(DK_LED2);
    return 0;
}

/* ===== Client ID ===== */
static int client_id_get(char *buffer, size_t buffer_len)
{
    int len;
    int err;
    char imei_buf[CGSN_RESPONSE_LENGTH];

    if (!buffer || buffer_len == 0) {
        return -EINVAL;
    }

    if (strlen(CONFIG_MQTT_SAMPLE_CLIENT_ID) > 0) {
        len = snprintk(buffer, buffer_len, "%s", CONFIG_MQTT_SAMPLE_CLIENT_ID);
        if ((len < 0) || (len >= buffer_len)) {
            return -EMSGSIZE;
        }
        return 0;
    }

    err = nrf_modem_at_cmd(imei_buf, sizeof(imei_buf), "AT+CGSN");
    if (err) {
        LOG_ERR("AT+CGSN err=%d", err);
        return err;
    }
    imei_buf[IMEI_LEN] = '\0';

    len = snprintk(buffer, buffer_len, "nrf-%.*s", IMEI_LEN, imei_buf);
    if ((len < 0) || (len >= buffer_len)) {
        return -EMSGSIZE;
    }
    return 0;
}

/* ===== Build topics ===== */
static void topics_build(void)
{
    /* v1 schema:
     *   pairs/<pair_id>/bump/<device_id>
     *   pairs/<pair_id>/loc/<device_id>
     *   pairs/<pair_id>/status/<device_id> (optional)
     */
    snprintk(topic_bump_me, sizeof(topic_bump_me),
         "pairs/%s/bump/%s", CONFIG_PAIR_ID, CONFIG_MY_ID);

    snprintk(topic_bump_peer, sizeof(topic_bump_peer),
         "pairs/%s/bump/%s", CONFIG_PAIR_ID, CONFIG_PEER_ID);

    snprintk(topic_loc_me, sizeof(topic_loc_me),
         "pairs/%s/loc/%s", CONFIG_PAIR_ID, CONFIG_MY_ID);

    snprintk(topic_loc_peer, sizeof(topic_loc_peer),
         "pairs/%s/loc/%s", CONFIG_PAIR_ID, CONFIG_PEER_ID);

    snprintk(topic_status_me, sizeof(topic_status_me),
         "pairs/%s/status/%s", CONFIG_PAIR_ID, CONFIG_MY_ID);

    LOG_INF("Topics:");
    LOG_INF("  bump_me   = %s", topic_bump_me);
    LOG_INF("  bump_peer = %s", topic_bump_peer);
    LOG_INF("  loc_me    = %s", topic_loc_me);
    LOG_INF("  loc_peer  = %s", topic_loc_peer);
    LOG_INF("  status_me = %s", topic_status_me);
}

/* ===== MQTT callbacks ===== */
static void on_mqtt_connack(enum mqtt_conn_return_code return_code, bool session_present)
{
    ARG_UNUSED(session_present);

    if (return_code == MQTT_CONNECTION_ACCEPTED) {
        LOG_INF("MQTT connected (host=%s, client_id=%s)",
            CONFIG_MQTT_SAMPLE_BROKER_HOSTNAME, (char *)client_id);

        atomic_set(&mqtt_connected, 1);
        k_sem_give(&mqtt_conn_sem);

        /* Subscribe immediately (bump + peer loc) */
        subscribe_topics();
    } else {
        LOG_WRN("MQTT conn rejected rc=%d", return_code);
        atomic_set(&mqtt_connected, 0);
        k_sem_give(&mqtt_conn_sem);
    }
}

static void on_mqtt_suback(uint16_t message_id, int result)
{
    if (result == MQTT_SUBACK_FAILURE) {
        LOG_ERR("SUBACK failure (id=%u)", message_id);
        k_sem_give(&mqtt_sub_sem);
        return;
    }

    if (message_id == SUBSCRIBE_TOPIC_ID) {
        LOG_INF("Subscribed OK (id=%u)", message_id);
        k_sem_give(&mqtt_sub_sem);
        return;
    }

    LOG_WRN("SUBACK unknown id=%u result=%d", message_id, result);
}

static void on_mqtt_publish(struct mqtt_helper_buf topic, struct mqtt_helper_buf payload)
{
    LOG_INF("RX topic=%.*s payload=%.*s",
        topic.size, topic.ptr, payload.size, payload.ptr);

    /* v1: You’ll parse JSON here.
     * For now we just log bumps/location and you can add dedupe later.
     */
}

static void on_mqtt_disconnect(int result)
{
    ARG_UNUSED(result);
    LOG_INF("MQTT disconnected");
    atomic_set(&mqtt_connected, 0);
}

/* ===== Subscribe to bump mailbox + peer location ===== */
static void subscribe_topics(void)
{
    int err;

    struct mqtt_topic topics[2] = {
        {
            .topic = {.utf8 = topic_bump_me, .size = strlen(topic_bump_me)},
            .qos = MQTT_QOS_1_AT_LEAST_ONCE
        },
        {
            .topic = {.utf8 = topic_loc_peer, .size = strlen(topic_loc_peer)},
            .qos = MQTT_QOS_0_AT_MOST_ONCE
        }
    };

    struct mqtt_subscription_list list = {
        .list = topics,
        .list_count = ARRAY_SIZE(topics),
        .message_id = SUBSCRIBE_TOPIC_ID
    };

    LOG_INF("Subscribing bump + peer loc...");
    err = mqtt_helper_subscribe(&list);
    if (err) {
        LOG_ERR("mqtt_helper_subscribe err=%d", err);
        /* Unblock waiter so main can handle as failure */
        k_sem_give(&mqtt_sub_sem);
    }
}

/* ===== Publish helper: QoS1 non-retained (bump/status) ===== */
static int publish_text_qos1_nonretained(const char *topic, const uint8_t *data, size_t len)
{
    int err;
    struct mqtt_publish_param p = {0};

    p.message.payload.data = (uint8_t *)data;
    p.message.payload.len = len;
    p.message.topic.qos = MQTT_QOS_1_AT_LEAST_ONCE;
    p.message_id = mqtt_helper_msg_id_get();
    p.message.topic.topic.utf8 = topic;
    p.message.topic.topic.size = strlen(topic);
    p.dup_flag = 0;
    p.retain_flag = 0;

    err = mqtt_helper_publish(&p);
    if (err) {
        LOG_WRN("publish err=%d topic=%s", err, topic);
        return err;
    }

    LOG_INF("TX topic=%s payload=%.*s", topic, (int)len, data);
    return 0;
}

/* ===== Time helpers ===== */
static int64_t now_utc_s(void)
{
    int64_t now_ms = 0;
    if (date_time_now(&now_ms) == 0) {
        return now_ms / 1000;
    }
    return 0;
}

static int ensure_time_valid(int timeout_s)
{
    /* Kick async update; if already valid, this is cheap */
    (void)date_time_update_async(NULL);

    for (int i = 0; i < timeout_s; i++) {
        if (date_time_is_valid()) {
            return 0;
        }
        k_sleep(K_SECONDS(1));
    }
    return -ETIMEDOUT;
}

/* ===== GNSS “due” (stub) ===== */
static bool gnss_is_due(void)
{
    int64_t now = now_utc_s();
    return (now > 0) && (now >= next_gnss_due_utc_s);
}

/* Publish retained location later; for now just send a status ping and bump if queued */
static int publish_location_stub(void)
{
    /* TODO: Replace with real GNSS + retained QoS0 publish to topic_loc_me */
    char msg[96];
    int64_t now = now_utc_s();

    int n = snprintk(msg, sizeof(msg),
             "{\"type\":\"loc_stub\",\"dev\":\"%s\",\"seq\":%u,\"pub_time_utc\":%lld}",
             CONFIG_MY_ID, loc_seq++, (long long)now);
    if (n < 0 || n >= (int)sizeof(msg)) {
        return -EMSGSIZE;
    }

    /* For now, publish this as status (QoS1). When you implement location,
     * you’ll publish retained QoS0 to topic_loc_me.
     */
    return publish_text_qos1_nonretained(topic_status_me, (uint8_t *)msg, strlen(msg));
}

/* ===== Button handler ===== */
static void button_handler(uint32_t button_state, uint32_t has_changed)
{
    /* Button 1 => queue a bump */
    if ((has_changed & DK_BTN1_MSK) && (button_state & DK_BTN1_MSK)) {
        LOG_INF("BTN1 pressed: queue bump for next poll window");
        atomic_set(&bump_send_queued, 1);
    }
}

/* ===== Main ===== */
int main(void)
{
    int err;

    err = dk_leds_init();
    if (err) {
        LOG_ERR("dk_leds_init err=%d", err);
        return 0;
    }

    err = dk_buttons_init(button_handler);
    if (err) {
        LOG_ERR("dk_buttons_init err=%d", err);
        return 0;
    }

    topics_build();

    err = modem_configure_and_connect_lte();
    if (err) {
        LOG_ERR("LTE bring-up failed err=%d", err);
        return 0;
    }

    /* MQTT helper init */
    struct mqtt_helper_cfg cfg = {
        .cb = {
            .on_connack = on_mqtt_connack,
            .on_disconnect = on_mqtt_disconnect,
            .on_publish = on_mqtt_publish,
            .on_suback = on_mqtt_suback,
        },
    };

    err = mqtt_helper_init(&cfg);
    if (err) {
        LOG_ERR("mqtt_helper_init err=%d", err);
        return 0;
    }

    err = client_id_get((char *)client_id, sizeof(client_id));
    if (err) {
        LOG_ERR("client_id_get err=%d", err);
        return 0;
    }

    struct mqtt_helper_conn_params conn_params = {
        .hostname.ptr = CONFIG_MQTT_SAMPLE_BROKER_HOSTNAME,
        .hostname.size = strlen(CONFIG_MQTT_SAMPLE_BROKER_HOSTNAME),

        .device_id.ptr = (char *)client_id,
        .device_id.size = strlen((char *)client_id),

        .user_name.ptr = CONFIG_LR_MQTT_USERNAME,
        .user_name.size = strlen(CONFIG_LR_MQTT_USERNAME),

        .password.ptr = CONFIG_LR_MQTT_PASSWORD,
        .password.size = strlen(CONFIG_LR_MQTT_PASSWORD),
    };

    /* Initialize schedules once time is valid */
    next_gnss_due_utc_s = 0;
    loc_seq = 0;
    bump_id = 0;

    while (1) {
        /* Ensure time is valid for TLS + UTC fields */
        err = ensure_time_valid(30);
        if (err) {
            LOG_WRN("Time not valid (err=%d). Sleeping then retry.", err);
            goto cycle_sleep;
        }

        if (next_gnss_due_utc_s == 0) {
            /* First time: schedule GNSS for “soon-ish” or immediately */
            next_gnss_due_utc_s = now_utc_s(); /* run GNSS stub on first successful cycle */
        }

        /* Connect MQTT */
        k_sem_reset(&mqtt_conn_sem);
        k_sem_reset(&mqtt_sub_sem);
        atomic_set(&mqtt_connected, 0);

        err = mqtt_helper_connect(&conn_params);
        if (err) {
            LOG_ERR("mqtt_helper_connect err=%d", err);
            goto cycle_sleep;
        }

        /* Wait for CONNACK */
        if (k_sem_take(&mqtt_conn_sem, K_SECONDS(30)) != 0 || !atomic_get(&mqtt_connected)) {
            LOG_WRN("MQTT connect timeout/fail");
            mqtt_helper_disconnect();
            goto cycle_sleep;
        }

        /* Wait for SUBACK (subscribe called in on_connack) */
        (void)k_sem_take(&mqtt_sub_sem, K_SECONDS(10));

        /* Poll window (receive bumps + retained peer loc) */
        LOG_INF("Poll window %ds", POLL_WINDOW_S);
        k_sleep(K_SECONDS(POLL_WINDOW_S));

        /* If bump queued (button pressed), send bump now */
        if (atomic_cas(&bump_send_queued, 1, 0)) {
            char bump_msg[96];
            int64_t now = now_utc_s();

            int n = snprintk(bump_msg, sizeof(bump_msg),
                     "{\"from\":\"%s\",\"bump_id\":%u,\"time_utc\":%lld}",
                     CONFIG_MY_ID, bump_id++, (long long)now);
            if (n > 0 && n < (int)sizeof(bump_msg)) {
                (void)publish_text_qos1_nonretained(topic_bump_peer,
                                    (uint8_t *)bump_msg,
                                    strlen(bump_msg));
            }
        }

        /* Conditional GNSS (stub for now) */
        if (gnss_is_due()) {
            LOG_INF("GNSS due (stub). Would get fix + publish retained loc.");
            (void)publish_location_stub();
            next_gnss_due_utc_s = now_utc_s() + GNSS_INTERVAL_S;
        }

        /* Disconnect */
        mqtt_helper_disconnect();

cycle_sleep:
        /* Go offline between polls for battery (baseline approach) */
        (void)lte_lc_offline();
        dk_set_led_off(DK_LED2);

        LOG_INF("Sleeping %ds...", POLL_INTERVAL_S);
        k_sleep(K_SECONDS(POLL_INTERVAL_S));

        /* Reconnect LTE for next cycle */
        LOG_INF("Reconnecting LTE...");
        k_sem_reset(&lte_connected);
        err = lte_lc_connect_async(lte_handler);
        if (err) {
            LOG_ERR("lte_lc_connect_async err=%d", err);
            /* If this fails, just wait and try again next loop */
            continue;
        }
        k_sem_take(&lte_connected, K_FOREVER);
        dk_set_led_on(DK_LED2);
    }
}
