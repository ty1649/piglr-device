/*
PIGLR

Terry Yu — 2026
*/

#include <stdio.h>
#include <string.h>
#include <math.h>

#include <zephyr/kernel.h>
#include <zephyr/logging/log.h>
#include <zephyr/settings/settings.h>

#include <dk_buttons_and_leds.h>

#include <modem/nrf_modem_lib.h>
#include <nrf_modem_at.h>
#include <modem/lte_lc.h>

#include <net/mqtt_helper.h>
#include <date_time.h>
#include <modem/modem_key_mgmt.h>

#include <errno.h>
#include <stdbool.h>
#include <nrf_modem_gnss.h>

#include <cJSON.h>

LOG_MODULE_REGISTER(PIGLR, LOG_LEVEL_INF);

/* ===== Timing / Cadence ===== */
#define POLL_INTERVAL_S          (4 * 60)
#define POLL_WINDOW_S            15
#define GNSS_INTERVAL_S          (3 * 60 * 60)
#define GNSS_TIMEOUT_S           180
#define GNSS_RETRY_INTERVAL_S    (15 * 60)
#define LTE_QUIET_WAIT_S         70
#define LTE_READY_TIMEOUT_S      180
#define MQTT_CONN_TIMEOUT_S      30
#define MQTT_SUB_TIMEOUT_S       10
#define TIME_SYNC_TIMEOUT_S      30

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
static K_SEM_DEFINE(gnss_fix_sem, 0, 1);

static atomic_t mqtt_connected;
/* If user presses bump while disconnected, queue it for next cycle */
static atomic_t bump_send_queued;

struct piglr_fix {
	double lat;
	double lon;
	uint32_t acc_m;
	int64_t fix_time_utc;
	bool valid;
};
static struct piglr_fix g_fix_candidate;
static atomic_t g_gnss_running;

struct piglr_peer_state {
	double lat;
	double lon;
	uint32_t acc_m;
	uint32_t seq;
	int64_t fix_time_utc;
	int64_t pub_time_utc;
	int64_t last_rx_time_utc;
	bool valid;
};
static struct piglr_peer_state g_peer_loc;

struct piglr_local_state {
	double lat;
	double lon;
	uint32_t acc_m;
	uint32_t seq;
	int64_t fix_time_utc;
	int64_t pub_time_utc;
	bool valid;
};
static struct piglr_local_state g_local_loc;

static double g_mock_heading_deg = 0.0;

static uint8_t client_id[CLIENT_ID_LEN];

/* ===== Topics (built at runtime) ===== */
static char topic_bump_me[TOPIC_BUF_LEN];
static char topic_loc_peer[TOPIC_BUF_LEN];
static char topic_loc_me[TOPIC_BUF_LEN];
static char topic_bump_peer[TOPIC_BUF_LEN];
// static char topic_status_me[TOPIC_BUF_LEN];

/* ===== Persistent state (Settings) =====
 * These survive reset/power-cycle when CONFIG_SETTINGS_NVS is enabled.
 */
static uint32_t g_loc_seq;
static uint32_t g_bump_id;
static int64_t  g_next_gnss_due_utc_s; /* epoch seconds */
static uint32_t g_last_rx_bump_id;

/* ===== Forward decls ===== */
static void subscribe_topics(void);
static int publish_text_qos1_nonretained(const char *topic, const uint8_t *data, size_t len);
static int publish_location_retained(const struct piglr_fix *fix);
static int ensure_time_valid(int timeout_s);
static int64_t now_utc_s(void);
static int ensure_lte_registered(int timeout_s);
static uint32_t backoff_s(uint32_t fail_count);
static bool handle_peer_location_json(const char *buf, size_t len);
static bool topic_matches(const struct mqtt_helper_buf *topic, const char *expected);
static int mqtt_connect_blocking(struct mqtt_helper_conn_params *conn_params);
static void mqtt_disconnect_clean(void);
static bool topic_matches(const struct mqtt_helper_buf *topic, const char *expected);
static bool handle_peer_location_json(const char *buf, size_t len);
static bool peer_location_is_stale(int64_t stale_threshold_s);
static int64_t peer_location_age_s(void);
static double deg_to_rad(double deg);
static double distance_between_coords_m(double lat1, double lon1, double lat2, double lon2);
static int distance_to_peer_m(uint32_t *distance_m);
// static double normalize_angle_360(double deg);
static double normalize_angle_180(double deg);
static double bearing_between_coords_deg(double lat1, double lon1, double lat2, double lon2);
static int bearing_to_peer_deg(double *bearing_deg);
static int arrow_angle_to_peer_deg(double device_heading_deg, double *arrow_deg);

/* ===== Settings handlers ===== */
static int settings_set_handler(const char *key, size_t len,
                                settings_read_cb read_cb, void *cb_arg)
{
	if (strcmp(key, "loc_seq") == 0) {
		return read_cb(cb_arg, &g_loc_seq, sizeof(g_loc_seq));
	}
	if (strcmp(key, "bump_id") == 0) {
		return read_cb(cb_arg, &g_bump_id, sizeof(g_bump_id));
	}
	if (strcmp(key, "next_gnss_due") == 0) {
		return read_cb(cb_arg, &g_next_gnss_due_utc_s, sizeof(g_next_gnss_due_utc_s));
	}
	if (strcmp(key, "last_rx_bump_id") == 0) {
		return read_cb(cb_arg, &g_last_rx_bump_id, sizeof(g_last_rx_bump_id));
	}
	return -ENOENT;
}

static struct settings_handler app_settings = {
	.name = "piglr",
	.h_set = settings_set_handler,
};

static void persist_u32(const char *key, uint32_t v)
{
	char path[64];
	snprintk(path, sizeof(path), "piglr/%s", key);
	(void)settings_save_one(path, &v, sizeof(v));
}

static void persist_i64(const char *key, int64_t v)
{
	char path[64];
	snprintk(path, sizeof(path), "piglr/%s", key);
	(void)settings_save_one(path, &v, sizeof(v));
}

static int settings_init_and_load(void)
{
	int err = settings_subsys_init();
	if (err) {
		return err;
	}
	err = settings_register(&app_settings);
	if (err) {
		return err;
	}
	err = settings_load_subtree("piglr");
	if (err) {
		return err;
	}

	LOG_INF("Settings loaded: loc_seq=%u bump_id=%u next_gnss_due=%lld last_rx_bump_id=%u",
	        g_loc_seq, g_bump_id, (long long)g_next_gnss_due_utc_s, g_last_rx_bump_id);
	return 0;
}

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

	case LTE_LC_EVT_PSM_UPDATE:
		LOG_INF("PSM granted: TAU=%d s Active=%d s",
				evt->psm_cfg.tau,
				evt->psm_cfg.active_time);
		break;

	default:
		break;
	}
}

/* Ensure registered before MQTT work (handles transient NOT_REGISTERED) */
static int ensure_lte_registered(int timeout_s)
{
	enum lte_lc_nw_reg_status st = LTE_LC_NW_REG_UNKNOWN;
	int err = lte_lc_nw_reg_status_get(&st);
	if (err == 0) {
		if (st == LTE_LC_NW_REG_REGISTERED_HOME || st == LTE_LC_NW_REG_REGISTERED_ROAMING) {
			return 0;
		}
		LOG_WRN("LTE not registered (status=%s). Reconnecting...", reg_status_str(st));
	} else {
		LOG_WRN("lte_lc_nw_reg_status_get err=%d. Reconnecting...", err);
	}

	k_sem_reset(&lte_connected);
	err = lte_lc_connect_async(lte_handler);
	if (err) {
		LOG_ERR("lte_lc_connect_async err=%d", err);
		return err;
	}

	if (k_sem_take(&lte_connected, K_SECONDS(timeout_s)) != 0) {
		LOG_ERR("LTE registration timeout");
		return -ETIMEDOUT;
	}
	return 0;
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
static int modem_init_and_lte_connect(void)
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
	err = ensure_lte_registered(180);
	if (err) {
		return err;
	}

	/* Power pivot: request PSM (network will decide timers) */
	(void)lte_lc_psm_req(true);

	LOG_INF("LTE ready (registered).");
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
		if ((len < 0) || (len >= (int)buffer_len)) {
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
	if ((len < 0) || (len >= (int)buffer_len)) {
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
	 *   pairs/<pair_id>/status/<device_id> (debug)
	 */
	snprintk(topic_bump_me, sizeof(topic_bump_me),
	         "pairs/%s/bump/%s", CONFIG_PAIR_ID, CONFIG_MY_ID);

	snprintk(topic_bump_peer, sizeof(topic_bump_peer),
	         "pairs/%s/bump/%s", CONFIG_PAIR_ID, CONFIG_PEER_ID);

	snprintk(topic_loc_me, sizeof(topic_loc_me),
	         "pairs/%s/loc/%s", CONFIG_PAIR_ID, CONFIG_MY_ID);

	snprintk(topic_loc_peer, sizeof(topic_loc_peer),
	         "pairs/%s/loc/%s", CONFIG_PAIR_ID, CONFIG_PEER_ID);

	// snprintk(topic_status_me, sizeof(topic_status_me),
	//          "pairs/%s/status/%s", CONFIG_PAIR_ID, CONFIG_MY_ID);

	LOG_INF("Topics:");
	LOG_INF("  bump_me   = %s", topic_bump_me);
	LOG_INF("  bump_peer = %s", topic_bump_peer);
	LOG_INF("  loc_me    = %s", topic_loc_me);
	LOG_INF("  loc_peer  = %s", topic_loc_peer);
	// LOG_INF("  status_me = %s", topic_status_me);
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

static bool handle_bump_json(const char *buf, size_t len)
{
	cJSON *root = cJSON_ParseWithLength(buf, len);
	if (!root) {
		LOG_WRN("Bump JSON parse failed");
		return false;
	}

	cJSON *bump_id = cJSON_GetObjectItemCaseSensitive(root, "bump_id");
	if (!cJSON_IsNumber(bump_id)) {
		cJSON_Delete(root);
		LOG_WRN("Bump JSON missing bump_id");
		return false;
	}

	uint32_t id = (uint32_t)bump_id->valuedouble;

	/* Simple monotonic dedupe */
	if (id <= g_last_rx_bump_id) {
		LOG_INF("Bump deduped (id=%u last=%u)", id, g_last_rx_bump_id);
		cJSON_Delete(root);
		return true;
	}

	g_last_rx_bump_id = id;
	persist_u32("last_rx_bump_id", g_last_rx_bump_id);

	LOG_INF("Bump accepted (id=%u) -> TODO haptic/UI", id);

	cJSON_Delete(root);
	return true;
}

static void on_mqtt_publish(struct mqtt_helper_buf topic, struct mqtt_helper_buf payload)
{
	LOG_INF("RX topic=%.*s payload=%.*s",
	        topic.size, topic.ptr, payload.size, payload.ptr);

	/* Parse bumps set to me */
	if (topic_matches(&topic, topic_bump_me)) {
		(void)handle_bump_json(payload.ptr, payload.size);
		return;
	}

	if (topic_matches(&topic, topic_loc_peer)) {
		(void)handle_peer_location_json(payload.ptr, payload.size);
		return;
	}
}

static void on_mqtt_disconnect(int result)
{
	ARG_UNUSED(result);
	LOG_INF("MQTT disconnected");
	atomic_set(&mqtt_connected, 0);
}

static int mqtt_connect_blocking(struct mqtt_helper_conn_params *conn_params)
{
	int err;

	k_sem_reset(&mqtt_conn_sem);
	k_sem_reset(&mqtt_sub_sem);
	atomic_set(&mqtt_connected, 0);

	err = mqtt_helper_connect(conn_params);
	if (err) {
		LOG_ERR("mqtt_helper_connect err=%d", err);
		return err;
	}

	if (k_sem_take(&mqtt_conn_sem, K_SECONDS(MQTT_CONN_TIMEOUT_S)) != 0 ||
	    !atomic_get(&mqtt_connected)) {
		LOG_WRN("MQTT connect timeout/fail");
		mqtt_helper_disconnect();
		return -ETIMEDOUT;
	}

	(void)k_sem_take(&mqtt_sub_sem, K_SECONDS(MQTT_SUB_TIMEOUT_S));
	return 0;
}

static void mqtt_disconnect_clean(void)
{
	mqtt_helper_disconnect();
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

/* Peer Location Stuff */
static bool topic_matches(const struct mqtt_helper_buf *topic, const char *expected)
{
	size_t expected_len = strlen(expected);

	return (topic->size == expected_len) &&
	       (memcmp(topic->ptr, expected, expected_len) == 0);
}

static bool handle_peer_location_json(const char *buf, size_t len)
{
	cJSON *root = cJSON_ParseWithLength(buf, len);
	if (!root) {
		LOG_WRN("Peer loc JSON parse failed");
		return false;
	}

	cJSON *lat = cJSON_GetObjectItemCaseSensitive(root, "lat");
	cJSON *lon = cJSON_GetObjectItemCaseSensitive(root, "lon");
	cJSON *acc_m = cJSON_GetObjectItemCaseSensitive(root, "acc_m");
	cJSON *seq = cJSON_GetObjectItemCaseSensitive(root, "seq");
	cJSON *fix_time_utc = cJSON_GetObjectItemCaseSensitive(root, "fix_time_utc");
	cJSON *pub_time_utc = cJSON_GetObjectItemCaseSensitive(root, "pub_time_utc");

	if (!cJSON_IsNumber(lat) ||
	    !cJSON_IsNumber(lon) ||
	    !cJSON_IsNumber(acc_m) ||
	    !cJSON_IsNumber(seq) ||
	    !cJSON_IsNumber(fix_time_utc) ||
	    !cJSON_IsNumber(pub_time_utc)) {
		LOG_WRN("Peer loc JSON missing required fields");
		cJSON_Delete(root);
		return false;
	}

	g_peer_loc.lat = lat->valuedouble;
	g_peer_loc.lon = lon->valuedouble;
	g_peer_loc.acc_m = (uint32_t)acc_m->valuedouble;
	g_peer_loc.seq = (uint32_t)seq->valuedouble;
	g_peer_loc.fix_time_utc = (int64_t)fix_time_utc->valuedouble;
	g_peer_loc.pub_time_utc = (int64_t)pub_time_utc->valuedouble;
	g_peer_loc.last_rx_time_utc = now_utc_s();
	g_peer_loc.valid = true;

	/* DEBUGS */
	uint32_t distance_m;
	if (distance_to_peer_m(&distance_m) == 0) {
		LOG_INF("Distance to peer: %u m", distance_m);
	} else {
		LOG_INF("Distance to peer unavailable (need valid local + peer fixes)");
	}
	double bearing_deg;
	if (bearing_to_peer_deg(&bearing_deg) == 0) {
		LOG_INF("Bearing to peer: %.1f deg true", bearing_deg);
	} else {
		LOG_INF("Bearing unavailable (need valid local + peer fixes)");
	}
	double arrow_deg;
	if (arrow_angle_to_peer_deg(g_mock_heading_deg, &arrow_deg) == 0) {
		LOG_INF("Arrow to peer (mock heading %.1f): %.1f deg",
		        g_mock_heading_deg, arrow_deg);
	} else {
		LOG_INF("Arrow unavailable");
	}

	LOG_INF("Peer loc updated: lat=%.6f lon=%.6f acc=%u seq=%u fix_age=%llds",
	        g_peer_loc.lat,
	        g_peer_loc.lon,
	        g_peer_loc.acc_m,
	        g_peer_loc.seq,
	        (long long)(g_peer_loc.last_rx_time_utc - g_peer_loc.fix_time_utc));

	cJSON_Delete(root);
	return true;
}

static bool peer_location_is_stale(int64_t stale_threshold_s)
{
	if (!g_peer_loc.valid) {
		return true;
	}

	int64_t now = now_utc_s();
	if (now <= 0) {
		return true;
	}

	return (now - g_peer_loc.fix_time_utc) > stale_threshold_s;
}

static int64_t peer_location_age_s(void)
{
	if (!g_peer_loc.valid) {
		return -1;
	}

	int64_t now = now_utc_s();
	if (now <= 0) {
		return -1;
	}

	return now - g_peer_loc.fix_time_utc;
}

static double deg_to_rad(double deg)
{
	return deg * (3.14159265358979323846 / 180.0);
}

static double distance_between_coords_m(double lat1, double lon1,
					double lat2, double lon2)
{
	const double earth_radius_m = 6371000.0;

	double lat1_rad = deg_to_rad(lat1);
	double lat2_rad = deg_to_rad(lat2);
	double dlat = deg_to_rad(lat2 - lat1);
	double dlon = deg_to_rad(lon2 - lon1);

	double a = sin(dlat / 2.0) * sin(dlat / 2.0) +
		   cos(lat1_rad) * cos(lat2_rad) *
		   sin(dlon / 2.0) * sin(dlon / 2.0);

	double c = 2.0 * atan2(sqrt(a), sqrt(1.0 - a));

	return earth_radius_m * c;
}

static int distance_to_peer_m(uint32_t *distance_m)
{
	if (!distance_m) {
		return -EINVAL;
	}

	if (!g_local_loc.valid || !g_peer_loc.valid) {
		return -ENOENT;
	}

	double d = distance_between_coords_m(
		g_local_loc.lat,
		g_local_loc.lon,
		g_peer_loc.lat,
		g_peer_loc.lon
	);

	if (d < 0.0) {
		return -EINVAL;
	}

	*distance_m = (uint32_t)(d + 0.5); /* round to nearest meter */
	return 0;
}

static double bearing_between_coords_deg(double lat1, double lon1,
					 double lat2, double lon2)
{
	double lat1_rad = deg_to_rad(lat1);
	double lat2_rad = deg_to_rad(lat2);
	double dlon_rad = deg_to_rad(lon2 - lon1);

	double y = sin(dlon_rad) * cos(lat2_rad);
	double x = cos(lat1_rad) * sin(lat2_rad) -
		   sin(lat1_rad) * cos(lat2_rad) * cos(dlon_rad);

	double bearing_rad = atan2(y, x);
	double bearing_deg = bearing_rad * (180.0 / 3.14159265358979323846);

	/* normalize to 0..360 */
	if (bearing_deg < 0.0) {
		bearing_deg += 360.0;
	}

	return bearing_deg;
}

static int bearing_to_peer_deg(double *bearing_deg)
{
	if (!bearing_deg) {
		return -EINVAL;
	}

	if (!g_local_loc.valid || !g_peer_loc.valid) {
		return -ENOENT;
	}

	*bearing_deg = bearing_between_coords_deg(
		g_local_loc.lat,
		g_local_loc.lon,
		g_peer_loc.lat,
		g_peer_loc.lon
	);

	return 0;
}

static double normalize_angle_180(double deg)
{
	while (deg <= -180.0) {
		deg += 360.0;
	}
	while (deg > 180.0) {
		deg -= 360.0;
	}
	return deg;
}

// static double normalize_angle_360(double deg)
// {
// 	while (deg < 0.0) {
// 		deg += 360.0;
// 	}
// 	while (deg >= 360.0) {
// 		deg -= 360.0;
// 	}
// 	return deg;
// }

static int arrow_angle_to_peer_deg(double device_heading_deg, double *arrow_deg)
{
	double bearing_deg;

	if (!arrow_deg) {
		return -EINVAL;
	}

	if (bearing_to_peer_deg(&bearing_deg) != 0) {
		return -ENOENT;
	}

	/* Relative angle: from peer to device clocking */
	*arrow_deg = normalize_angle_180(bearing_deg - device_heading_deg);
	return 0;
}

/* ===== Send queued bumps ===== */
static void send_queued_bump_if_any(void)
{
	if (!atomic_cas(&bump_send_queued, 1, 0)) {
		return;
	}

	char bump_msg[128];
	int64_t now = now_utc_s();
	uint32_t my_bump_id = g_bump_id;

	int n = snprintk(bump_msg, sizeof(bump_msg),
			 "{\"from\":\"%s\",\"bump_id\":%u,\"time_utc\":%lld}",
			 CONFIG_MY_ID, my_bump_id, (long long)now);

	if (n > 0 && n < (int)sizeof(bump_msg)) {
		if (publish_text_qos1_nonretained(topic_bump_peer,
						  (uint8_t *)bump_msg,
						  strlen(bump_msg)) == 0) {
			g_bump_id++;
			persist_u32("bump_id", g_bump_id);
			LOG_INF("Bump sent successfully");
		}
	}
}

/* ===== MQTT publish helpers ===== */
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
	(void)date_time_update_async(NULL);

	for (int i = 0; i < timeout_s; i++) {
		if (date_time_is_valid()) {
			return 0;
		}
		k_sleep(K_SECONDS(1));
	}
	return -ETIMEDOUT;
}

/* ===== GNSS scheduling ===== */
static bool gnss_is_due(void)
{
	int64_t now = now_utc_s();
	return (now > 0) && (now >= g_next_gnss_due_utc_s);
}

static void gnss_event_handler(int event)
{
	int err;

	switch (event) {
	case NRF_MODEM_GNSS_EVT_PVT:
	{
		struct nrf_modem_gnss_pvt_data_frame pvt;

		err = nrf_modem_gnss_read(&pvt, sizeof(pvt), NRF_MODEM_GNSS_DATA_PVT);
		if (err) {
			LOG_WRN("nrf_modem_gnss_read(PVT) err=%d", err);
			return;
		}

		/* Only accept a real fix */
		if ((pvt.flags & NRF_MODEM_GNSS_PVT_FLAG_FIX_VALID) == 0) {
			return;
		}

		g_fix_candidate.lat = pvt.latitude;
		g_fix_candidate.lon = pvt.longitude;

		/* Accuracy field naming can vary slightly by modem/NCS version.
		 * Commonly: pvt.accuracy or pvt.accuracy_horizontal.
		 * Use whichever exists in your SDK.
		 */
#ifdef CONFIG_PIGLR_USE_PVT_ACCURACY_HORIZONTAL
		g_fix_candidate.acc_m = (uint32_t)pvt.accuracy_horizontal;
#else
		g_fix_candidate.acc_m = (uint32_t)pvt.accuracy;
#endif

		g_fix_candidate.fix_time_utc = now_utc_s();
		g_fix_candidate.valid = true;

		k_sem_give(&gnss_fix_sem);
		break;
	}

	case NRF_MODEM_GNSS_EVT_FIX:
		/* Optional: useful debug signal */
		LOG_INF("GNSS EVT_FIX");
		break;

	case NRF_MODEM_GNSS_EVT_BLOCKED:
		LOG_WRN("GNSS blocked by LTE activity");
		break;

	case NRF_MODEM_GNSS_EVT_UNBLOCKED:
		LOG_INF("GNSS unblocked");
		break;

	default:
		break;
	}
}

static int gnss_init_once(void)
{
	static bool inited;
	int err;

	if (inited) {
		return 0;
	}

	// err = nrf_modem_gnss_prio_mode_enable();
	// if (err) {
	// 	LOG_ERR("nrf_modem_gnss_prio_mode_enable err=%d", err);
	// 	return err;
	// }

	err = nrf_modem_gnss_event_handler_set(gnss_event_handler);
	if (err) {
		LOG_ERR("nrf_modem_gnss_event_handler_set err=%d", err);
		return err;
	}

	/* Continuous navigation mode is fine for a timeout-based fix function.
	 * Interval 1 / timeout 0 means continuous searching until stopped.
	 */
	err = nrf_modem_gnss_fix_interval_set(1);
	if (err) {
		LOG_ERR("nrf_modem_gnss_fix_interval_set err=%d", err);
		return err;
	}

	err = nrf_modem_gnss_fix_retry_set(0);
	if (err) {
		LOG_ERR("nrf_modem_gnss_fix_retry_set err=%d", err);
		return err;
	}

	inited = true;
	return 0;
}

static int gnss_get_fix_with_timeout(struct piglr_fix *fix, uint32_t timeout_s)
{
	int err;

	if (!fix) {
		return -EINVAL;
	}

	memset(fix, 0, sizeof(*fix));
	memset(&g_fix_candidate, 0, sizeof(g_fix_candidate));
	k_sem_reset(&gnss_fix_sem);

	err = gnss_init_once();
	if (err) {
		return err;
	}

	err = nrf_modem_gnss_start();
	if (err) {
		LOG_ERR("nrf_modem_gnss_start err=%d", err);
		return err;
	}

	atomic_set(&g_gnss_running, 1);
	LOG_INF("GNSS started, waiting up to %u s for fix", timeout_s);

	err = k_sem_take(&gnss_fix_sem, K_SECONDS(timeout_s));
	if (err == 0 && g_fix_candidate.valid) {
		*fix = g_fix_candidate;
		(void)nrf_modem_gnss_stop();
		atomic_set(&g_gnss_running, 0);
		LOG_INF("GNSS stopped after fix");
		return 0;
	}

	(void)nrf_modem_gnss_stop();
	atomic_set(&g_gnss_running, 0);
	LOG_WRN("GNSS timeout after %u s", timeout_s);
	return -ETIMEDOUT;
}

static int run_gnss_cycle(struct mqtt_helper_conn_params *conn_params)
{
	struct piglr_fix fix = {0};
	int err;

	LOG_INF("Waiting %ds for LTE to go quiet before GNSS...", LTE_QUIET_WAIT_S);
	k_sleep(K_SECONDS(LTE_QUIET_WAIT_S));

	LOG_INF("GNSS due. Attempting fix...");
	err = gnss_get_fix_with_timeout(&fix, GNSS_TIMEOUT_S);
	if (err || !fix.valid) {
		LOG_WRN("GNSS fix failed / timed out");
		g_next_gnss_due_utc_s = now_utc_s() + GNSS_RETRY_INTERVAL_S;
		persist_i64("next_gnss_due", g_next_gnss_due_utc_s);
		return err ? err : -ETIMEDOUT;
	}

	LOG_INF("GNSS fix acquired: lat=%.6f lon=%.6f acc=%u m",
		fix.lat, fix.lon, fix.acc_m);

	err = mqtt_connect_blocking(conn_params);
	if (err) {
		LOG_ERR("MQTT reconnect for loc publish failed err=%d", err);
		return err;
	}

	err = publish_location_retained(&fix);
	mqtt_disconnect_clean();

	if (err) {
		LOG_ERR("Location publish failed err=%d", err);
		return err;
	}

	g_next_gnss_due_utc_s = now_utc_s() + GNSS_INTERVAL_S;
	persist_i64("next_gnss_due", g_next_gnss_due_utc_s);

	LOG_INF("GNSS cycle complete");
	return 0;
}

static int publish_location_retained(const struct piglr_fix *fix)
{
	int err;

	if (!fix || !fix->valid) {
		return -EINVAL;
	}

	cJSON *root = cJSON_CreateObject();
	if (!root) {
		return -ENOMEM;
	}

	int64_t now = now_utc_s();

	cJSON_AddNumberToObject(root, "lat", fix->lat);
	cJSON_AddNumberToObject(root, "lon", fix->lon);
	cJSON_AddNumberToObject(root, "acc_m", fix->acc_m);
	cJSON_AddNumberToObject(root, "seq", g_loc_seq);
	cJSON_AddNumberToObject(root, "fix_time_utc", fix->fix_time_utc);
	cJSON_AddNumberToObject(root, "pub_time_utc", now);

	char *json = cJSON_PrintUnformatted(root);
	if (!json) {
		cJSON_Delete(root);
		return -ENOMEM;
	}

	struct mqtt_publish_param p = {0};

	p.message.payload.data = (uint8_t *)json;
	p.message.payload.len = strlen(json);

	p.message.topic.qos = MQTT_QOS_0_AT_MOST_ONCE;

	p.message_id = mqtt_helper_msg_id_get();

	p.message.topic.topic.utf8 = topic_loc_me;
	p.message.topic.topic.size = strlen(topic_loc_me);

	p.dup_flag = 0;
	p.retain_flag = 1;   /* retained location */

	err = mqtt_helper_publish(&p);

	if (err) {
		LOG_WRN("Location publish failed err=%d", err);
		cJSON_free(json);
		cJSON_Delete(root);
		return err;
	}

	LOG_INF("Piglr TX retained loc topic=%s payload=%s", topic_loc_me, json);

	g_local_loc.lat = fix->lat;
	g_local_loc.lon = fix->lon;
	g_local_loc.acc_m = fix->acc_m;
	g_local_loc.seq = g_loc_seq;
	g_local_loc.fix_time_utc = fix->fix_time_utc;
	g_local_loc.pub_time_utc = now;
	g_local_loc.valid = true;

	/* increment + persist sequence */
	g_loc_seq++;
	persist_u32("loc_seq", g_loc_seq);

	cJSON_free(json);
	cJSON_Delete(root);

	return 0;
}

/* ===== Button handler ===== */
static void button_handler(uint32_t button_state, uint32_t has_changed)
{
	if ((has_changed & DK_BTN1_MSK) && (button_state & DK_BTN1_MSK)) {
		LOG_INF("BTN1 pressed: queue bump for next poll window");
		atomic_set(&bump_send_queued, 1);
	}
}

/* ===== Backoff ===== */
static uint32_t backoff_s(uint32_t fail_count)
{
	if (fail_count == 0) return 60;
	if (fail_count == 1) return 120;
	if (fail_count == 2) return 300;
	if (fail_count == 3) return 600;
	return 900;
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

	/* Build topics before anything else */
	topics_build();

	/* Settings persistence (non-fatal if unavailable) */
	err = settings_init_and_load();
	if (err) {
		LOG_WRN("Settings init/load failed: %d (continuing without persistence)", err);
	}

	/* Modem + LTE register (once); request PSM */
	err = modem_init_and_lte_connect();
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

	/* If never set, schedule GNSS “soon” after first valid time */
	if (g_next_gnss_due_utc_s == 0) {
		g_next_gnss_due_utc_s = 0; /* will be initialized after time valid */
	}

	uint32_t fail_count = 0;

	while (1) {
		/* Make sure we're registered (PSM may be active) */
		err = ensure_lte_registered(LTE_READY_TIMEOUT_S);
		if (err) {
			LOG_WRN("LTE not ready (err=%d)", err);
			fail_count++;
			goto cycle_sleep;
		}

		/* Ensure time is valid for TLS + UTC fields */
		err = ensure_time_valid(TIME_SYNC_TIMEOUT_S);
		if (err) {
			LOG_WRN("Time not valid (err=%d)", err);
			fail_count++;
			goto cycle_sleep;
		}

		if (g_next_gnss_due_utc_s == 0) {
			g_next_gnss_due_utc_s = now_utc_s();
			persist_i64("next_gnss_due", g_next_gnss_due_utc_s);
		}

		/* Connect MQTT */
		err = mqtt_connect_blocking(&conn_params);
		if (err) {
			fail_count++;
			goto cycle_sleep;
		}

		/* Poll window */
		LOG_INF("Poll window %ds", POLL_WINDOW_S);
		k_sleep(K_SECONDS(POLL_WINDOW_S));

		/* If bump queued (button pressed), send bump now */
		send_queued_bump_if_any();

		/* Disconnect MQTT */
		mqtt_disconnect_clean();

		/* Conditional GNSS */
		if (gnss_is_due()) {
			(void)run_gnss_cycle(&conn_params);
		}

		/* Successful cycle */
		fail_count = 0;

		cycle_sleep: {
			/* LED2 indicates modem registered (not necessarily RRC connected) */
			dk_set_led_on(DK_LED2);

			uint32_t sleep_s = (fail_count == 0) ? POLL_INTERVAL_S : backoff_s(fail_count);
			LOG_INF("Sleeping %us... (fail_count=%u)", sleep_s, fail_count);
			k_sleep(K_SECONDS(sleep_s));
		}
	}
}