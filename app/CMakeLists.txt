#
# Copyright (c) 2018 Nordic Semiconductor ASA
#
# SPDX-License-Identifier: LicenseRef-Nordic-5-Clause
#

cmake_minimum_required(VERSION 3.20.0)

find_package(Zephyr REQUIRED HINTS $ENV{ZEPHYR_BASE})
project(cellular_fundamentals)

target_sources(app PRIVATE src/main.c)

if(CONFIG_MODEM_KEY_MGMT)
  # STEP 5 - Generate include files from the certificate
set(gen_dir ${CMAKE_CURRENT_BINARY_DIR}/certs)
zephyr_include_directories(${gen_dir})
generate_inc_file_for_target(app credentials/ca-cert.pem ${gen_dir}/ca-cert.pem)
endif()