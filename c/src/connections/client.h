#pragma once

#include <pthread.h>
#include "../deps/json.hpp"
#include <openssl/bn.h>
#include <openssl/crypto.h>
#include <openssl/dh.h>
#include "../permissions/accounts.h"
#include "../deps/simdjson/simdjson.h"
#include "../deps/rapidjson/document.h"

struct client_socket_data {
    struct version_t {
        int major;
        int minor;
    };

    struct config_t {
        // Removes unnecessary bytes from the element names of all transmissions and
        // shortens them to a single byte equivelant (plus a number if duplicate)
        // in an effort to reduce network traffic.
        // Client sent JSON will also be expected to make use of short attributes.
        bool short_attr = false; // short_attributes

        // Removes the long and detailed 'text' data entry that gets sent with every error
        // since it is largely pointless as the errors can be hardcoded + the relevant code
        // always gets sent. This should be disabled in production and only used during testing.
        bool error_text = true; // error_text
    };

    struct encryption_t {
        bool enabled = false;
        char aes_secret[32];
        char aes_client_iv[16];
        char aes_server_iv[16];
        EVP_CIPHER_CTX* aes_ctx;
    };

    encryption_t encryption;
    pthread_t thread_id;
    int socket_id;
    char address[16];
    config_t config;
    version_t version;  
    uint64_t last_packet_time = 0;
    DatabaseAccount* account;

    simdjson::ondemand::parser parser;
    rapidjson::Document object;
};

namespace errors {
    enum {
        json_invalid,
        packet_size_exceeded,
        overflow_protection_triggered,
        internal,
        params_invalid,
        handshake_config_json_invalid,
        outdated_client_version,
        outdated_server_version,
        invalid_query,
        table_not_found,
        op_invalid,
        op_not_found,
        data_invalid,
        nonce_invalid,
        table_conflict,
        table_already_open,
        table_not_open,
        insufficient_memory,
        invalid_account_credentials,
        too_many_connections,
        traffic_encryption_mandatory,
        account_username_in_use,
        name_reserved,
        value_reserved,
        username_not_found,
        insufficient_privileges
    };

    extern const char* text[];
}

void* client_connection_handle(void* arg);
int send_ka(client_socket_data* socket_data);
void send_res(client_socket_data* socket_data, const char* data, uint32_t length);
void send_json(client_socket_data* socket_data, const rapidjson::Document& data);
