#pragma once

#include <netinet/in.h>
#include <openssl/sha.h>
#include <pthread.h>
#include <openssl/bn.h>
#include <openssl/crypto.h>
#include <openssl/dh.h>
#include "../permissions/accounts.h"
#include "../deps/simdjson/simdjson.h"
#include "../deps/rapidjson/document.h"

// Rapid JSON query keys.
struct rj_query_keys {
    inline static const rapidjson::GenericStringRef<char> nonce = "n";
    inline static const rapidjson::GenericStringRef<char> data = "d";
    inline static const rapidjson::GenericStringRef<char> error = "e";
    inline static const rapidjson::GenericStringRef<char> error_code = "c";
    inline static const rapidjson::GenericStringRef<char> error_text = "t";
};

// SIMD JSON query keys.
struct sj_query_keys {
    inline static constexpr const char* data = "d";
    inline static constexpr const char* op = "o";
};

struct client_socket_data {
    struct version_t {
        int major;
        int minor;
    };

    struct config_t {
        // Removes the long and detailed 'text' data entry that gets sent with every error
        // since it is largely pointless as the errors can be hardcoded + the relevant code
        // always gets sent. This should be disabled in production and only used during testing.
        bool error_text = true;
    };


    struct encryption_t {
        bool enabled = false;
        char aes_secret[SHA256_DIGEST_LENGTH];
        EVP_CIPHER_CTX* aes_ctx;
    };

    encryption_t encryption;
    pthread_t thread_id;
    int socket_id;
    char address[INET_ADDRSTRLEN];
    config_t config;
    version_t version;  
    uint64_t last_packet_time = 0;
    DatabaseAccount* account;

    simdjson::ondemand::parser parser;
    rapidjson::Document object;
};

enum query_error {
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
    table_name_in_use,
    insufficient_memory,
    invalid_account_credentials,
    too_many_connections,
    traffic_encryption_mandatory,
    account_username_in_use,
    name_reserved,
    value_reserved,
    username_not_found,
    insufficient_privileges,
    too_many_conditions,
    too_many_columns,
    unexpected_packet_size
};

extern const rapidjson::GenericStringRef<char> query_error_text[];

void* client_connection_handle(void* arg);
ssize_t send_ka(client_socket_data* socket_data);
void send_res(client_socket_data* socket_data, const char* data, uint32_t length);
void send_json(client_socket_data* socket_data, rapidjson::Document& data);
