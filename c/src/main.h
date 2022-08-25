#pragma once
#include "connections/client.h"
#include <openssl/rsa.h>
#include <openssl/pem.h>

extern int server_socket_id;
extern std::unordered_map<int, client_socket_data*>* socket_connections;

namespace server_config {
    namespace version {
        extern int major;
        extern int minor;
    }

    namespace rsa {
        extern BIO* public_key;
        extern BIO* private_key;
    }

    extern bool force_encrypted_traffic;
    extern char* password;
    extern int port;
    extern unsigned int max_connections;
};