#pragma once
#include "connections/client.h"
#include "misc/template_utils.h"
#include "permissions/accounts.h"
#include <openssl/rsa.h>
#include <openssl/pem.h>

extern int server_socket_id;
extern std::unordered_map<std::string, DatabaseAccount*, MapStringViewHash, MapStringViewEqual> database_accounts;
extern FILE* database_accounts_handle;

#define DB_MAJOR_VERSION 10
#define DB_MINOR_VERSION 0

// For use to detect incompatible schema versions between updates.
#define DB_SCHEMA_MAJOR_VERSION 1

namespace server_config {
    namespace rsa {
        extern BIO* public_key;
        extern BIO* private_key;
    }

    extern bool force_encrypted_traffic;
    extern int port;
    extern unsigned int max_connections;
    extern bool root_account_enabled;
    extern char* root_password;
    extern std::string data_directory;
};