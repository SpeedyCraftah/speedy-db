#include "main.h"
#include <openssl/rand.h>
#include <string>
#include <sys/stat.h>
#include <unistd.h>
#include <stdio.h>
#include <sys/socket.h>
#include <stdlib.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <string.h>
#include <pthread.h>
#include <errno.h>
#include <unordered_map>
#include "connections/client.h"
#include "crypto/crypto.h"
#include "logging/logger.h"
#include "connections/handler.h"
#include "permissions/accounts.h"
#include <cstdlib>
#include "misc/files.h"
#include "storage/table.h"
#include <csignal>

// Global variable holding the socket ID.
int server_socket_id;
int connections_size = 0;
std::unordered_map<int, client_socket_data*> socket_connections;
std::unordered_map<std::string, ActiveTable*> open_tables;
std::unordered_map<std::string, DatabaseAccount*> database_accounts;
FILE* database_accounts_handle = nullptr;

// Default server options and attributes.
int server_config::version::major = 7;
int server_config::version::minor = 3;
int server_config::port = 4546;
bool server_config::force_encrypted_traffic = false;
bool server_config::root_account_enabled = false;
unsigned int server_config::max_connections = 10;
char* server_config::root_password = nullptr;
std::string server_config::data_directory = "./data/";

void on_terminate() {
    log("Killing socket and exiting");
    close(server_socket_id);

    // Close all file handles and finalise table operations.
    fclose(database_accounts_handle);
    for (auto t : open_tables) {
        delete t.second;
    }

    exit(0);
}

int main(int argc, char** args) {
    // Enable terminal buffering (for systemctl).
    setvbuf(stdout, NULL, _IONBF, 0);

    // Disable SIGPIPE.
    std::signal(SIGPIPE, SIG_IGN);

    // If the user provided some arguments.
    if (argc > 1) {
        // Iterate through all arguments.
        for (int i = 1; i < argc; i++) {
            std::string arg = args[i];
            
            // Extract argument in the format of NAME=VALUE.
            size_t c = arg.find_first_of('=');

            // If the argument has no value, indicating a flag.
            if (c == std::string::npos) {
                if (arg == "force-encrypted-traffic") {
                    server_config::force_encrypted_traffic = true;
                } else if (arg == "enable-root-account") {
                    // Create the details for the temporary root account.
                    server_config::root_account_enabled = true;
                    server_config::root_password = (char*)malloc(21);
                    server_config::root_password[20] = 0;

                    // Generate random password only for production build (optimised) to make debugging easier.
                    #ifdef __OPTIMIZE__
                        // Generate random bytes for the password.
                        RAND_bytes((unsigned char*)server_config::root_password, 20);

                        // Convert the bytes to ASCII codes from 0-Z.
                        for (int i = 0; i < 20; i++) {
                            server_config::root_password[i] = 48 + ((unsigned char)server_config::root_password[i] % 42);
                        }
                    #else
                        memcpy(server_config::root_password, "#DEBUG_ROOT_PASSWORD", 20);
                    #endif

                    log("The session password for the root account is \033[47m%s\033[0m with the username being 'root'", server_config::root_password);
                } else {
                    logerr("One or more command line arguments provided are incorrect.");
                    exit(1);
                }
            } else {
                // Extract name and value of argument.
                std::string name = arg.substr(0, c);
                std::string value = arg.substr(c + 1);

                try {
                    if (name == "max-connections") {
                        server_config::max_connections = std::stoi(value);
                    } else if (name == "port") {
                        server_config::port = std::stoi(value);
                    } else if (name == "data-directory") {
                        std::string data_directory = value.ends_with('/') ? value : value.append("/");
                        server_config::data_directory = data_directory;
                    } else {
                        logerr("One or more command line arguments provided are incorrect.");
                        exit(1);
                    }
                } catch(std::exception& e) {
                    logerr("One or more command line arguments provided are incorrect.");
                    exit(1);
                }
            }
        }
    }

    if (server_config::root_account_enabled) {
        logwarn("The root account is enabled with the temporary password being printed to the logs which is unsafe");
        logwarn("Make sure to disable the root account after creating a user account");
    }

    // Check for first run or incomplete data directory.
    std::string account_bin_path = server_config::data_directory + "accounts.bin";
    bool data_directory_exists = folder_exists(server_config::data_directory.c_str());
    if (!data_directory_exists || !file_exists(account_bin_path.c_str())) {
        log("First boot detected - welcome to SpeedyDB");
        
        // Create data folder with standard R/W permissions.
        if (!data_directory_exists) mkdir(server_config::data_directory.c_str(), 0777);

        // Create accounts storage file.
        fclose(fopen(account_bin_path.c_str(), "a"));

        // Create the table permissions table which holds permission data on all tables.

        table_column columns[3];

        // Unique identifier of permission entry.
        columns[0].index = 0;
        columns[0].name_length = sizeof("index") - 1;
        strcpy(columns[0].name, "index");
        columns[0].size = sizeof(size_t);
        columns[0].type = types::long64;

        // Name of target table.
        columns[1].index = 1;
        columns[1].name_length = sizeof("table") - 1;
        strcpy(columns[1].name, "table");
        columns[1].size = 0;
        columns[1].type = types::string;

        // Permission bitfield of entry.
        columns[2].index = 2;
        columns[2].name_length = sizeof("permissions") - 1;
        strcpy(columns[2].name, "permissions");
        columns[2].size = sizeof(uint8_t);
        columns[2].type = types::byte;

        // Create account table permissions table.
        create_table("--internal-table-permissions", columns, 3);
    }

    // Register exit handler.
    std::atexit(on_terminate);
    std::signal(SIGINT, [](int signum) { exit(0); });
    std::signal(SIGTERM, [](int signum) { exit(0); });

    if (server_config::root_account_enabled) {
        // Add the root account to the list.
        DatabaseAccount* account = (DatabaseAccount*)malloc(sizeof(DatabaseAccount));

        // Set all permissions to true.
        memset(&account->permissions, 0xFF, sizeof(account->permissions));
        account->permissions.HIERARCHY_INDEX = 0;

        // Set name to root.
        strcpy(account->username, "root");

        // Generate a password hash.
        crypto::password::hash(server_config::root_password, &account->password);

        // Add to map.
        database_accounts[std::string("root")] = account;
    }

    // Open the internal permissions table.
    open_tables["--internal-table-permissions"] = new ActiveTable("--internal-table-permissions", true);

    // Load the database accounts into memory.
    // Open the file containing the database accounts.
    database_accounts_handle = fopen(account_bin_path.c_str(), "r+b");
    if (database_accounts_handle == NULL) {
        logerr("Could not open the database accounts file");
        exit(1);
    }

    // Seek to the start.
    fseek(database_accounts_handle, 0, SEEK_SET);

    // Load accounts until the end of the file is reached.
    DatabaseAccount loaded_account;
    while (fread_unlocked(&loaded_account, 1, sizeof(DatabaseAccount), database_accounts_handle) == sizeof(DatabaseAccount)) {
        if (!loaded_account.active) continue;

        // Allocate memory for the account and copy it.
        DatabaseAccount* account = (DatabaseAccount*)malloc(sizeof(DatabaseAccount));
        memcpy(account, &loaded_account, sizeof(DatabaseAccount));

        // Add it to the map.
        database_accounts[std::string(account->username)] = account;
    }

    if (database_accounts.size() != 0) log("Loaded %d database user accounts into memory", database_accounts.size());
    else if (!server_config::root_account_enabled) {
        logwarn("Did not find any database user accounts - root account is also not enabled");
        logwarn("You will be unable to connect and perform any queries, including addition of new user accounts");
        logwarn("You have to create at least one account by enabling the root account with the enable-root-account argument, connecting with username 'root' with the password being generated and printed to the logs, then running the account create query");
    }

    // Create a server socket interface.
    // PF_INET - IPv4 interface
    // SOCK_STREAM - Reliable duplex channel
    // IPPROTO_TCP - Use the reliable error-checking TCP protocol
    server_socket_id = socket(PF_INET, SOCK_STREAM, IPPROTO_TCP);
    if (server_socket_id == -1) {
        logerr("Unable to create a server socket interface (errno %d)", errno);
        exit(1);
    }

    log("Created SpeedDB server socket");

    // Setup a server connection structure.
    // AF_INET - The protocol (IPv4)
    // htons(port) - The listening port of the server
    // htonl(INADDR_ANY) - Allow any incoming address type
    struct sockaddr_in address;
    address.sin_family = AF_INET;
    address.sin_port = htons(server_config::port);
    address.sin_addr.s_addr = htonl(INADDR_ANY);
    
    // Bind and reserve the port for the database.
    int bind_status = bind(server_socket_id, (struct sockaddr*)&address, sizeof(address));
    if (bind_status == -1) {
        logerr("Unable to bind to port %d (errno %d) - is it already in use?", server_config::port, errno);
        exit(1);
    }

    log("Binded SpeedDB socket to port %d", server_config::port);

    // Listen to the port for connections.
    // 10 connections can wait for an established connection.
    int listen_status = listen(server_socket_id, 10);
    if (listen_status == -1) {
        logerr("Unable to listen to binded port %d (errno %d)", server_config::port, errno);
        exit(1);
    }

    accept_connections();

    return 0;
}