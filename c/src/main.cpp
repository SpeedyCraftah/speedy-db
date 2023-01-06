#include "main.h"
#include <exception>
#include <openssl/rand.h>
#include <string>
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
#include "logging/logger.h"
#include "connections/handler.h"
#include "permissions/accounts.h"
#include "storage/driver.h"
#include <cstdlib>

// Global variable holding the socket ID.
int server_socket_id;
int connections_size = 0;
std::unordered_map<int, client_socket_data*>* socket_connections;
std::unordered_map<std::string, active_table*>* open_tables;

// Default server options and attributes.
int server_config::version::major = 2;
int server_config::version::minor = 0;
int server_config::port = 4546;
bool server_config::force_encrypted_traffic = false;
char* server_config::password = nullptr;
bool server_config::root_account_enabled = false;
unsigned int server_config::max_connections = 10;

void on_terminate() {
    log("Killing socket and exiting");
    close(server_socket_id);
}

int main(int argc, char** args) {
    bool no_password_acknowledged = false;

    if (argc > 1) {
        for (int i = 1; i < argc; i++) {
            std::string arg = args[i];
            
            size_t c = arg.find_first_of('=');

            if (c == std::string::npos) {
                if (arg == "no-password") {
                    no_password_acknowledged = true;
                } else if (arg == "force-encrypted-traffic") {
                    server_config::force_encrypted_traffic = true;
                } else if (arg == "enable-root-account") {
                    server_config::root_account_enabled = true;
                    server_config::root_password = (char*)malloc(17);
                    server_config::root_password[16] = 0;

                    // Generate random bytes for the password.
                    RAND_bytes((unsigned char*)server_config::root_password, 16);

                    // Convert the bytes to ASCII codes from 0 to Z.
                    for (int i = 0; i < 16; i++) {
                        server_config::root_password[i] = 48 + server_config::root_password[i] % 43;
                    }

                    log("The session password for the root account is %s with the username being 'root'", server_config::root_password);
                } else {
                    logerr("One or more command line arguments provided are incorrect.");
                    exit(1);
                }
            } else {
                std::string name = arg.substr(0, c);
                std::string value = arg.substr(c + 1);

                try {
                    if (name == "password") {
                        // Set the password.
                        server_config::password = (char*)malloc(value.length() + 1);
                        server_config::password[value.length()] = 0;
                        memcpy(server_config::password, value.c_str(), value.length());
                    } else if (name == "max-connections") {
                        server_config::max_connections = std::stoi(value);
                    } else if (name == "port") {
                        server_config::port = std::stoi(value);
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

    if (server_config::password == nullptr) {
        if (!no_password_acknowledged) {
            logerr("There is no password set for the database. This will allow anyone to connect to the database and perform queries. Either set a password with the argument password=YOUR_PASSWORD or disable this protection feature with the no-password argument.");
            exit(1);
        } else {
            logwarn("There is no password set for the database. Anyone can connect and perform queries on the database.");
        }
    }

    if (server_config::root_account_enabled) {
        logwarn("The root account is enabled with the temporary password being printed to the logs which is unsafe");
        logwarn("Make sure to disable the root account after creating a user account");
    }

    // Register exit handler.
    std::atexit(on_terminate);

    // Create structures.
    socket_connections = new std::unordered_map<int, client_socket_data*>();
    open_tables = new std::unordered_map<std::string, active_table*>();
    database_accounts = new std::unordered_map<std::string, DatabaseAccount*>();

    // Load the database accounts into memory.
    // Open the file containing the database accounts.
    database_accounts_handle = fopen("./data/accounts.bin", "r+b");
    if (database_accounts_handle == NULL) {
        logerr("Could not open the database accounts file");
        exit(1);
    }

    // Seek to the start.
    fseek(database_accounts_handle, 0, SEEK_SET);

    // Load accounts until the end of the file is reached.
    DatabaseAccount loaded_account;
    while (fread(&loaded_account, sizeof(DatabaseAccount), 1, database_accounts_handle) == sizeof(DatabaseAccount)) {
        // Allocate memory for the account and copy it.
        DatabaseAccount* account = (DatabaseAccount*)malloc(sizeof(DatabaseAccount));
        memcpy(account, &loaded_account, sizeof(DatabaseAccount));

        // Add it to the map.
        (*database_accounts)[std::string(account->username)] = account;
    }

    if (database_accounts->size() != 0) log("Loaded %d database user accounts into memory", database_accounts->size());
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
    }

    accept_connections();

    return 0;
}