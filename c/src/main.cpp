#include "main.h"
#include <exception>
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
#include "storage/driver.h"
#include <cstdlib>

// Global variable holding the socket ID.
int server_socket_id;
int connections_size = 0;
std::unordered_map<int, client_socket_data*>* socket_connections;
std::unordered_map<std::string, active_table*>* open_tables;

// Default server options and attributes.
int server_config::version::major = 0;
int server_config::version::minor = 1;
int server_config::port = 4546;
bool server_config::force_encrypted_traffic = false;
char* server_config::password = nullptr;
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

    // Register exit handler.
    std::atexit(on_terminate);

    // Create structures.
    socket_connections = new std::unordered_map<int, client_socket_data*>();
    open_tables = new std::unordered_map<std::string, active_table*>();

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