#include "handler.h"
#include "../main.h"
#include <arpa/inet.h>
#include <stdio.h>
#include <sys/socket.h>
#include <stdlib.h>
#include <netinet/in.h>
#include "../logging/logger.h"
#include <unistd.h>
#include <chrono>
#include <thread>
#include "keepalive.h"

// Global variable to hold active sockets.
speedystd::guard_mutex<std::unordered_map<int, client_socket_data*>> socket_connections_guard;

void accept_connections() {
    log("SpeedDB is now listening for connections at TCP port %d", server_config::port);

    // Start the keepalive monitoring thread.
    pthread_t ka_thread_id;
    int thread_status = pthread_create(&ka_thread_id, NULL, keepalive_thread_handle, NULL);
    if (thread_status != 0) {
        logerr("Failed to create keepalive thread (errno %d)", thread_status);
        exit(1);
    }

    pthread_detach(ka_thread_id);
    log("Socket keep-alive monitoring thread has been started");

    struct sockaddr client_address;
    socklen_t client_address_length = sizeof(client_address);

    while (1) {
        // Blocks until a connection has been made and populates the client_address struct.
        int client_id = accept(server_socket_id, (struct sockaddr*)&client_address, &client_address_length);
        if (client_id == -1) {
            logerr("Connection attempt has failed (errno %d)", errno);
            continue;
        }

        client_socket_data* socket_data;

        {
            auto socket_connections = socket_connections_guard.lock();

            // If the maximum connections is limited and has been exhausted.
            if (server_config::max_connections != 0 && socket_connections->size() >= server_config::max_connections) {
                logerr("A connection attempt has been refused due to no more connection slots");
                close(client_id);
                continue;
            }
    
            sockaddr_in* addr = (sockaddr_in*)&client_address;
    
            // Create connection object and add information.
            socket_data = new client_socket_data;
            socket_data->socket_id = client_id;
            socket_data->last_packet_time = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch()).count();
            inet_ntop(AF_INET, &(addr->sin_addr), socket_data->address, INET_ADDRSTRLEN);
    
            log("A connection has been established with socket handle %d and IP %s", client_id, socket_data->address);
    
            // Add to map.
            (*socket_connections)[client_id] = socket_data;
        }

        // Create thread for connection.
        // Grab connection data from map.
        int thread_status = pthread_create(&socket_data->thread_id, NULL, client_connection_handle, socket_data);
        if (thread_status != 0) {
            logerr("Failed to create thread for connection with socket handle %d (errno %d), hence it has been refused", client_id, thread_status);
            socket_connections.erase(client_id);
            delete socket_data;
            close(client_id);
            continue;
        }

        pthread_detach(socket_data->thread_id);
    }
}

void terminate_socket(int handle) {
    // If socket is already terminated, return.
    if (socket_connections_guard.lock()->count(handle) == 0) return;

    // Wait a second for a possible error message to send.
    std::this_thread::sleep_for(std::chrono::milliseconds(500));

    {
        auto socket_connections = socket_connections_guard.lock();
    
        // Clear any potential socket data.
        client_socket_data* data = (*socket_connections)[handle];
        if (data->encryption.enabled) {
            OPENSSL_free(data->encryption.aes_ctx);
        }
    
        // Free the socket object.
        delete data;
    
        // Remove from map.
        socket_connections->erase(handle);
    }

    // Close the TCP connection.
    close(handle);
}