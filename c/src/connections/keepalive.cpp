#include "keepalive.h"
#include "client.h"
#include "handler.h"
#include <chrono>
#include <exception>
#include <thread>
#include <unistd.h>
#include "../logging/logger.h"

const uint32_t ka_data = 0;
const uint32_t ka_length = sizeof(uint32_t);
void* keepalive_thread_handle(void* args) {
    while (true) {
        for (auto& socket : *socket_connections) {
            // If there has been no packets even after several keep-alive messages.
            if (std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch()).count() - socket.second->last_packet_time > 50000 + 60000) {
                logerr("Socket with handle %d has been terminated as it has not replied to multiple keep-alive packets", socket.second->socket_id);
                // Terminate the socket as it does not appear to be alive and is just sapping resources.
                pthread_cancel(socket.second->thread_id);
                terminate_socket(socket.second->socket_id);
            }

            // If there has been no packets from the socket for a while.
            else if (std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch()).count() - socket.second->last_packet_time > 60000) {
                // Send an empty keep-alive message which should be sent back by the client.
                // Which would confirm the connection is still alive, just silent.
                send_ka(socket.second);
            }
        }

        // Sleep for 16 seconds before next check.
        std::this_thread::sleep_for(std::chrono::milliseconds(60000));
    }

    return 0;
}