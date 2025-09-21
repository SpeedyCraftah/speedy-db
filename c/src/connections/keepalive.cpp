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
        // Temporary solution to prevent segmentation faults when removing elements.
        client_socket_data* socket_to_delete = nullptr;

        {
            auto socket_connections = socket_connections_guard.lock();
            for (auto& socket : *socket_connections) {
                // If there has been no packets even after several keep-alive messages.
                if (std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch()).count() - socket.second->last_packet_time > 60000 + 60000 + 30000) {
                    logerr("Socket with handle %d has been terminated as it has not replied to multiple keep-alive packets", socket.second->socket_id);
                    // Terminate the socket as it does not appear to be alive and is just sapping resources.
                    socket_to_delete = socket.second;
                    break;
                }
    
                // If there has been no packets from the socket for a while.
                else if (std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch()).count() - socket.second->last_packet_time > 60000) {
                    // Send an empty keep-alive message which should be sent back by the client.
                    // Which would confirm the connection is still alive, just silent.
                    int ka_result = send_ka(socket.second);
                    if (ka_result == -1) {
                        logerr("Socket with handle %d has been terminated due to a broken pipe", socket.second->socket_id);
                        // Terminate the socket as the kernel has detected the handle to be dead.
                        socket_to_delete = socket.second;
                        break;
                    }
                }
            }
        }

        // If there is a connection which needs to be terminated.
        // Done outside of loop as deletion is not supported inside of loops.
        if (socket_to_delete != nullptr) {
            pthread_cancel(socket_to_delete->thread_id);
            terminate_socket(socket_to_delete->socket_id);
            socket_to_delete = nullptr;
            continue;
        }

        // Sleep for 30 seconds before next check.
        std::this_thread::sleep_for(std::chrono::milliseconds(30000));
    }

    return 0;
}