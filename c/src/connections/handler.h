#pragma once

#include "client.h"
#include <unordered_map>

#define connection_limit 1

extern std::unordered_map<int, client_socket_data*>* socket_connections;

void* keep_alive_handle(void* arg);
void terminate_socket(int handle);

void accept_connections();