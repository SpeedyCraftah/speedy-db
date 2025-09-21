#pragma once

#include "client.h"
#include "../structures/mutex_guard.h"
#include <unordered_map>

#define connection_limit 1

extern speedystd::guard_mutex<std::unordered_map<int, client_socket_data*>> socket_connections_guard;

void* keep_alive_handle(void* arg);
void terminate_socket(int handle);

void accept_connections();