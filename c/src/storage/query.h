#pragma once

#include "../deps/json.hpp"
#include "../connections/client.h"

namespace query_ops {
    enum {
        create_table,
        open_table,
        fetch_table_meta,
        insert_record,
        find_one_record,
        find_all_records,
        erase_all_records,
        update_all_records,
        close_table,
        rebuild_table,
        no_query_found_placeholder
    };
}

void process_query(client_socket_data* socket_data, const nlohmann::json& data);