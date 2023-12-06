#pragma once

#include "../deps/json.hpp"
#include "../connections/client.h"
#include "../deps/simdjson/simdjson.h"
#include "../deps/rapidjson/document.h"

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
        create_database_account,
        delete_database_account,
        set_table_account_privileges,
        fetch_account_table_permissions,
        fetch_database_tables,
        fetch_database_accounts,
        fetch_account_privileges,
        no_operation,
        no_query_found_placeholder
    };
}

void send_query_error(client_socket_data* socket_data, int nonce, int error);
void process_query(client_socket_data* socket_data, uint nonce, simdjson::ondemand::document& data);