#include "query.h"

#include "../connections/client.h"
#include <exception>
#include <memory>
#include <regex>
#include <string>
#include <string_view>
#include <sys/types.h>
#include <thread>
#include <unistd.h>
#include <vector>
#include "../logging/logger.h"
#include <chrono>
#include <iostream>
#include "../main.h"
#include "../misc/valid_string.h"
#include "compiled-query.h"
#include "query-compiler.h"
#include "structures/types.h"
#include "table.h"
#include <dirent.h>
#include "structures/types.h"
#include "../misc/constants.h"

void send_query_response(client_socket_data* socket_data, int nonce, rapidjson::Document& data) {
    rapidjson::Document response_object;
    response_object.SetObject();
    response_object.AddMember(rj_query_keys::nonce, nonce, response_object.GetAllocator());
    response_object.AddMember(rj_query_keys::data, data, response_object.GetAllocator());

    send_json(socket_data, response_object);
}

void send_query_response(client_socket_data* socket_data, int nonce) {
    rapidjson::Document response_object;
    response_object.SetObject();
    response_object.AddMember(rj_query_keys::nonce, nonce, response_object.GetAllocator());

    send_json(socket_data, response_object);
}

void send_query_error(client_socket_data* socket_data, int nonce, QueryError error) {
    rapidjson::Document response_object;
    response_object.SetObject();
    response_object.AddMember(rj_query_keys::nonce, nonce, response_object.GetAllocator());
    response_object.AddMember(rj_query_keys::error, 1, response_object.GetAllocator());
    
    rapidjson::Document data_object;
    data_object.SetObject();
    data_object.AddMember(rj_query_keys::error_code, error, response_object.GetAllocator());
    if (socket_data->config.error_text) data_object.AddMember(rj_query_keys::error_text, query_error_text[error], response_object.GetAllocator());
    response_object.AddMember(rj_query_keys::data, data_object, response_object.GetAllocator());

    send_json(socket_data, response_object);
}

// TODO - query and query compiler errors are the same, remove error code in response with next update.
void send_query_error(client_socket_data* socket_data, int nonce, query_compiler::error error) {
    rapidjson::Document response_object;
    response_object.SetObject();
    response_object.AddMember(rj_query_keys::nonce, nonce, response_object.GetAllocator());
    response_object.AddMember(rj_query_keys::error, 1, response_object.GetAllocator());
    
    rapidjson::Document data_object;
    data_object.SetObject();
    data_object.AddMember(rj_query_keys::error_code, error, response_object.GetAllocator());
    if (socket_data->config.error_text) data_object.AddMember(rj_query_keys::error_text, query_compiler::error_text[error], response_object.GetAllocator());
    response_object.AddMember(rj_query_keys::data, data_object, response_object.GetAllocator());

    send_json(socket_data, response_object);
}

#define query_error(error) send_query_error(socket_data, nonce, error)

ActiveTable* _ensure_table_open(DatabaseAccount* account, client_socket_data* socket_data, uint nonce, std::string_view name) {
    // If table is already open, else open it.
    auto table_lookup = open_tables.find(name);
    if (table_lookup != open_tables.end()) {
        return table_lookup->second;
    } else {
        // If name is invalid.
        if (!misc::name_string_legal(name)) {
            send_query_error(socket_data, nonce, QueryError::params_invalid);
            return nullptr;
        }

        // If name starts with a reserved sequence.
        if (name.starts_with("--internal")) {
            send_query_error(socket_data, nonce, QueryError::name_reserved);
            return nullptr;
        }

        std::lock_guard<std::mutex> lock(table_open_mutex);

        // Check if table exists.
        if (!table_exists(name)) {
            send_query_error(socket_data, nonce, QueryError::table_not_found);
            return nullptr;
        }

        // Open the table.
        ActiveTable* table = new ActiveTable(name, false);
        open_tables[std::string(name)] = table;

        log("Table %s has been loaded into memory", table->header.name);

        return table;
    }
}

// Wrapper which automatically resolves the table name from the payload.
inline ActiveTable* _ensure_table_open(DatabaseAccount* account, client_socket_data* socket_data, uint nonce, simdjson::ondemand::object& d) {
    std::string_view name;
    if (d["table"].get(name) != simdjson::error_code::SUCCESS) {
        send_query_error(socket_data, nonce, QueryError::params_invalid);
        return nullptr;
    }

    return _ensure_table_open(account, socket_data, nonce, name);
}

#define ensure_table_open(str_or_object) _ensure_table_open(account, socket_data, nonce, str_or_object)


void process_query(client_socket_data* socket_data, uint nonce, simdjson::ondemand::document& data) {
    DatabaseAccount* account = socket_data->account;

    simdjson::ondemand::object d;

    size_t raw_op;
    if (data[sj_query_keys::op].get(raw_op) != simdjson::error_code::SUCCESS) {
        send_query_error(socket_data, nonce, QueryError::op_invalid);
        return;
    } else if (data[sj_query_keys::data].get(d) != simdjson::error_code::SUCCESS) {
        send_query_error(socket_data, nonce, QueryError::data_invalid);
        return;
    }

    QueryOp op = (QueryOp)raw_op;

    if (op >= QueryOp::NoQueryFoundPlaceholder) {
        send_query_error(socket_data, nonce, QueryError::op_invalid);
        return;
    }

    switch (op) {
        case QueryOp::NoOperation: {
            send_query_response(socket_data, nonce);
            return;
        }

        case QueryOp::OpenTable: {
            if (ensure_table_open(d) != nullptr) send_query_response(socket_data, nonce);
            return;
        }

        case QueryOp::CreateTable: {
            if (!account->permissions.CREATE_TABLES) return query_error(QueryError::insufficient_privileges);

            // Allows the record optimizer to insert padding to prevent unaligned access.
            bool opt_allow_layout_optimization = true;

            // Fetch the custom table options (if any).
            simdjson::ondemand::object options_object;
            if (d["options"].get(options_object) == simdjson::error_code::SUCCESS) {
                if (
                    options_object[TABLE_OPT_ALLOW_LAYOUT_OPTI_NAME].get(opt_allow_layout_optimization) == simdjson::error_code::INCORRECT_TYPE
                ) {
                    send_query_error(socket_data, nonce, QueryError::params_invalid);
                    return;
                }
            }

            std::string_view name;
            simdjson::ondemand::object columns_object;
            if (
                d["name"].get(name) != simdjson::error_code::SUCCESS || 
                d["columns"].get(columns_object) != simdjson::error_code::SUCCESS
            ) return query_error(QueryError::params_invalid);

            if (columns_object.is_empty()) {
                send_query_error(socket_data, nonce, QueryError::params_invalid);
                return;
            }

            // Slow but doesn't matter in this query.
            size_t columns_object_count = columns_object.count_fields();

            if (columns_object_count > DB_MAX_PHYSICAL_COLUMNS) {
                send_query_error(socket_data, nonce, QueryError::too_many_columns);
                return;
            }

            // If name starts with a reserved sequence.
            if (name.starts_with("--internal")) {
                send_query_error(socket_data, nonce, QueryError::name_reserved);
                return;
            }

            if (!misc::name_string_legal(name)) {
                send_query_error(socket_data, nonce, QueryError::params_invalid);
                return;
            }

            if (table_exists(name)) {
                send_query_error(socket_data, nonce, QueryError::table_name_in_use);
                return;
            }

            // Allocate space for the columns.
            size_t iteration = 0;

            std::list<TableCreateColumn> columns;
            for (auto item : columns_object) {
                std::string_view column_name = item.unescaped_key();
                if (!misc::column_name_string_legal(column_name)) {
                    send_query_error(socket_data, nonce, QueryError::params_invalid);
                    return;
                }

                simdjson::ondemand::object column_d = item.value();

                // Check if type exists.
                std::string_view d = column_d["type"];
                ColumnType type = string_to_column_type(d);

                if (type == (ColumnType)-1) {
                    send_query_error(socket_data, nonce, QueryError::params_invalid);
                    return;
                }

                // Setup struct.
                TableCreateColumn new_column;
                new_column.type = type;
                new_column.name = std::string(column_name);

                columns.push_back(new_column);
                ++iteration;
            }

            // Create table.
            create_table(name, columns, opt_allow_layout_optimization);
            
            send_query_response(socket_data, nonce);
            return;
        }

        case QueryOp::CreateDatabaseAccount: {
            // TODO - dangerous permission since users can create accounts with permissions they dont have effectively 
            if (!account->permissions.CREATE_ACCOUNTS) return query_error(QueryError::insufficient_privileges);

            size_t hierarchy_index = d["hierarchy_index"];

            // If hierarchy index requested is more important or equal to current users index.
            if (hierarchy_index <= account->permissions.HIERARCHY_INDEX) return query_error(QueryError::insufficient_privileges);

            // Check if user is granting privileges which the current user does not have.
            // TODO - implement this, need to upgrade C++ to C++17+ for constexpr

            std::string_view username_sv = d["username"];
            std::string username = {username_sv.begin(), username_sv.end()};
            std::string_view password_sv = d["password"];

            // If username or passwords are too short or are too long.
            if (!misc::name_string_legal(username) || password_sv.length() > 100 || password_sv.length() < 2) {
                send_query_error(socket_data, nonce, QueryError::params_invalid);
                return;
            }

            // If username is reserved by being called root.
            if (username == "root") {
                send_query_error(socket_data, nonce, QueryError::name_reserved);
                return;
            }

            // If hierarchy index is 0, or is above 1,000,000 which is reserved.
            if (hierarchy_index == 0 || hierarchy_index > 1000000) {
                send_query_error(socket_data, nonce, QueryError::value_reserved);
                return;
            }

            
            DatabasePermissions permissions;
            
            // Deny the permissions by default by setting everything to 0.
            memset(&permissions, 0, sizeof(permissions));
            
            // Apply the hierarchy index.
            permissions.HIERARCHY_INDEX = hierarchy_index;
            
            // Set user-specified permissions, otherwise deny by default.
            simdjson::ondemand::object permissions_object = d["permissions"];
            for (auto permission : permissions_object) {
                std::string_view key = permission.unescaped_key();
                bool value = permission.value();
                
                if (key == "CREATE_TABLES") permissions.CREATE_TABLES = value;
                else if (key == "DELETE_TABLES") permissions.DELETE_TABLES = value;
                else if (key == "CREATE_ACCOUNTS") permissions.CREATE_ACCOUNTS = value;
                else if (key == "UPDATE_ACCOUNTS") permissions.UPDATE_ACCOUNTS = value;
                else if (key == "DELETE_ACCOUNTS") permissions.DELETE_ACCOUNTS = value;
                else if (key == "TABLE_ADMINISTRATOR") permissions.TABLE_ADMINISTRATOR = value;
                else throw QueryError::params_invalid;                
            }

            accounts_mutex.lock();

            // If username is already taken.
            if (database_accounts.count(username) != 0) {
                accounts_mutex.unlock();
                send_query_error(socket_data, nonce, QueryError::account_username_in_use);
                return;
            }

            // Save and load the account.
            create_database_account_unlocked(std::move(username), password_sv, permissions);

            accounts_mutex.unlock();

            // Send a successful response.
            send_query_response(socket_data, nonce);
            return;
        }

        case QueryOp::DeleteDatabaseAccount: {
            if (!account->permissions.DELETE_ACCOUNTS) return query_error(QueryError::insufficient_privileges);

            std::string_view username_sv = d["username"];
            std::string username = {username_sv.begin(), username_sv.end()};

            accounts_mutex.lock();

            // Find the account.
            auto account_lookup = database_accounts.find(username);
            if (account_lookup == database_accounts.end()) {
                accounts_mutex.unlock();
                send_query_error(socket_data, nonce, QueryError::username_not_found);
                return;
            }

            DatabaseAccount* t_account = account_lookup->second;
            
            // If target account has a higher or same hierarchy index.
            if (t_account->permissions.HIERARCHY_INDEX <= account->permissions.HIERARCHY_INDEX) {
                accounts_mutex.unlock();
                return query_error(QueryError::insufficient_privileges);
            }

            delete_database_account_unlocked(t_account);

            accounts_mutex.unlock();

            // Send a successful response.
            send_query_response(socket_data, nonce);
            return;
        }

        case QueryOp::FetchAccountPrivileges: {
            std::string_view username = d["username"];

            accounts_mutex.lock();

            // Find the account.
            auto account_lookup = database_accounts.find(username);
            if (account_lookup == database_accounts.end()) {
                accounts_mutex.unlock();
                send_query_error(socket_data, nonce, QueryError::username_not_found);
                return;
            }

            DatabaseAccount* t_account = account_lookup->second;

            // Convert the permission data into an object that can be sent.
            rapidjson::Document permissions;
            permissions.SetObject();
            permissions.AddMember("CREATE_ACCOUNTS", (bool)t_account->permissions.CREATE_ACCOUNTS, permissions.GetAllocator());
            permissions.AddMember("DELETE_ACCOUNTS", (bool)t_account->permissions.DELETE_ACCOUNTS, permissions.GetAllocator());
            permissions.AddMember("UPDATE_ACCOUNTS", (bool)t_account->permissions.UPDATE_ACCOUNTS, permissions.GetAllocator());
            permissions.AddMember("CREATE_TABLES", (bool)t_account->permissions.CREATE_TABLES, permissions.GetAllocator());
            permissions.AddMember("DELETE_TABLES", (bool)t_account->permissions.DELETE_TABLES, permissions.GetAllocator());
            permissions.AddMember("TABLE_ADMINISTRATOR", (bool)t_account->permissions.TABLE_ADMINISTRATOR, permissions.GetAllocator());
            permissions.AddMember("HIERARCHY_INDEX", (uint32_t)t_account->permissions.HIERARCHY_INDEX, permissions.GetAllocator());

            accounts_mutex.unlock();

            // Send the response.
            send_query_response(socket_data, nonce, permissions);
            return;
        }

        case QueryOp::SetTableAccountPrivileges: {
            if (!account->permissions.TABLE_ADMINISTRATOR) return query_error(QueryError::insufficient_privileges);

            std::string_view username = d["username"];
            std::string_view table_name = d["table"];

            // If username is reserved by being called root.
            if (username == "root") {
                send_query_error(socket_data, nonce, QueryError::name_reserved);
                return;
            }

            std::lock_guard<std::mutex> account_mutex_lock(accounts_mutex);

            // Find the account.
            auto account_lookup = database_accounts.find(username);
            if (account_lookup == database_accounts.end()) {
                send_query_error(socket_data, nonce, QueryError::username_not_found);
                return;
            }
            
            DatabaseAccount* t_account = account_lookup->second;

            ActiveTable* table = ensure_table_open(table_name);
            if (table == nullptr) return;

            // If table is internal.
            if (table->is_internal) {
                send_query_error(socket_data, nonce, QueryError::name_reserved);
                return;
            }

            // Get existing permissions as base.
            TablePermissions permissions = *get_table_permissions_for_account_unlocked(table, t_account, false);

            // Set user specified permissions.
            simdjson::ondemand::object permissions_object = d["permissions"];
            for (auto permission : permissions_object) {
                std::string_view key = permission.unescaped_key();
                bool value = permission.value();
                
                if (key == "VIEW") permissions.VIEW = value;
                else if (key == "READ") permissions.READ = value;
                else if (key == "WRITE") permissions.WRITE = value;
                else if (key == "UPDATE") permissions.UPDATE = value;
                else if (key == "ERASE") permissions.ERASE = value;
                else {
                    send_query_error(socket_data, nonce, QueryError::params_invalid);
                    return;
                }
            }

            // Apply the table permissions to the account.
            set_table_account_permissions_unlocked(table, t_account, permissions);

            // Send a successful response.
            send_query_response(socket_data, nonce);
            return;
        }

        case QueryOp::FetchAccountTablePermissions: {
            std::string_view username = d["username"];
            std::string_view table_name = d["table"];

            // If table name starts with a reserved sequence.
            if (table_name.starts_with("--internal")) {
                send_query_error(socket_data, nonce, QueryError::name_reserved);
                return;
            }

            std::lock_guard<std::mutex> account_mutex_lock(accounts_mutex);

            // Find the account.
            auto account_lookup = database_accounts.find(username);
            if (account_lookup == database_accounts.end()) {
                send_query_error(socket_data, nonce, QueryError::username_not_found);
                return;
            }

            DatabaseAccount* t_account = account_lookup->second;

            ActiveTable* table = ensure_table_open(table_name);
            if (table == nullptr) return;

            // If user cannot view the table.
            if (!get_table_permissions_for_account_unlocked(table, account)->VIEW) {
                send_query_error(socket_data, nonce, QueryError::insufficient_privileges);
                return;
            }

            // Retrieve the permissions.
            const TablePermissions* permissions = get_table_permissions_for_account_unlocked(table, t_account, false);

            // Construct the account permissions for the table.
            rapidjson::Document data;
            data.SetObject();
            data.AddMember("VIEW", permissions->VIEW, data.GetAllocator());
            data.AddMember("READ", permissions->READ, data.GetAllocator());
            data.AddMember("WRITE", permissions->WRITE, data.GetAllocator());
            data.AddMember("UPDATE", permissions->UPDATE, data.GetAllocator());
            data.AddMember("ERASE", permissions->ERASE, data.GetAllocator());

            // Send the response.
            send_query_response(socket_data, nonce, data);
            return;
        }

        case QueryOp::FetchDatabaseTables: {
            // Create an array to hold the table names.
            rapidjson::Document tables;
            tables.SetArray();

            // Open the data directory.
            DIR* dir;
            struct dirent* ent;
            dir = opendir(server_config::data_directory.c_str());

            // If directory could not be opened.
            if (dir == NULL) {
                send_query_error(socket_data, nonce, QueryError::internal);
                return;
            }

            // Iterate over every directory.
            while ((ent = readdir(dir)) != NULL) {
                if (ent->d_type == DT_DIR) {
                    std::string_view name = ent->d_name;

                    // If name is "." or "..".
                    if (name == "." || name == "..") continue;

                    // If table is an internal table.
                    if (name.starts_with("--internal-")) continue;

                    // Push table name to the array.
                    tables.PushBack(rapidjson_string_view(name), tables.GetAllocator());
                }
            }

            // Close the directory.
            closedir(dir);

            // Send the response.
            send_query_response(socket_data, nonce, tables);
            return;
        }

        case QueryOp::FetchDatabaseAccounts: {
            // Create an array to hold the account names.
            rapidjson::Document accounts;
            accounts.SetArray();

            // Iterate over accounts map.
            for (auto& element : database_accounts) {
                // Push the accout name to the array.
                accounts.PushBack(rapidjson_string_view(element.first), accounts.GetAllocator());
            }

            // Send the response.
            send_query_response(socket_data, nonce, accounts);
            return;
        }

        default: {
            break;
        }
    }

    // OPs that require table past here.

    ActiveTable* table = ensure_table_open(d);
    if (table == nullptr) return;

    // If user is querying an internal table.
    // Only exception to this is if the build is in debug mode since it is helpful to be able to query internal tables.
    #ifdef __OPTIMIZE__
        if (table->is_internal) {
            send_query_error(socket_data, nonce, QueryError::name_reserved);
            return;
        }
    #else
        if (table->is_internal && account->permissions.HIERARCHY_INDEX != 0) {
            send_query_error(socket_data, nonce, QueryError::name_reserved);
            return;
        }
    #endif

    // Get the permissions available for the user in the table.
    const TablePermissions* table_permissions = get_table_permissions_for_account_unlocked(table, account);

    // If account does not have the permission to view the table.
    if (!table_permissions->VIEW) {
        send_query_error(socket_data, nonce, QueryError::insufficient_privileges);
        return;
    }

    switch (op) {
        case QueryOp::FetchTableMeta: {
            rapidjson::Document json;
            json.SetObject();
            json.AddMember("name", rapidjson_string_view(table->name), json.GetAllocator());
            json.AddMember("column_count", table->column_count, json.GetAllocator());
            json.AddMember("physical_record_size", table->header.record_size, json.GetAllocator());

            // Add the table options.
            rapidjson::Document options(&json.GetAllocator());
            options.SetObject();
            options.AddMember(TABLE_OPT_ALLOW_LAYOUT_OPTI_NAME, table->header.options.allow_layout_optimization, options.GetAllocator());
            json.AddMember("options", options, json.GetAllocator());

            rapidjson::Document columns(&json.GetAllocator());
            columns.SetObject();

            // Iterate over columns.
            for (uint32_t i = 0; i < table->header.num_columns; i++) {
                TableColumn& column = table->actual_header_columns[i];
                if (column.is_implementation) continue;

                std::string_view column_name = column.name;
                std::string_view column_type_name = column_type_to_string(column.type);

                rapidjson::Document column_object(&json.GetAllocator());
                column_object.SetObject();
                column_object.AddMember("name", rapidjson_string_view(column_name), column_object.GetAllocator());
                column_object.AddMember("size", column_type_sizeof(column.type), column_object.GetAllocator());
                column_object.AddMember("type", rapidjson_string_view(column_type_name), column_object.GetAllocator());

                // Column index is spoofed so have to work it out here.
                // Technically we could lie here, but we want to give the calling program an accurate idea of the table layout.
                column_object.AddMember("physical_index", i, column_object.GetAllocator());

                columns.AddMember(rapidjson_string_view(column_name), column_object, columns.GetAllocator());
            }

            json.AddMember("columns", columns, json.GetAllocator());

            send_query_response(socket_data, nonce, json);
            return;
        }

        case QueryOp::CloseTable: {
            table_open_mutex.lock();
            
            std::string owned_name(table->name);

            // Close the table.
            delete open_tables[owned_name];
            open_tables.erase(owned_name);

            table_open_mutex.unlock();

            log("Table %s has been unloaded from memory", owned_name.c_str());

            send_query_response(socket_data, nonce);
            return;
        }

        case QueryOp::InsertRecord: {
            if (!table_permissions->WRITE) {
                send_query_error(socket_data, nonce, QueryError::insufficient_privileges);
                return;
            }

            simdjson::ondemand::object columns = d["columns"];
            query_compiler::CompiledInsertQuery* query = query_compiler::compile_insert_query(table, columns);

            table->insert_record(query);
            
            send_query_response(socket_data, nonce);

            delete query;
            return;
        }

        case QueryOp::FindOneRecord: {
            if (!table_permissions->READ) {
                send_query_error(socket_data, nonce, QueryError::insufficient_privileges);
                return;
            }

            query_compiler::CompiledFindQuery* query = query_compiler::compile_find_query(table, d);

            rapidjson::Document result;
            bool found = table->find_one_record(query, result);
            if (!found) result.SetNull();

            send_query_response(socket_data, nonce, result);

            delete query;
            return;
        }

        case QueryOp::FindAllRecords: {
            if (!table_permissions->READ) {
                send_query_error(socket_data, nonce, QueryError::insufficient_privileges);
                return;
            }

            query_compiler::CompiledFindQuery* query = query_compiler::compile_find_query(table, d);

            rapidjson::Document result;
            table->find_many_records(query, result);

            send_query_response(socket_data, nonce, result);

            delete query;
            return;
        }

        case QueryOp::EraseAllRecords: {
            if (!table_permissions->ERASE) {
                send_query_error(socket_data, nonce, QueryError::insufficient_privileges);
                return;
            }

            query_compiler::CompiledEraseQuery* query = query_compiler::compile_erase_query(table, d);

            rapidjson::Document result;
            result.SetObject();
            result.AddMember("count", table->erase_many_records(query), result.GetAllocator());

            send_query_response(socket_data, nonce, result);

            delete query;
            return;
        }

        case QueryOp::UpdateAllRecords: {
            if (!table_permissions->UPDATE) {
                send_query_error(socket_data, nonce, QueryError::insufficient_privileges);
                return;
            }

            query_compiler::CompiledUpdateQuery* query = query_compiler::compile_update_query(table, d);

            rapidjson::Document result;
            result.SetObject();
            result.AddMember("count", table->update_many_records(query), result.GetAllocator());

            send_query_response(socket_data, nonce, result);

            delete query;
            return;
        }

        case QueryOp::RebuildTable: {
            if (!table_permissions->WRITE) {
                send_query_error(socket_data, nonce, QueryError::insufficient_privileges);
                return;
            }

            log("Rebuild of table %s has been started", table->header.name);

            auto start_time = std::chrono::high_resolution_clock::now();
            TableRebuildStatistics stats = rebuild_table(&table);
            auto end_time = std::chrono::high_resolution_clock::now();

            log("Rebuild of table %s has been completed (took %ums)", table->name, (unsigned int)((end_time - start_time) / std::chrono::milliseconds(1)));
            log("=== Table %s rebuild statistics ===\n- %u records discovered\n- %u dead records removed\n- %u short dynamics optimized", table->header.name, stats.record_count, stats.dead_record_count, stats.short_dynamic_count);
            log("=== Table %s rebuild statistics ===", table->name);

            rapidjson::Document data;
            data.SetObject();
            data.AddMember("short_dynamic_count", stats.short_dynamic_count, data.GetAllocator());
            data.AddMember("dead_record_count", stats.dead_record_count, data.GetAllocator());
            data.AddMember("record_count", stats.record_count, data.GetAllocator());

            send_query_response(socket_data, nonce, data);
            return;
        }

        // This case is impossible if the above switch case was defined properly.
        default: {}
    }
}