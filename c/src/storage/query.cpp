#include "query.h"

#include "../connections/client.h"
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
#include "table.h"
#include <dirent.h>

std::string_view type_int_to_string(types type) {
    if (type == types::integer) return "integer";
    else if (type == types::string) return "string";
    else if (type == types::byte) return "byte";
    else if (type == types::float32) return "float";
    else if (type == types::long64) return "long";
    else return std::string_view("");
}

types type_string_to_int(std::string_view& type) {
    if (type == "integer") return types::integer;
    else if (type == "string") return types::string;
    else if (type == "byte") return types::byte;
    else if (type == "float") return types::float32;
    else if (type == "long") return types::long64;
    else return (types)-1;
}

// TODO - optimisation needs to be looked into

void send_query_response(client_socket_data* socket_data, int nonce, rapidjson::Document& data) {
    rapidjson::Document response_object;
    response_object.SetObject();
    response_object.AddMember(rapidjson_string_view(socket_data->key_strings.nonce), nonce, response_object.GetAllocator());
    response_object.AddMember(rapidjson_string_view(socket_data->key_strings.data), data, response_object.GetAllocator());

    send_json(socket_data, response_object);
}

void send_query_response(client_socket_data* socket_data, int nonce) {
    rapidjson::Document response_object;
    response_object.SetObject();
    response_object.AddMember(rapidjson_string_view(socket_data->key_strings.nonce), nonce, response_object.GetAllocator());

    send_json(socket_data, response_object);
}

void send_query_error(client_socket_data* socket_data, int nonce, query_error error) {
    rapidjson::Document data_object;
    data_object.SetObject();
    data_object.AddMember(rapidjson_string_view(socket_data->key_strings.error_code), error, data_object.GetAllocator());
    if (socket_data->config.error_text) data_object.AddMember(rapidjson_string_view(socket_data->key_strings.error_text), query_error_text[error], data_object.GetAllocator());

    rapidjson::Document response_object;
    response_object.SetObject();
    response_object.AddMember(rapidjson_string_view(socket_data->key_strings.nonce), nonce, response_object.GetAllocator());
    response_object.AddMember(rapidjson_string_view(socket_data->key_strings.data), data_object, response_object.GetAllocator());
    response_object.AddMember(rapidjson_string_view(socket_data->key_strings.error), 1, response_object.GetAllocator());

    send_json(socket_data, response_object);
}

// TODO - query and query compiler errors are the same, remove error code in response with next update.
void send_query_error(client_socket_data* socket_data, int nonce, query_compiler::error error) {
    rapidjson::Document data_object;
    data_object.SetObject();
    data_object.AddMember(rapidjson_string_view(socket_data->key_strings.error_code), error, data_object.GetAllocator());
    if (socket_data->config.error_text) data_object.AddMember(rapidjson_string_view(socket_data->key_strings.error_text), query_compiler::error_text[error], data_object.GetAllocator());

    rapidjson::Document response_object;
    response_object.SetObject();
    response_object.AddMember(rapidjson_string_view(socket_data->key_strings.nonce), nonce, response_object.GetAllocator());
    response_object.AddMember(rapidjson_string_view(socket_data->key_strings.data), data_object, response_object.GetAllocator());
    response_object.AddMember(rapidjson_string_view(socket_data->key_strings.error), 1, response_object.GetAllocator());

    send_json(socket_data, response_object);
}

// TODO - convert all to this
#define query_error(error) send_query_error(socket_data, nonce, error)

std::mutex table_open_mutex;

// TODO - remove op-o conversion in js
void process_query(client_socket_data* socket_data, uint nonce, simdjson::ondemand::document& data) {
    int socket_id = socket_data->socket_id;
    bool short_attr = socket_data->config.short_attr;
    bool error_text = socket_data->config.error_text;
    DatabaseAccount* account = socket_data->account;

    size_t op;
    simdjson::ondemand::object d;

    if (data["op"].get(op) != simdjson::error_code::SUCCESS) {
        send_query_error(socket_data, nonce, query_error::op_invalid);
        return;
    } else if (data[socket_data->key_strings.sj_data].get(d) != simdjson::error_code::SUCCESS) {
        send_query_error(socket_data, nonce, query_error::data_invalid);
        return;
    }

    if (op >= query_ops::no_query_found_placeholder) {
        send_query_error(socket_data, nonce, query_error::op_invalid);
        return;
    }

    switch (op) {
        case query_ops::no_operation: {
            send_query_response(socket_data, nonce);
            return;
        }

        case query_ops::open_table: {
            if (!account->permissions.OPEN_CLOSE_TABLES) {
                send_query_error(socket_data, nonce, query_error::insufficient_privileges);
                return;
            }

            // TODO - try not to use string
            std::string_view name_sv;
            if (d["table"].get(name_sv) != simdjson::error_code::SUCCESS) {
                send_query_error(socket_data, nonce, query_error::params_invalid);
                return;
            }
            std::string name = {name_sv.begin(), name_sv.end()};

            // If name is invalid.
            if (!misc::name_string_legal(name)) {
                send_query_error(socket_data, nonce, query_error::params_invalid);
                return;
            }

            // If name starts with a reserved sequence.
            if (name.starts_with("--internal")) {
                send_query_error(socket_data, nonce, query_error::name_reserved);
                return;
            }

            table_open_mutex.lock();

            // Check if table is already open.
            if (open_tables->contains(name)) {
                table_open_mutex.unlock();
                send_query_error(socket_data, nonce, query_error::table_already_open);
                return;
            }

            // Check if table exists.
            if (!table_exists(name.c_str())) {
                table_open_mutex.unlock();
                send_query_error(socket_data, nonce, query_error::table_not_found);
                return;
            }

            // Open the table.
            (*open_tables)[name] = new ActiveTable(name.c_str(), false);

            table_open_mutex.unlock();

            log("Table %s has been loaded into memory", name.c_str());

            send_query_response(socket_data, nonce);
            return;
        }

        case query_ops::create_table: {
            if (!account->permissions.CREATE_TABLES) return query_error(query_error::insufficient_privileges);

            std::string_view name_sv;
            simdjson::ondemand::object columns_object;
            if (
                d["name"].get(name_sv) != simdjson::error_code::SUCCESS || 
                d["columns"].get(columns_object) != simdjson::error_code::SUCCESS
            ) return query_error(query_error::params_invalid);

            std::string name = {name_sv.begin(), name_sv.end()};

            if (columns_object.is_empty()) {
                send_query_error(socket_data, nonce, query_error::params_invalid);
                return;
            }

            // Slow but doesn't matter in this query.
            size_t columns_object_count = columns_object.count_fields();

            if (columns_object_count > 20) {
                send_query_error(socket_data, nonce, query_error::too_many_columns);
                return;
            }

            // If name starts with a reserved sequence.
            if (name.starts_with("--internal")) {
                send_query_error(socket_data, nonce, query_error::name_reserved);
                return;
            }

            if (!misc::name_string_legal(name)) {
                send_query_error(socket_data, nonce, query_error::params_invalid);
                return;
            }

            if (table_exists(name.c_str())) {
                send_query_error(socket_data, nonce, query_error::table_conflict);
                return;
            }

            // Allocate space for the columns.
            size_t iteration = 0;

            std::unique_ptr<table_column[]> columns(new table_column[columns_object_count]);
            for (auto item : columns_object) {
                // C++ regex doesn't support string views :(
                std::string_view key_sv = item.unescaped_key();
                std::string key = {key_sv.begin(), key_sv.end()};
                if (
                    !std::regex_match(key, std::regex("^[a-z_]+$")) ||
                    key.length() > 32 || key.length() < 2
                ) {
                    send_query_error(socket_data, nonce, query_error::params_invalid);
                    return;
                }

                simdjson::ondemand::object column_d = item.value();

                // Check if type exists.
                std::string_view d = column_d["type"];
                types type = type_string_to_int(d);

                if (type == -1) {
                    send_query_error(socket_data, nonce, query_error::params_invalid);
                    return;
                }

                // Setup struct.
                table_column new_column;
                new_column.type = type;

                // Compute size.
                if (type == types::integer) new_column.size = 4;
                else if (type == types::string) new_column.size = 0;
                else if (type == types::byte) new_column.size = 1;
                else if (type == types::float32) new_column.size = 4;
                else if (type == types::long64) new_column.size = 8;

                // Copy name.
                strcpy(new_column.name, key.c_str());
                new_column.name_length = key.length();

                columns[iteration] = new_column;
                ++iteration;
            }

            // Create table.
            create_table(name.c_str(), columns.get(), columns_object_count);
            
            send_query_response(socket_data, nonce);
            return;
        }

        case query_ops::create_database_account: {
            // TODO - dangerous permission since users can create accounts with permissions they dont have effectively 
            if (!account->permissions.CREATE_ACCOUNTS) return query_error(query_error::insufficient_privileges);

            size_t hierarchy_index = d["hierarchy_index"];

            // If hierarchy index requested is more important or equal to current users index.
            if (hierarchy_index <= account->permissions.HIERARCHY_INDEX) return query_error(query_error::insufficient_privileges);

            // Check if user is granting privileges which the current user does not have.
            // TODO - implement this, need to upgrade C++ to C++17+ for constexpr

            std::string_view username_sv = d["username"];
            std::string username = {username_sv.begin(), username_sv.end()};
            std::string_view password_sv = d["password"];
            std::string password = {password_sv.begin(), password_sv.end()};

            // If username or passwords are too short or are too long.
            if (!misc::name_string_legal(username) || password.length() > 100 || password.length() < 2) {
                send_query_error(socket_data, nonce, query_error::params_invalid);
                return;
            }

            // If username is reserved by being called root.
            if (username == "root") {
                send_query_error(socket_data, nonce, query_error::name_reserved);
                return;
            }

            // If hierarchy index is 0, or is above 1,000,000 which is reserved.
            if (hierarchy_index == 0 || hierarchy_index > 1000000) {
                send_query_error(socket_data, nonce, query_error::value_reserved);
                return;
            }

            // If username is already taken.
            if (database_accounts->count(username) != 0) {
                send_query_error(socket_data, nonce, query_error::account_username_in_use);
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

                if (key == "OPEN_CLOSE_TABLES") permissions.OPEN_CLOSE_TABLES = value;
                else if (key == "CREATE_TABLES") permissions.CREATE_TABLES = value;
                else if (key == "DELETE_TABLES") permissions.DELETE_TABLES = value;
                else if (key == "CREATE_ACCOUNTS") permissions.CREATE_ACCOUNTS = value;
                else if (key == "UPDATE_ACCOUNTS") permissions.UPDATE_ACCOUNTS = value;
                else if (key == "DELETE_ACCOUNTS") permissions.DELETE_ACCOUNTS = value;
                else if (key == "TABLE_ADMINISTRATOR") permissions.TABLE_ADMINISTRATOR = value;
                else throw query_error::params_invalid;                
            }

            // Save and load the account.
            create_database_account((char*)username.c_str(), (char*)password.c_str(), permissions);

            // Send a successful response.
            send_query_response(socket_data, nonce);
            return;
        }

        case query_ops::delete_database_account: {
            if (!account->permissions.DELETE_ACCOUNTS) return query_error(query_error::insufficient_privileges);

            std::string_view username_sv = d["username"];
            std::string username = {username_sv.begin(), username_sv.end()};
            std::string_view password_sv = d["password"];
            std::string password = {password_sv.begin(), password_sv.end()};

            // Find the account.
            auto account_lookup = database_accounts->find(username);
            if (account_lookup == database_accounts->end()) {
                send_query_error(socket_data, nonce, query_error::username_not_found);
                return;
            }

            DatabaseAccount* t_account = account_lookup->second;

            // If target account has a higher or same hierarchy index.
            if (t_account->permissions.HIERARCHY_INDEX <= account->permissions.HIERARCHY_INDEX) return query_error(query_error::insufficient_privileges);

            delete_database_account(t_account);

            // Send a successful response.
            send_query_response(socket_data, nonce);
            return;
        }

        case query_ops::fetch_account_privileges: {
            std::string_view username_sv = d["username"];
            std::string username = {username_sv.begin(), username_sv.end()};

            // Find the account.
            auto account_lookup = database_accounts->find(username);
            if (account_lookup == database_accounts->end()) {
                send_query_error(socket_data, nonce, query_error::username_not_found);
                return;
            }

            DatabaseAccount* t_account = account_lookup->second;

            // Convert the permission data into an object that can be sent.
            // TODO - construct initial object once.
            rapidjson::Document permissions;
            permissions.SetObject();
            permissions.AddMember("CREATE_ACCOUNTS", (bool)t_account->permissions.CREATE_ACCOUNTS, permissions.GetAllocator());
            permissions.AddMember("DELETE_ACCOUNTS", (bool)t_account->permissions.DELETE_ACCOUNTS, permissions.GetAllocator());
            permissions.AddMember("UPDATE_ACCOUNTS", (bool)t_account->permissions.UPDATE_ACCOUNTS, permissions.GetAllocator());
            permissions.AddMember("CREATE_TABLES", (bool)t_account->permissions.CREATE_TABLES, permissions.GetAllocator());
            permissions.AddMember("DELETE_TABLES", (bool)t_account->permissions.DELETE_TABLES, permissions.GetAllocator());
            permissions.AddMember("OPEN_CLOSE_TABLES", (bool)t_account->permissions.OPEN_CLOSE_TABLES, permissions.GetAllocator());
            permissions.AddMember("TABLE_ADMINISTRATOR", (bool)t_account->permissions.TABLE_ADMINISTRATOR, permissions.GetAllocator());
            permissions.AddMember("HIERARCHY_INDEX", (uint32_t)t_account->permissions.HIERARCHY_INDEX, permissions.GetAllocator());

            // Send the response.
            send_query_response(socket_data, nonce, permissions);
            return;
        }

        case query_ops::set_table_account_privileges: {
            if (!account->permissions.TABLE_ADMINISTRATOR) return query_error(query_error::insufficient_privileges);

            std::string_view username_sv = d["username"];
            std::string username = {username_sv.begin(), username_sv.end()};
            std::string_view table_name_sv = d["table"];
            std::string table_name = {table_name_sv.begin(), table_name_sv.end()};

            // If username is reserved by being called root.
            if (username == "root") {
                send_query_error(socket_data, nonce, query_error::name_reserved);
                return;
            }

            // Find the account.
            auto account_lookup = database_accounts->find(username);
            if (account_lookup == database_accounts->end()) {
                send_query_error(socket_data, nonce, query_error::username_not_found);
                return;
            }

            // Find the table.
            auto table_lookup = open_tables->find(table_name);
            if (table_lookup == open_tables->end()) {
                send_query_error(socket_data, nonce, query_error::table_not_open);
                return;
            }

            DatabaseAccount* t_account = account_lookup->second;
            ActiveTable* table = table_lookup->second;

            // If table is internal.
            if (table->is_internal) {
                send_query_error(socket_data, nonce, query_error::name_reserved);
                return;
            }

            // Get existing permissions as base.
            TablePermissions permissions = *get_table_permissions_for_account(table, t_account, false);

            // TODO - could use with constexpr too
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
                    send_query_error(socket_data, nonce, query_error::params_invalid);
                    return;
                }
            }

            // Apply the table permissions to the account.
            set_table_account_permissions(table, t_account, permissions);

            // Send a successful response.
            send_query_response(socket_data, nonce);
            return;
        }

        case query_ops::fetch_account_table_permissions: {
            std::string_view username_sv = d["username"];
            std::string username = {username_sv.begin(), username_sv.end()};
            std::string_view table_name_sv = d["table"];
            std::string table_name = {table_name_sv.begin(), table_name_sv.end()};

            // If table name starts with a reserved sequence.
            if (table_name.find("--internal") == 0) {
                send_query_error(socket_data, nonce, query_error::name_reserved);
                return;
            }

            // Find the account.
            auto account_lookup = database_accounts->find(username);
            if (account_lookup == database_accounts->end()) {
                send_query_error(socket_data, nonce, query_error::username_not_found);
                return;
            }

            // Find the table.
            auto table_lookup = open_tables->find(table_name);
            if (table_lookup == open_tables->end()) {
                send_query_error(socket_data, nonce, query_error::table_not_open);
                return;
            }

            DatabaseAccount* t_account = account_lookup->second;
            ActiveTable* table = table_lookup->second;

            // If user cannot view the table.
            if (!get_table_permissions_for_account(table, account)->VIEW) {
                send_query_error(socket_data, nonce, query_error::insufficient_privileges);
                return;
            }

            // Retrieve the permissions.
            const TablePermissions* permissions = get_table_permissions_for_account(table, t_account, false);

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

        case query_ops::fetch_database_tables: {
            // Create an array to hold the table names.
            rapidjson::Document tables;
            tables.SetArray();

            // Open the data directory.
            DIR* dir;
            struct dirent* ent;
            dir = opendir(server_config::data_directory.c_str());

            // If directory could not be opened.
            if (dir == NULL) {
                send_query_error(socket_data, nonce, query_error::internal);
                return;
            }

            // Iterate over every directory.
            while ((ent = readdir(dir)) != NULL) {
                if (ent->d_type == DT_DIR) {
                    std::string_view name = ent->d_name;

                    // If name is "." or "..".
                    if (name == "." || name == "..") continue;

                    // If table is an internal table.
                    if (name.find("--internal-") == 0) continue;

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

        case query_ops::fetch_database_accounts: {
            // Create an array to hold the account names.
            rapidjson::Document accounts;
            accounts.SetArray();

            // Iterate over accounts map.
            for (auto& element : *database_accounts) {
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


    std::string_view name_sv = d["table"];
    std::string name = {name_sv.begin(), name_sv.end()};

    // Check if table is open.
    if (open_tables->count(name) == 0) {
        send_query_error(socket_data, nonce, query_error::table_not_open);
        return;
    }

    ActiveTable* table = (*open_tables)[name];

    // If user is querying internal table.
    if (table->is_internal) {
        send_query_error(socket_data, nonce, query_error::name_reserved);
        return;
    }

    // Get the permissions available for the user in the table.
    const TablePermissions* table_permissions = get_table_permissions_for_account(table, account);

    // If account does not have the permission to view the table.
    if (!table_permissions->VIEW) {
        send_query_error(socket_data, nonce, query_error::insufficient_privileges);
        return;
    }

    switch (op) {
        case query_ops::fetch_table_meta: {
            rapidjson::Document json;
            json.SetObject();
            json.AddMember("name", rapidjson_string_view(table->name), json.GetAllocator());
            json.AddMember("column_count", table->header.num_columns, json.GetAllocator());

            rapidjson::Document columns;
            columns.SetObject();

            // Iterate over columns.
            for (int i = 0; i < table->header.num_columns; i++) {
                table_column& column = table->header_columns[i];
                std::string_view column_name = column.name;
                std::string_view column_type_name = type_int_to_string(column.type);

                rapidjson::Document column_object;
                column_object.SetObject();
                column_object.AddMember("name", rapidjson_string_view(column_name), columns.GetAllocator());
                column_object.AddMember("size", column.size, columns.GetAllocator());
                column_object.AddMember("type", rapidjson_string_view(column_type_name), columns.GetAllocator());
                column_object.AddMember("physical_index", column.index, columns.GetAllocator());

                columns.AddMember(rapidjson_string_view(column_name), column_object, columns.GetAllocator());
            }

            json.AddMember("columns", columns, json.GetAllocator());

            send_query_response(socket_data, nonce, json);
            return;
        }

        case query_ops::close_table: {
            if (!account->permissions.OPEN_CLOSE_TABLES) {
                send_query_error(socket_data, nonce, query_error::insufficient_privileges);
                return;
            }

            table_open_mutex.lock();

            // Close the table.
            delete (*open_tables)[name];
            open_tables->erase(name);

            table_open_mutex.unlock();

            log("Table %s has been unloaded from memory", name.c_str());

            send_query_response(socket_data, nonce);
            return;
        }

        case query_ops::insert_record: {
            if (!table_permissions->WRITE) {
                send_query_error(socket_data, nonce, query_error::insufficient_privileges);
                return;
            }

            simdjson::ondemand::object columns = d["columns"];
            query_compiler::CompiledInsertQuery* query = query_compiler::compile_insert_query(table, columns);

            table->insert_record(query);
            
            send_query_response(socket_data, nonce);

            query->destroy();
            return;
        }

        case query_ops::find_one_record: {
            if (!table_permissions->READ) {
                send_query_error(socket_data, nonce, query_error::insufficient_privileges);
                return;
            }

            query_compiler::CompiledFindQuery* query = query_compiler::compile_find_query(table, d);

            rapidjson::Document result;
            bool found = table->find_one_record(query, result);
            if (!found) result.SetNull();

            send_query_response(socket_data, nonce, result);

            query->destroy();
            return;
        }

        case query_ops::find_all_records: {
            if (!table_permissions->READ) {
                send_query_error(socket_data, nonce, query_error::insufficient_privileges);
                return;
            }

            query_compiler::CompiledFindQuery* query = query_compiler::compile_find_query(table, d);

            rapidjson::Document result;
            table->find_many_records(query, result);

            send_query_response(socket_data, nonce, result);

            query->destroy();
            return;
        }

        case query_ops::erase_all_records: {
            if (!table_permissions->ERASE) {
                send_query_error(socket_data, nonce, query_error::insufficient_privileges);
                return;
            }

            query_compiler::CompiledEraseQuery* query = query_compiler::compile_erase_query(table, d);

            rapidjson::Document result;
            result.SetObject();
            result.AddMember("count", table->erase_many_records(query), result.GetAllocator());

            send_query_response(socket_data, nonce, result);

            query->destroy();
            return;
        }

        case query_ops::update_all_records: {
            if (!table_permissions->UPDATE) {
                send_query_error(socket_data, nonce, query_error::insufficient_privileges);
                return;
            }

            query_compiler::CompiledUpdateQuery* query = query_compiler::compile_update_query(table, d);

            rapidjson::Document result;
            result.SetObject();
            result.AddMember("count", table->update_many_records(query), result.GetAllocator());

            send_query_response(socket_data, nonce, result);

            query->destroy();
            return;
        }

        case query_ops::rebuild_table: {
            if (!table_permissions->WRITE) {
                send_query_error(socket_data, nonce, query_error::insufficient_privileges);
                return;
            }

            log("Rebuild of table %s has been started", table->header.name);

            auto start_time = std::chrono::high_resolution_clock::now();
            table_rebuild_statistics stats = rebuild_table(&table);
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
    }
}