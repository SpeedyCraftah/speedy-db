#include "query.h"

#include "../deps/json.hpp"
#include "../connections/client.h"
#include "driver.h"
#include <regex>
#include <sys/types.h>
#include <thread>
#include <unistd.h>
#include <vector>
#include "../logging/logger.h"
#include <chrono>
#include <iostream>
#include "../main.h"
#include "../misc/valid_string.h"
#include <dirent.h>

const char* type_int_to_string(types type) {
    if (type == types::integer) return "integer";
    else if (type == types::string) return "string";
    else if (type == types::byte) return "byte";
    else if (type == types::float32) return "float";
    else if (type == types::long64) return "long";
    else return nullptr;
}

types type_string_to_int(const char* type) {
    if (strcmp(type, "integer") == 0) return types::integer;
    else if (strcmp(type, "string") == 0) return types::string;
    else if (strcmp(type, "byte") == 0) return types::byte;
    else if (strcmp(type, "float") == 0) return types::float32;
    else if (strcmp(type, "long") == 0) return types::long64;
    else return (types)-1;
}

void send_query_response(client_socket_data* socket_data, int nonce, nlohmann::json& data) {
    send_json(socket_data, {
        { socket_data->config.short_attr ? "n" : "nonce", nonce },
        { socket_data->config.short_attr ? "d" : "data", data }
    });
}

void send_query_response(client_socket_data* socket_data, int nonce) {
    send_json(socket_data, {
        { socket_data->config.short_attr ? "n" : "nonce", nonce }
    });
}

void send_query_error(client_socket_data* socket_data, int nonce, int error) {
    send_json(socket_data, {
        { socket_data->config.short_attr ? "n" : "nonce", nonce },
        { socket_data->config.short_attr ? "d" : "data", {
            { socket_data->config.short_attr ? "c" : "code", error },
            { socket_data->config.short_attr ? "t" : "text", socket_data->config.error_text ? errors::text[error] : "" }
        }},
        { socket_data->config.short_attr ? "e" : "error", true }
    });
}

void process_query(client_socket_data* socket_data, const nlohmann::json& data) {
    int socket_id = socket_data->socket_id;
    bool short_attr = socket_data->config.short_attr;
    bool error_text = socket_data->config.error_text;
    DatabaseAccount* account = socket_data->account;
    uint nonce = 0;

    if (data.contains(short_attr ? "n" : "nonce") && data[short_attr ? "n" : "nonce"].is_number_unsigned()) {
        nonce = data[short_attr ? "n" : "nonce"];
    }

    if (!data.contains(short_attr ? "o" : "op") || !data[short_attr ? "o" : "op"].is_number_unsigned()) {
        send_query_error(socket_data, nonce, errors::op_invalid);
        return;
    } else if (!data.contains(short_attr ? "d" : "data") || !data[short_attr ? "d" : "data"].is_object()) {
        send_query_error(socket_data, nonce, errors::data_invalid);
        return;
    }

    auto d = data[short_attr ? "d" : "data"];
    uint op = data[short_attr ? "o" : "op"];

    // Check if OP exists.
    if (op >= query_ops::no_query_found_placeholder) {
        send_query_error(socket_data, nonce, errors::op_invalid);
        return;
    }

    if (op == query_ops::create_table) {
        if (
            !d.contains("name") || !d["name"].is_string() ||
            !d.contains("columns") || !d["columns"].is_object()
        ) {
            send_query_error(socket_data, nonce, errors::params_invalid);
            return;
        }

        if (d["columns"].size() == 0) {
            send_query_error(socket_data, nonce, errors::params_invalid);
            return;
        }

        if (d["columns"].size() > 20) {
            send_query_error(socket_data, nonce, errors::packet_size_exceeded);
            return;
        }

        std::string name = d["name"];

        // If name starts with a reserved sequence.
        if (name.find("--internal") == 0) {
            send_query_error(socket_data, nonce, errors::name_reserved);
            return;
        }

        if (
            name.length() > 32 || name.length() < 2 || !misc::name_string_legal(name)
        ) {
            send_query_error(socket_data, nonce, errors::params_invalid);
            return;
        }

        if (table_exists(name.c_str())) {
            send_query_error(socket_data, nonce, errors::table_conflict);
            return;
        }

        // Allocate space for the columns.
        int iteration = 0;
        table_column* columns = (table_column*)malloc(d["columns"].size() * sizeof(table_column));

        for (auto& item : d["columns"].items()) {
            if (
                !std::regex_match(item.key(), std::regex("^[a-z_]+$")) ||
                item.key().length() > 32 || item.key().length() < 2
            ) {
                send_query_error(socket_data, nonce, errors::params_invalid);
                return;
            }

            auto column_d = item.value();

            if (
                !column_d.is_object() ||
                !column_d.contains("type") ||
                !column_d["type"].is_string()
            ) {
                send_query_error(socket_data, nonce, errors::params_invalid);
                return;
            }

            // Check if type exists.
            std::string d = column_d["type"];
            types type = type_string_to_int(d.c_str());

            if (type == -1) {
                send_query_error(socket_data, nonce, errors::params_invalid);
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
            strcpy(new_column.name, item.key().c_str());

            columns[iteration] = new_column;
            ++iteration;
        }

        // Create table.
        create_table(name.c_str(), columns, d["columns"].size());

        // Release columns.
        free(columns);
        
        send_query_response(socket_data, nonce);
    } else if (op == query_ops::open_table) {
        if (!account->permissions.OPEN_CLOSE_TABLES) {
            send_query_error(socket_data, nonce, errors::insufficient_privileges);
            return;
        }

        if (!d.contains("table") || !d["table"].is_string()) {
            send_query_error(socket_data, nonce, errors::params_invalid);
            return;
        }

        std::string name = d["table"];

        // If name starts with a reserved sequence.
        if (name.find("--internal") == 0) {
            send_query_error(socket_data, nonce, errors::name_reserved);
            return;
        }

        // Check if table is already open.
        if (open_tables->count(name) == 1) {
            send_query_error(socket_data, nonce, errors::table_already_open);
            return;
        }

        // Check if table exists.
        if (!table_exists(name.c_str())) {
            send_query_error(socket_data, nonce, errors::table_not_found);
            return;
        }

        // Open the table.
        open_table(name.c_str());

        log("Table %s has been loaded into memory", name.c_str());

        send_query_response(socket_data, nonce);
    } else if (op == query_ops::create_database_account) {
        if (
            !d.contains("username") || !d["username"].is_string() ||
            !d.contains("password") || !d["password"].is_string() ||
            !d.contains("hierarchy_index") || !d["hierarchy_index"].is_number_unsigned() ||
            !d.contains("permissions") || !d["permissions"].is_object()
        ) {
            send_query_error(socket_data, nonce, errors::params_invalid);
            return;
        }

        // Ensure all items in the permissions object are booleans.
        for (auto& item : d["permissions"]) {
            if (!item.is_boolean()) {
                send_query_error(socket_data, nonce, errors::params_invalid);
                return;
            }
        }

        std::string username = d["username"];
        std::string password = d["password"];

        // If username or passwords are too short or are too long.
        if (username.length() > 32 || username.length() < 2 || !misc::name_string_legal(username) || password.length() > 100 || password.length() < 2) {
            send_query_error(socket_data, nonce, errors::params_invalid);
            return;
        }

        // If username is reserved by being called root.
        if (username == "root") {
            send_query_error(socket_data, nonce, errors::name_reserved);
            return;
        }

        // If hierarchy index is 0, or is above 1,000,000 which is reserved.
        if ((unsigned int)d["hierarchy_index"] == 0 || (unsigned int)d["hierarchy_index"] > 1000000) {
            send_query_error(socket_data, nonce, errors::value_reserved);
            return;
        }

        // If username is already taken.
        if (database_accounts->count(username) != 0) {
            send_query_error(socket_data, nonce, errors::account_username_in_use);
            return;
        }

        DatabasePermissions permissions;

        // Deny the permissions by default by setting everything to 0.
        memset(&permissions, 0, sizeof(permissions));

        // Apply the hierarchy index.
        permissions.HIERARCHY_INDEX = d["hierarchy_index"];

        // Set user-specified permissions, otherwise deny by default.
        if (d["permissions"].contains("OPEN_CLOSE_TABLES")) permissions.OPEN_CLOSE_TABLES = d["permissions"]["OPEN_CLOSE_TABLES"];
        if (d["permissions"].contains("CREATE_TABLES")) permissions.CREATE_TABLES = d["permissions"]["CREATE_TABLES"];
        if (d["permissions"].contains("DELETE_TABLES")) permissions.DELETE_TABLES = d["permissions"]["DELETE_TABLES"];
        if (d["permissions"].contains("CREATE_ACCOUNTS")) permissions.CREATE_ACCOUNTS = d["permissions"]["CREATE_ACCOUNTS"];
        if (d["permissions"].contains("UPDATE_ACCOUNTS")) permissions.UPDATE_ACCOUNTS = d["permissions"]["UPDATE_ACCOUNTS"];
        if (d["permissions"].contains("DELETE_ACCOUNTS")) permissions.DELETE_ACCOUNTS = d["permissions"]["DELETE_ACCOUNTS"];
        if (d["permissions"].contains("TABLE_ADMINISTRATOR")) permissions.TABLE_ADMINISTRATOR = d["permissions"]["TABLE_ADMINISTRATOR"];

        // Save and load the account.
        create_database_account((char*)username.c_str(), (char*)password.c_str(), permissions);

        // Send a successful response.
        send_query_response(socket_data, nonce);
    } else if (op == query_ops::delete_database_account) {
        if (!d.contains("username") || !d["username"].is_string()) {
            send_query_error(socket_data, nonce, errors::params_invalid);
            return;
        }

        std::string username = d["username"];

        // If username is reserved by being called root.
        if (username == "root") {
            send_query_error(socket_data, nonce, errors::name_reserved);
            return;
        }

        // Find the account.
        auto account_lookup = database_accounts->find(username);
        if (account_lookup == database_accounts->end()) {
            send_query_error(socket_data, nonce, errors::username_not_found);
            return;
        }

        DatabaseAccount* account = account_lookup->second;
        delete_database_account(account);

        // Send a successful response.
        send_query_response(socket_data, nonce);
    } else if (op == query_ops::set_table_account_privileges) {
        if (
            !d.contains("username") || !d["username"].is_string() ||
            !d.contains("permissions") || !d["permissions"].is_object() ||
            !d.contains("table") || !d["table"].is_string()
        ) {
            send_query_error(socket_data, nonce, errors::params_invalid);
            return;
        }

        // Ensure all items in the permissions object are booleans.
        for (auto& item : d["permissions"]) {
            if (!item.is_boolean()) {
                send_query_error(socket_data, nonce, errors::params_invalid);
                return;
            }
        }

        std::string username = d["username"];
        std::string table_name = d["table"];

        // If table name starts with a reserved sequence.
        if (table_name.find("--internal") == 0) {
            send_query_error(socket_data, nonce, errors::name_reserved);
            return;
        }

        // If username is reserved by being called root.
        if (username == "root") {
            send_query_error(socket_data, nonce, errors::name_reserved);
            return;
        }

        // Find the account.
        auto account_lookup = database_accounts->find(username);
        if (account_lookup == database_accounts->end()) {
            send_query_error(socket_data, nonce, errors::username_not_found);
            return;
        }

        // Find the table.
        auto table_lookup = open_tables->find(table_name);
        if (table_lookup == open_tables->end()) {
            send_query_error(socket_data, nonce, errors::table_not_open);
            return;
        }

        DatabaseAccount* account = account_lookup->second;
        active_table* table = table_lookup->second;

        // Create the table permissions.
        TablePermissions permissions;

        // Deny the permissions by default by setting everything to 0.
        memset(&permissions, 0, sizeof(permissions));

        // Set user specified permissions.
        if (d["permissions"].contains("VIEW")) permissions.VIEW = d["permissions"]["VIEW"];
        if (d["permissions"].contains("READ")) permissions.READ = d["permissions"]["READ"];
        if (d["permissions"].contains("WRITE")) permissions.WRITE = d["permissions"]["WRITE"];
        if (d["permissions"].contains("UPDATE")) permissions.UPDATE = d["permissions"]["UPDATE"];
        if (d["permissions"].contains("ERASE")) permissions.ERASE = d["permissions"]["ERASE"];

        // Apply the table permissions to the account.
        set_table_account_permissions(table, account, permissions);

        // Send a successful response.
        send_query_response(socket_data, nonce);
    } else if (op == query_ops::fetch_account_table_permissions) {
        if (
            !d.contains("username") || !d["username"].is_string() ||
            !d.contains("table") || !d["table"].is_string()
        ) {
            send_query_error(socket_data, nonce, errors::params_invalid);
            return;
        }

        std::string username = d["username"];
        std::string table_name = d["table"];

        // If table name starts with a reserved sequence.
        if (table_name.find("--internal") == 0) {
            send_query_error(socket_data, nonce, errors::name_reserved);
            return;
        }

        // Find the account.
        auto account_lookup = database_accounts->find(username);
        if (account_lookup == database_accounts->end()) {
            send_query_error(socket_data, nonce, errors::username_not_found);
            return;
        }

        // Find the table.
        auto table_lookup = open_tables->find(table_name);
        if (table_lookup == open_tables->end()) {
            send_query_error(socket_data, nonce, errors::table_not_open);
            return;
        }

        DatabaseAccount* account = account_lookup->second;
        active_table* table = table_lookup->second;

        // Retrieve the permissions.
        const TablePermissions* permissions = get_table_permissions_for_account(table, account, false);

        // Construct the account permissions for the table.
        nlohmann::json data = {
            { "VIEW", permissions->VIEW },
            { "READ", permissions->READ },
            { "WRITE", permissions->WRITE },
            { "UPDATE", permissions->UPDATE },
            { "ERASE", permissions->ERASE }
        };

        // Send the response.
        send_query_response(socket_data, nonce, data);
    } else if (op == query_ops::fetch_database_tables) {
        // Create an array to hold the table names.
        nlohmann::json tables = nlohmann::json::array();

        // Open the data directory.
        DIR* dir;
        struct dirent* ent;
        dir = opendir("./data");

        // If directory could not be opened.
        if (dir == NULL) {
            send_query_error(socket_data, nonce, errors::internal);
            return;
        }

        // Iterate over every directory.
        while ((ent = readdir(dir)) != NULL) {
            if (ent->d_type == DT_DIR) {
                std::string name = std::string(ent->d_name);

                // If name is "." or "..".
                if (name == "." || name == "..") continue;

                // If table is an internal table.
                if (name.find("--internal-") == 0) continue;

                // Push table name to the array.
                tables.push_back(name);
            }
        }

        // Close the directory.
        closedir(dir);

        // Send the response.
        send_query_response(socket_data, nonce, tables);
    } else if (op == query_ops::fetch_database_accounts) {
        // Create an array to hold the account names.
        nlohmann::json accounts = nlohmann::json::array();

        // Iterate over accounts map.
        for (auto& element : *database_accounts) {
            // Push the accout name to the array.
            accounts.push_back(element.first);
        }

        // Send the response.
        send_query_response(socket_data, nonce, accounts);
    }




    // OPs that require table past here.
    
    if (!d.contains("table") || !d["table"].is_string()) {
        send_query_error(socket_data, nonce, errors::params_invalid);
        return;
    }

    std::string name = d["table"];

    // Check if table is open.
    if (open_tables->count(name) == 0) {
        send_query_error(socket_data, nonce, errors::table_not_open);
        return;
    }

    active_table* table = (*open_tables)[name];

    // Get the permissions available for the user in the table.
    const TablePermissions* table_permissions = get_table_permissions_for_account(table, account);

    // If account does not have the permission to view the table.
    if (!table_permissions->VIEW) {
        send_query_error(socket_data, nonce, errors::table_not_found);
        return;
    }

    if (op == query_ops::fetch_table_meta) {
        auto json = nlohmann::json({
            { "name", table->header.name },
            { "column_count", (int)table->header.num_columns },
            { "columns", {} }
        });

        // Iterate over columns.
        for (int i = 0; i < table->header.num_columns; i++) {
            table_column& column = table->header.columns[i];

            json["columns"][column.name] = {
                { "name", column.name },
                { "size", (int)column.size },
                { "type", type_int_to_string(column.type) },
                { "physical_index", (int)column.index }
            };
        }

        send_query_response(socket_data, nonce, json);
    } else if (op == query_ops::close_table) {
        if (!account->permissions.OPEN_CLOSE_TABLES) {
            send_query_error(socket_data, nonce, errors::insufficient_privileges);
            return;
        }

        // Close the table.
        close_table(name.c_str());

        log("Table %s has been unloaded from memory", name.c_str());

        send_query_response(socket_data, nonce);
    } else if (op == query_ops::insert_record) {
        if (!table_permissions->WRITE) {
            send_query_error(socket_data, nonce, errors::insufficient_privileges);
            return;
        }

        // Check if all parameters are present.
        if (table->header.num_columns != d["columns"].size()) {
            send_query_error(socket_data, nonce, errors::params_invalid);
            return;
        }

        // Verify data.
        for (auto& item : d["columns"].items()) {
            auto column_n = item.key();

            // Check if column exists.
            if (table->columns->count(column_n) == 0) {
                send_query_error(socket_data, nonce, errors::params_invalid);
                return;
            }

            auto column_d = item.value();
            table_column& column = (*table->columns)[column_n];

            // Validate type.
            if (
                column.type == types::integer && !column_d.is_number_integer() ||
                column.type == types::byte && !column_d.is_number_integer() ||
                column.type == types::string && !column_d.is_string() ||
                column.type == types::float32 && (!column_d.is_number()) ||
                column.type == types::long64 && !column_d.is_number()
            ) {
                send_query_error(socket_data, nonce, errors::params_invalid);
                return;
            }
        }

        insert_record(name.c_str(), d["columns"]);
        send_query_response(socket_data, nonce);
    } else if (op == query_ops::find_one_record) {
        if (!table_permissions->READ) {
            send_query_error(socket_data, nonce, errors::insufficient_privileges);
            return;
        }

        if (!d.contains("where") || !d["where"].is_object()) {
            send_query_error(socket_data, nonce, errors::params_invalid);
            return;
        }

        int seek_direction = 1;

        if (d.contains("seek_direction")) {
            if (!d["seek_direction"].is_number_integer()) {
                send_query_error(socket_data, nonce, errors::params_invalid);
                return;
            }

            if (d["seek_direction"] != -1 && d["seek_direction"] != 1) {
                send_query_error(socket_data, nonce, errors::params_invalid);
                return;
            }

            seek_direction = d["seek_direction"];
        }

        bool limited_results = false;

        if (d.contains("return")) {
            limited_results = true;

            if (!d["return"].is_array()) {
                send_query_error(socket_data, nonce, errors::params_invalid);
                return;
            }

            // Check values in the array.
            for (auto& column : d["return"]) {
                if (!column.is_string()) {
                    send_query_error(socket_data, nonce, errors::params_invalid);
                    return;
                }

                // Check if column exists.
                if (table->columns->count(column) == 0) {
                    send_query_error(socket_data, nonce, errors::params_invalid);
                    return;
                }
            }
        }

        int dynamic_count = 0;

        // Verify data.
        for (auto& item : d["where"].items()) {
            auto column_n = item.key();

            // Check if column exists.
            if (table->columns->count(column_n) == 0) {
                send_query_error(socket_data, nonce, errors::params_invalid);
                return;
            }

            auto column_d = item.value();
            table_column& column = (*table->columns)[column_n];

            if (column_d.is_object()) {
                // Check if column is a number.
                if (column.type <= types::byte) {
                    if (
                        (column_d.contains("greater_than") && !column_d["greater_than"].is_number()) ||
                        (column_d.contains("less_than") && !column_d["less_than"].is_number()) ||
                        (column_d.contains("greater_than_equal_to") && !column_d["greater_than_equal_to"].is_number()) ||
                        (column_d.contains("less_than_equal_to") && !column_d["less_than_equal_to"].is_number())
                    ) {
                        send_query_error(socket_data, nonce, errors::params_invalid);
                        return;
                    }
                } else if (column.type == types::string) {
                    if (
                        (column_d.contains("contains") && !column_d["contains"].is_string())
                    ) {
                        send_query_error(socket_data, nonce, errors::params_invalid);
                        return;
                    }
                }
            } else if (
                column.type == types::integer && !column_d.is_number_integer() ||
                column.type == types::byte && !column_d.is_number_integer() ||
                column.type == types::string && !column_d.is_string() ||
                column.type == types::float32 && !column_d.is_number() ||
                column.type == types::long64 && !column_d.is_number()
            ) {
                send_query_error(socket_data, nonce, errors::params_invalid);
                return;
            } 

            if (column.type == types::string) {
                ++dynamic_count;
            }
        }

        nlohmann::json result = find_one_record(name.c_str(), d, dynamic_count, seek_direction, limited_results);
        send_query_response(socket_data, nonce, result);
    } else if (op == query_ops::find_all_records) {
        if (!table_permissions->READ) {
            send_query_error(socket_data, nonce, errors::insufficient_privileges);
            return;
        }

        if (!d.contains("where") || !d["where"].is_object()) {
            send_query_error(socket_data, nonce, errors::params_invalid);
            return;
        }

        int seek_direction = 1;

        if (d.contains("seek_direction")) {
            if (!d["seek_direction"].is_number_integer()) {
                send_query_error(socket_data, nonce, errors::params_invalid);
                return;
            }

            if (d["seek_direction"] != -1 && d["seek_direction"] != 1) {
                send_query_error(socket_data, nonce, errors::params_invalid);
                return;
            }

            seek_direction = d["seek_direction"];
        }

        int limit = 0;

        if (d.contains("limit")) {
            if(!d["limit"].is_number_unsigned()) {
                send_query_error(socket_data, nonce, errors::params_invalid);
                return;
            }

            limit = d["limit"];
        }

        bool limited_results = false;

        if (d.contains("return")) {
            limited_results = true;

            if (!d["return"].is_array()) {
                send_query_error(socket_data, nonce, errors::params_invalid);
                return;
            }

            // Check values in the array.
            for (auto& column : d["return"]) {
                if (!column.is_string()) {
                    send_query_error(socket_data, nonce, errors::params_invalid);
                    return;
                }

                // Check if column exists.
                if (table->columns->count(column) == 0) {
                    send_query_error(socket_data, nonce, errors::params_invalid);
                    return;
                }
            }
        }

        int dynamic_count = 0;

        if (d.contains("seek_where")) {
            if (!d["seek_where"].is_object()) {
                send_query_error(socket_data, nonce, errors::params_invalid);
                return;
            }

            for (auto& item : d["seek_where"].items()) {
                auto column_n = item.key();

                // Check if column exists.
                if (table->columns->count(column_n) == 0) {
                    send_query_error(socket_data, nonce, errors::params_invalid);
                    return;
                }

                auto column_d = item.value();
                table_column& column = (*table->columns)[column_n];

                // Validate type.
                if (column_d.is_object()) {
                    // Check if column is a number.
                    if (column.type <= types::byte) {
                        if (
                            (column_d.contains("greater_than") && !column_d["greater_than"].is_number()) ||
                            (column_d.contains("less_than") && !column_d["less_than"].is_number()) ||
                            (column_d.contains("greater_than_equal_to") && !column_d["greater_than_equal_to"].is_number()) ||
                            (column_d.contains("less_than_equal_to") && !column_d["less_than_equal_to"].is_number())
                        ) {
                            send_query_error(socket_data, nonce, errors::params_invalid);
                            return;
                        }
                    } else if (column.type == types::string) {
                        if (
                            (column_d.contains("contains") && !column_d["contains"].is_string())
                        ) {
                            send_query_error(socket_data, nonce, errors::params_invalid);
                            return;
                        }
                    }
                } else if (
                    column.type == types::integer && !column_d.is_number_integer() ||
                    column.type == types::byte && !column_d.is_number_integer() ||
                    column.type == types::string && !column_d.is_string() ||
                    column.type == types::float32 && !column_d.is_number() ||
                    column.type == types::long64 && !column_d.is_number()
                ) {
                    send_query_error(socket_data, nonce, errors::params_invalid);
                    return;
                }
            }
        }

        // Verify data.
        for (auto& item : d["where"].items()) {
            auto column_n = item.key();

            // Check if column exists.
            if (table->columns->count(column_n) == 0) {
                send_query_error(socket_data, nonce, errors::params_invalid);
                return;
            }

            auto column_d = item.value();
            table_column& column = (*table->columns)[column_n];

            // Validate type.
            if (column_d.is_object()) {
                // Check if column is a number.
                if (column.type <= types::byte) {
                    if (
                        (column_d.contains("greater_than") && !column_d["greater_than"].is_number()) ||
                        (column_d.contains("less_than") && !column_d["less_than"].is_number()) ||
                        (column_d.contains("greater_than_equal_to") && !column_d["greater_than_equal_to"].is_number()) ||
                        (column_d.contains("less_than_equal_to") && !column_d["less_than_equal_to"].is_number())
                    ) {
                        send_query_error(socket_data, nonce, errors::params_invalid);
                        return;
                    }
                } else if (column.type == types::string) {
                    if (
                        (column_d.contains("contains") && !column_d["contains"].is_string())
                    ) {
                        send_query_error(socket_data, nonce, errors::params_invalid);
                        return;
                    }
                }
            } else if (
                column.type == types::integer && !column_d.is_number_integer() ||
                column.type == types::byte && !column_d.is_number_integer() ||
                column.type == types::string && !column_d.is_string() ||
                column.type == types::float32 && !column_d.is_number() ||
                column.type == types::long64 && !column_d.is_number()
            ) {
                send_query_error(socket_data, nonce, errors::params_invalid);
                return;
            }

            if (column.type == types::string) {
                ++dynamic_count;
            }
        }

        nlohmann::json result = find_all_records(name.c_str(), d, dynamic_count, limit, seek_direction, limited_results);
        send_query_response(socket_data, nonce, result);
    } else if (op == query_ops::erase_all_records) {
        if (!table_permissions->ERASE) {
            send_query_error(socket_data, nonce, errors::insufficient_privileges);
            return;
        }

        if (!d.contains("where") || !d["where"].is_object()) {
            send_query_error(socket_data, nonce, errors::params_invalid);
            return;
        }

        int limit = 0;

        if (d.contains("limit")) {
            if(!d["limit"].is_number_unsigned()) {
                send_query_error(socket_data, nonce, errors::params_invalid);
                return;
            }

            limit = d["limit"];
        }

        int dynamic_count = 0;

        // Verify data.
        for (auto& item : d["where"].items()) {
            auto column_n = item.key();

            // Check if column exists.
            if (table->columns->count(column_n) == 0) {
                send_query_error(socket_data, nonce, errors::params_invalid);
                return;
            }

            auto column_d = item.value();
            table_column& column = (*table->columns)[column_n];

            // Validate type.
            if (column_d.is_object()) {
                // Check if column is a number.
                if (column.type <= types::byte) {
                    if (
                        (column_d.contains("greater_than") && !column_d["greater_than"].is_number()) ||
                        (column_d.contains("less_than") && !column_d["less_than"].is_number()) ||
                        (column_d.contains("greater_than_equal_to") && !column_d["greater_than_equal_to"].is_number()) ||
                        (column_d.contains("less_than_equal_to") && !column_d["less_than_equal_to"].is_number())
                    ) {
                        send_query_error(socket_data, nonce, errors::params_invalid);
                        return;
                    }
                } else if (column.type == types::string) {
                    if (
                        (column_d.contains("contains") && !column_d["contains"].is_string())
                    ) {
                        send_query_error(socket_data, nonce, errors::params_invalid);
                        return;
                    }
                }
            } else if (
                column.type == types::integer && !column_d.is_number_integer() ||
                column.type == types::byte && !column_d.is_number_integer() ||
                column.type == types::string && !column_d.is_string() ||
                column.type == types::float32 && !column_d.is_number() ||
                column.type == types::long64 && !column_d.is_number()
            ) {
                send_query_error(socket_data, nonce, errors::params_invalid);
                return;
            }

            if (column.type == types::string) {
                ++dynamic_count;
            }
        }

        int result = erase_all_records(name.c_str(), d, dynamic_count, limit);
        nlohmann::json data = { {"count", result} };

        send_query_response(socket_data, nonce, data);
    } else if (op == query_ops::update_all_records) {
        if (!table_permissions->UPDATE) {
            send_query_error(socket_data, nonce, errors::insufficient_privileges);
            return;
        }

        if (!d.contains("where") || !d["where"].is_object()) {
            send_query_error(socket_data, nonce, errors::params_invalid);
            return;
        }

        if (!d.contains("changes") || !d["changes"].is_object()) {
            send_query_error(socket_data, nonce, errors::params_invalid);
            return;
        }

        int limit = 0;

        if (d.contains("limit")) {
            if(!d["limit"].is_number_unsigned()) {
                send_query_error(socket_data, nonce, errors::params_invalid);
                return;
            }

            limit = d["limit"];
        }

        int dynamic_count = 0;

        // Verify data.
        for (auto& item : d["where"].items()) {
            auto column_n = item.key();

            // Check if column exists.
            if (table->columns->count(column_n) == 0) {
                send_query_error(socket_data, nonce, errors::params_invalid);
                return;
            }

            auto column_d = item.value();
            table_column& column = (*table->columns)[column_n];

            // Validate type.
            if (column_d.is_object()) {
                // Check if column is a number.
                if (column.type <= types::byte) {
                    if (
                        (column_d.contains("greater_than") && !column_d["greater_than"].is_number()) ||
                        (column_d.contains("less_than") && !column_d["less_than"].is_number()) ||
                        (column_d.contains("greater_than_equal_to") && !column_d["greater_than_equal_to"].is_number()) ||
                        (column_d.contains("less_than_equal_to") && !column_d["less_than_equal_to"].is_number())
                    ) {
                        send_query_error(socket_data, nonce, errors::params_invalid);
                        return;
                    }
                } else if (column.type == types::string) {
                    if (
                        (column_d.contains("contains") && !column_d["contains"].is_string())
                    ) {
                        send_query_error(socket_data, nonce, errors::params_invalid);
                        return;
                    }
                }
            } else if (
                column.type == types::integer && !column_d.is_number_integer() ||
                column.type == types::byte && !column_d.is_number_integer() ||
                column.type == types::string && !column_d.is_string() ||
                column.type == types::float32 && !column_d.is_number() ||
                column.type == types::long64 && !column_d.is_number()
            ) {
                log("dome");
                send_query_error(socket_data, nonce, errors::params_invalid);
                return;
            }

            if (column.type == types::string) {
                ++dynamic_count;
            }
        }

        // Verify data for changes.
        for (auto& item : d["changes"].items()) {
            auto column_n = item.key();

            // Check if column exists.
            if (table->columns->count(column_n) == 0) {
                send_query_error(socket_data, nonce, errors::params_invalid);
                return;
            }

            auto column_d = item.value();
            table_column& column = (*table->columns)[column_n];

            // Validate type.
            if (
                column.type == types::integer && !column_d.is_number_integer() ||
                column.type == types::byte && !column_d.is_number_integer() ||
                column.type == types::string && !column_d.is_string() ||
                column.type == types::float32 && !column_d.is_number() ||
                column.type == types::long64 && !column_d.is_number()
            ) {
                send_query_error(socket_data, nonce, errors::params_invalid);
                return;
            }
        }

        int result = update_all_records(name.c_str(), d, dynamic_count, limit);
        nlohmann::json data = { {"count", result} };

        send_query_response(socket_data, nonce, data);
    } else if (op == query_ops::rebuild_table) {
        if (!table_permissions->WRITE) {
            send_query_error(socket_data, nonce, errors::insufficient_privileges);
            return;
        }

        log("Rebuild of table %s has been started", table->header.name);

        auto start_time = std::chrono::high_resolution_clock::now();
        table_rebuild_statistics stats = rebuild_table(table->header.name);
        auto end_time = std::chrono::high_resolution_clock::now();

        log("Rebuild of table %s has been completed (took %ums)", table->header.name, (unsigned int)((end_time - start_time) / std::chrono::milliseconds(1)));
        log("=== Table %s rebuild statistics ===\n- %u records discovered\n- %u dead records removed\n- %u short dynamics optimized", table->header.name, stats.record_count, stats.dead_record_count, stats.short_dynamic_count);
        log("=== Table %s rebuild statistics ===", table->header.name);

        nlohmann::json data = {
            {"short_dynamic_count", stats.short_dynamic_count},
            {"dead_record_count", stats.dead_record_count},
            {"record_count", stats.record_count}
        };

        send_query_response(socket_data, nonce, data);
    }
}