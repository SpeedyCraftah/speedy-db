#include <cstdio>
#include <cstring>
#include <mutex>
#include <stdint.h>
#include <string_view>
#include "accounts.h"
#include "../main.h"
#include "../crypto/crypto.h"
#include "permissions.h"
#include "../deps/xxh/xxhash.h"
#include "../storage/query-builder.h"

std::mutex accounts_mutex;

void create_database_account(char* username, char* password, DatabasePermissions& permissions) {
    accounts_mutex.lock();

    // Allocate memory for user account.
    DatabaseAccount* account = (DatabaseAccount*)malloc(sizeof(DatabaseAccount));
    account->active = true;

    // Copy the username (username has already been validated to be less than 33 characters).
    strcpy(account->username, username);

    // Copy the global permissions onto the account.
    account->permissions = permissions; 

    // Create the password hash and salt.
    crypto::password::hash(password, &account->password);

    // Seek to end of the file.
    fseek(database_accounts_handle, 0, SEEK_END);

    // Save the location and unique index of the account.
    account->internal_index = ftell(database_accounts_handle);

    // Write to the end of the file.
    fwrite_unlocked(account, sizeof(DatabaseAccount), 1, database_accounts_handle);

    // Seek to the start.
    fseek(database_accounts_handle, 0, SEEK_SET);

    // Add account to map.
    (*database_accounts)[username] = account;

    accounts_mutex.unlock();
}

// todo - replace old records with new ones
void delete_database_account(DatabaseAccount* account) {
    accounts_mutex.lock();

    // Seek to account location.
    fseek(database_accounts_handle, account->internal_index, SEEK_SET);

    // Set the active boolean in the account file to false.
    bool status = false;
    fwrite_unlocked(&status, sizeof(bool), 1, database_accounts_handle);
    fseek(database_accounts_handle, 0, SEEK_SET);

    // Remove the account from the map.
    database_accounts->erase(std::string(account->username));

    ActiveTable* permissions_table = (*open_tables)["--internal-table-permissions"];

    // Remove all table-specific permissions from account.
    NumericType index_u;
    index_u.unsigned64_raw = account->internal_index;

    query_builder::erase_query<1> query(permissions_table);
    query.add_where_condition("index", query.numeric_equal_to(index_u));

    permissions_table->erase_many_records(query.build());

    // Free the account from memory.
    free(account);

    accounts_mutex.unlock();
}

void update_database_account(DatabaseAccount* account, DatabaseAccount new_account) {
    accounts_mutex.lock();

    // Copy over the internal index.
    new_account.internal_index = account->internal_index;
    
    accounts_mutex.unlock();
}

void set_table_account_permissions(ActiveTable* table, DatabaseAccount* account, TablePermissions permissions) {
    ActiveTable* permissions_table = (*open_tables)["--internal-table-permissions"];

    // Check if permissions are already set for this table and account.
    if (table->permissions->count(account->internal_index)) {
        // Delete existing set.
        table->permissions->erase(account->internal_index);

        // Update the database on the new permissions.
        NumericType permissions_u;
        permissions_u.byte = *(uint8_t*)&permissions;

        query_builder::update_query<1, 1> query(permissions_table);
        query.add_where_condition("table", query.string_equal_to(table->name));
        query.add_change("permissions", query.update_numeric(permissions_u));
        query.set_limit(1);
        
        permissions_table->update_many_records(query.build());
    } else {
        NumericType index_u;
        index_u.unsigned64_raw = account->internal_index;

        NumericType permissions_u;
        permissions_u.byte = *(uint8_t*)&permissions;

        query_builder::insert_query<3> query(permissions_table);
        query.set_value("index", index_u);
        query.set_value("permissions", permissions_u);
        query.set_value("table", table->name);

        // Create the new permission entry.
        permissions_table->insert_record(query.build());
    }

    // Set the new permissions.
    (*table->permissions)[account->internal_index] = permissions;
}

void delete_table_account_permissions(ActiveTable* table, DatabaseAccount* account) {
    ActiveTable* permissions_table = (*open_tables)["--internal-table-permissions"];

    NumericType index_u;
    index_u.unsigned64_raw = account->internal_index;

    query_builder::erase_query<2> query(permissions_table);
    query.add_where_condition("table", query.string_equal_to(table->name));
    query.add_where_condition("index", query.numeric_equal_to(index_u));
    query.set_limit(1);

    permissions_table->erase_many_records(query.build());
}

// Placeholder struct for all permissions.
const TablePermissions placeholder_table_all_permissions = { 1, 1, 1, 1, 1 };
const TablePermissions placeholder_table_no_permissions = { 0, 0, 0, 0, 0 };

const TablePermissions* get_table_permissions_for_account(ActiveTable* table, DatabaseAccount* account, bool include_table_admin) {
    // If account is a table administrator, all permissions are granted regardless of table overrides.
    if (include_table_admin && account->permissions.TABLE_ADMINISTRATOR) return &placeholder_table_all_permissions;

    // Fetch the table permission overrides.
    auto lookup = table->permissions->find(account->internal_index);

    // If account has no overrides.
    if (lookup == table->permissions->end()) return &placeholder_table_no_permissions;

    // Return the table overrides.
    return &lookup->second;
}