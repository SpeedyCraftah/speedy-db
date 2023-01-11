#include <cstdio>
#include <mutex>
#include <stdint.h>
#include "accounts.h"
#include "../main.h"
#include "../crypto/crypto.h"
#include "../storage/driver.h"

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

    // Remove all table-specific permissions from account.
    nlohmann::json query = {
        { "where", {
            { "index", account->internal_index }
        } }
    };
    erase_all_records("--internal-table-permissions", query, 0, 0);

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

void set_table_account_permissions(active_table* table, DatabaseAccount* account, TablePermissions& permissions) {
    // Check if permissions are already set for this table and account.
    if (table->permissions->count(account->internal_index)) {
        // Delete existing set.
        table->permissions->erase(account->internal_index);

        // Update the database on the new permissions.
        nlohmann::json query = {
            { "where", {
                { "table", table->header.name },
                { "index", account->internal_index }
            } },
            { "changes", {
                { "permissions", *(uint8_t*)&permissions }
            } }
        };
        update_all_records("--internal-table-permissions", query, 1, 1);
    } else {
        // Create the new permission entry.
        nlohmann::json query = {
            { "index", account->internal_index },
            { "table", table->header.name },
            { "permissions", *(uint8_t*)&permissions }
        };
        insert_record("--internal-table-permissions", query);
    }

    // Set the new permissions.
    (*table->permissions)[account->internal_index] = permissions;
}

void delete_table_account_permissions(active_table* table, DatabaseAccount* account) {
    nlohmann::json query = {
        { "where", {
            { "table", table->header.name },
            { "index", account->internal_index }
        } }
    };
    erase_all_records("--internal-table-permissions", query, 1, 1);
}