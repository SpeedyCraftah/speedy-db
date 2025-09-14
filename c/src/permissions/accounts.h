#pragma once

#include <stdint.h>
#include "../storage/table.h"
#include "permissions.h"

struct AccountPassword {
    char hash[32] = {0};
    char salt[32] = {0};
};

struct DatabaseAccount {
    bool active;
    size_t internal_index;
    char username[33] = {0};
    AccountPassword password;
    DatabasePermissions permissions;
};

extern std::mutex accounts_mutex;

void create_database_account_unlocked(std::string username, std::string_view password, DatabasePermissions& permissions);
void delete_database_account_unlocked(DatabaseAccount* account);
void update_database_account(DatabaseAccount* account, DatabaseAccount new_account);

void set_table_account_permissions_unlocked(ActiveTable* table, DatabaseAccount* account, TablePermissions permissions);
void delete_table_account_permissions(ActiveTable* table, DatabaseAccount* account);

const TablePermissions* get_table_permissions_for_account_unlocked(ActiveTable* table, DatabaseAccount* account, bool include_table_admin = true);