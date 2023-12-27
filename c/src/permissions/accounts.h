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

void create_database_account(char* username, char* password, DatabasePermissions& permissions);
void delete_database_account(DatabaseAccount* account);
void update_database_account(DatabaseAccount* account, DatabaseAccount new_account);

void set_table_account_permissions(ActiveTable* table, DatabaseAccount* account, TablePermissions permissions);
void delete_table_account_permissions(ActiveTable* table, DatabaseAccount* account);

const TablePermissions* get_table_permissions_for_account(ActiveTable* table, DatabaseAccount* account, bool include_table_admin = true);

