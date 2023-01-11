#pragma once

#include <cstddef>
#include <stdint.h>

struct DatabasePermissions {
    // The hierarchy index of the account, where 0 = top of hierarchy and MAX = bottom of hierarchy.
    // Accounts which have a higher hierarchy index will be able to make operations such as UPDATE_ACCOUNTS and DELETE_ACCOUNTS, if granted,
    // on accounts with a lower hierarchy index, and accounts with a lower hierarchy index will not be able to perform these operations on
    // accounts with a higher hierarchy index. For example, account with hierarchy index 0 (reserved for root) can modify accounts with
    // a hierarchy index < 0, but accounts with a hierarchy index of < 0 cannot modify accounts with a hierarchy of index 0.
    // Multiple accounts can also have the same hierarchy index, where such accounts will not be able to modify each other.
    uint32_t HIERARCHY_INDEX;

    // Allows the account to be able to open unloaded tables, as well as close them on the database.
    bool OPEN_CLOSE_TABLES : 1;

    // Allows the account to be able to create tables on the database.
    bool CREATE_TABLES : 1;

    // ALlows the account to be able to delete tables on the database.
    bool DELETE_TABLES : 1;

    // Allows the accoumt to be able to create accounts on the database.
    bool CREATE_ACCOUNTS : 1;

    // Allows the account to be able to update the permissions and other characteristics of the account such as the password.
    bool UPDATE_ACCOUNTS : 1;

    // Allows the account to be able to delete accounts on the database.
    bool DELETE_ACCOUNTS : 1;

    // Allows the account to be able to access and perform any operation on all tables regardless of table-based permissions.
    bool TABLE_ADMINISTRATOR : 1;
};

struct TablePermissions {
    // Allows the account to be able to see the table in the list of tables and be able to interact with it in any way.
    // If this permission is denied, the table will not be visible to the account and will return error "table_not_found" on query.
    // This will superceed every other table permission.
    bool VIEW : 1;

    // Allows the account to be able to read and conditionally query all records in a table.
    bool READ : 1;

    // ALlows the account to be able to write and insert records into the table.
    bool WRITE : 1;

    // Allows the account to be able to update records and columns in the table.
    // If the read permission is not granted, this permission will not be granted as well.
    bool UPDATE : 1;

    // Allows the account to be able to delete any records in the database.
    bool ERASE : 1;
};

struct TablePermissionsEntry {
    size_t account_handle;
    TablePermissions permissions;
};