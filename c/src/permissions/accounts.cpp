#include <cstdio>
#include <mutex>
#include <stdint.h>
#include "accounts.h"
#include "../main.h"
#include "../crypto/crypto.h"

std::mutex mutex;

void create_database_account(char* username, char* password, DatabasePermissions& permissions) {
    mutex.lock();

    // Allocate memory for user account.
    DatabaseAccount* account = (DatabaseAccount*)malloc(sizeof(DatabaseAccount));

    // Copy the username (username has already been validated to be less than 33 characters).
    strcpy(account->username, username);

    // Copy the global permissions onto the account.
    account->permissions = permissions; 

    // Create the password hash and salt.
    crypto::password::hash(password, &account->password);

    // Seek to end of the file.
    fseek(database_accounts_handle, 0, SEEK_END);

    // Write to the end of the file.
    fwrite(account, sizeof(DatabaseAccount), 1, database_accounts_handle);

    // Seek to the start.
    fseek(database_accounts_handle, 0, SEEK_SET);

    mutex.unlock();
}