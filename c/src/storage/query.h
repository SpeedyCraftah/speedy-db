#pragma once

#include "../connections/client.h"
#include "../deps/simdjson/simdjson.h"
#include "../deps/rapidjson/document.h"
#include "../connections/client.h"
#include "query-compiler.h"

enum struct QueryOp : size_t {
    CreateTable,
    OpenTable,
    FetchTableMeta,
    InsertRecord,
    FindOneRecord,
    FindAllRecords,
    EraseAllRecords,
    UpdateAllRecords,
    CloseTable,
    RebuildTable,
    CreateDatabaseAccount,
    DeleteDatabaseAccount,
    SetTableAccountPrivileges,
    FetchAccountTablePermissions,
    FetchDatabaseTables,
    FetchDatabaseAccounts,
    FetchAccountPrivileges,
    NoOperation,
    NoQueryFoundPlaceholder
};

void send_query_error(client_socket_data* socket_data, int nonce, QueryError error);
void send_query_error(client_socket_data* socket_data, int nonce, query_compiler::error error);

void process_query(client_socket_data* socket_data, uint nonce, simdjson::ondemand::document& data);