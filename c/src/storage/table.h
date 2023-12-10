#pragma once

#include <cstdio>
#include <mutex>
#include <unordered_map>
#include "../deps/json.hpp"
#include "../permissions/permissions.h"
#include "../deps/simdjson/simdjson.h"
#include "compiled-query.h"

#define HASH_SEED 8293236
#define TABLE_MAGIC_NUMBER 3829859236

// Table structs.
enum types: uint32_t {
    integer,
    float32,
    long64,
    byte,
    string
};

struct table_column {
    char name[33] = {0};
    types type;
    uint32_t size;
    uint32_t index;
};

struct table_header {
    uint32_t magic_number;
    char name[33] = {0};
    uint32_t num_columns;
};

struct table_rebuild_statistics {
    uint32_t record_count = 0;
    uint32_t dead_record_count = 0;
    uint32_t short_dynamic_count = 0;
};

enum record_flags : uint8_t {
    dirty = 1, // Determines if this block has been written to at some point.
    active = 2, // Determines whether this block holds an active record.
    available_optimisation = 4, // Determines whether this block can be optimised for better storage use.
};

struct record_header {
    uint8_t flags;
    uint8_t data[];
} __attribute__((packed));

struct dynamic_record {
    size_t record_location;
    uint32_t physical_size;
    char data[];
} __attribute__((packed));

struct hashed_entry {
    size_t hash;
    uint32_t size;
    size_t record_location;
} __attribute__((packed));

class ActiveTable {
    public:
        ActiveTable(const char* table_name, bool is_internal);
        ~ActiveTable();

        void insert_record(query_compiler::CompiledInsertQuery* query);

        friend table_rebuild_statistics rebuild_table(ActiveTable** table);

    private:
        FILE* data_handle;
        FILE* dynamic_handle;

        std::mutex op_mutex;

        uint32_t hashed_column_count = 0;
        uint32_t record_data_size = 0;
        uint32_t record_size = 0;
        
        bool is_internal;

        // Create record buffer so operations don't need to constantly allocate the same buffer.
        // Needs refactoring with concurrent operations.
        record_header* header_buffer;

    public:
        // TODO - make private in the future somehow.
        std::map<std::string, table_column, std::less<>> columns;
        std::unordered_map<size_t, TablePermissions>* permissions = nullptr;
        table_header header;
        table_column* header_columns;
};

// External table functions.
bool table_exists(const char* name);
void create_table(const char* table_name, table_column* columns, int length);
table_rebuild_statistics rebuild_table(ActiveTable** table);

extern std::unordered_map<std::string, ActiveTable*>* open_tables;