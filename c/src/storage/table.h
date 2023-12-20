#pragma once

#include <cstdio>
#include <mutex>
#include <unordered_map>
#include "../deps/json.hpp"
#include "../permissions/permissions.h"
#include "../deps/simdjson/simdjson.h"
#include "../deps/rapidjson/document.h"
#include "compiled-query.h"
#include "../logging/logger.h"

#define HASH_SEED 8293236
#define TABLE_MAGIC_NUMBER 3829859236

#define BULK_HEADER_READ_COUNT 10

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
    uint8_t name_length;
    types type;
    uint32_t size;
    uint32_t index;
    uint32_t buffer_offset;
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

#define rapidjson_string_view(str) rapidjson::GenericStringRef<char>(str.data(), str.size())

class ActiveTable {
    public:
        ActiveTable(const char* table_name, bool is_internal);
        ~ActiveTable();

        bool find_one_record(query_compiler::CompiledFindQuery* query, rapidjson::Document& result);

        void insert_record(query_compiler::CompiledInsertQuery* query);

        //friend table_rebuild_statistics rebuild_table(ActiveTable** table);

        bool is_internal;

    private:
        FILE* data_handle;
        FILE* dynamic_handle;

        std::mutex op_mutex;

        uint32_t hashed_column_count = 0;
        uint32_t record_data_size = 0;
        uint32_t record_size = 0;

        // Create record buffer so operations don't need to constantly allocate the same buffer.
        // Needs refactoring with concurrent operations.
        record_header* header_buffer;

        bool verify_record_conditions_match(record_header* record, query_compiler::GenericQueryComparison* conditions, uint32_t conditions_length);
        void assemble_record_data_to_json(record_header* record, size_t included_columns, rapidjson::Document& output);

        // Iterator for scanning the tables.
        // Performance when compiled with Ofast is comparable to a normal loop.

        // TODO - add bidirectional seek
        class data_iterator {
            ActiveTable* table;
            bool complete = false;
            size_t buffer_index = BULK_HEADER_READ_COUNT;
            size_t buffer_records_available = BULK_HEADER_READ_COUNT;

            public:
                inline data_iterator(ActiveTable* tbl) : table(tbl) {}
                #ifndef __OPTIMIZE__
                ~data_iterator() { if (table != nullptr) table->is_iterator_running = false; }
                #endif

                // Load the next record.
                data_iterator operator++();

                inline record_header& operator*() { return table->header_buffer[buffer_index]; };
                inline bool operator!=(const data_iterator& _unused) { return !this->complete; }
        };

        #ifndef __OPTIMIZE__
        bool is_iterator_running = false;
        #endif

        inline data_iterator begin() {
            #ifndef __OPTIMIZE__
                if (is_iterator_running) {
                    logerr("[RUNTIME DEBUG] table '%s' iterator begin() called while another iterator is already running", this->header.name);
                    exit(1);
                }
                is_iterator_running = true;
            #endif

            fseek(this->data_handle, 0, SEEK_SET);
            data_iterator i = data_iterator(this);
            i.operator++();
            
            return i;
        }

        inline data_iterator end() {
            return data_iterator(nullptr);
        }

    public:
        // TODO - make private in the future somehow.
        std::map<std::string, table_column*, std::less<>> columns;
        std::unordered_map<size_t, TablePermissions>* permissions = nullptr;
        table_header header;
        table_column* header_columns;
};

// External table functions.
bool table_exists(const char* name);
void create_table(const char* table_name, table_column* columns, int length);
//table_rebuild_statistics rebuild_table(ActiveTable** table);

extern std::unordered_map<std::string, ActiveTable*>* open_tables;