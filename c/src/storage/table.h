#pragma once

#include <cstdio>
#include <mutex>
#include <unordered_map>
#include "../permissions/permissions.h"
#include "../deps/simdjson/simdjson.h"
#include "../deps/rapidjson/document.h"
#include "compiled-query.h"
#include "../logging/logger.h"
#include "table-reusable-types.h"
#include <map>

#define HASH_SEED 8293236
#define TABLE_MAGIC_NUMBER 3829859236

#define BULK_HEADER_READ_COUNT 2000

// Some operations use size_t bitfields to take action on certain columns, hence limit has to be that of maximum size_t bits.
#define DB_MAX_PHYSICAL_COLUMNS (sizeof(size_t) * 8)

// Table structs.

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
        void find_many_records(query_compiler::CompiledFindQuery* query, rapidjson::Document& result);

        void insert_record(query_compiler::CompiledInsertQuery* query);
        size_t erase_many_records(query_compiler::CompiledEraseQuery* query);
        size_t update_many_records(query_compiler::CompiledUpdateQuery* query);

        friend table_rebuild_statistics rebuild_table(ActiveTable** table);

        bool is_internal;

    private:
        FILE* data_handle;
        int data_handle_precise;

        int dynamic_handle;

        std::mutex op_mutex;

        uint32_t hashed_column_count = 0;
        uint32_t record_data_size = 0;
        uint32_t record_size = 0;

        // Create record buffer so operations don't need to constantly allocate the same buffer.
        // Needs refactoring with concurrent operations.
        record_header* header_buffer;

        bool verify_record_conditions_match(record_header* record, query_compiler::QueryComparator* conditions, uint32_t conditions_length);
        void assemble_record_data_to_json(record_header* record, size_t included_columns, rapidjson::Document& output);

        // Iterator for scanning the tables.
        // Performance when compiled with Ofast is comparable to a normal loop.
        class data_iterator {
            public:
                ActiveTable* table;
                bool complete = false;
                size_t buffer_index = BULK_HEADER_READ_COUNT;
                size_t buffer_records_available = BULK_HEADER_READ_COUNT;

                inline data_iterator(ActiveTable* tbl) : table(tbl) {}
                #ifndef __OPTIMIZE__
                ~data_iterator() { if (table != nullptr) table->is_iterator_running = false; }
                #endif

                // Load the next record.
                data_iterator operator++();

                // Manual version of operator++ for when records have to be modified in bulk in a tight frame.
                // Returns number of records read.
                inline size_t request_bulk_records() {
                    return fread_unlocked(table->header_buffer, table->record_size, BULK_HEADER_READ_COUNT, table->data_handle);
                }

                inline record_header* operator*() { return reinterpret_cast<record_header*>(reinterpret_cast<uint8_t*>(table->header_buffer) + (buffer_index * table->record_size)); };
                inline bool operator!=(const data_iterator& _unused) { return !this->complete; }
        };

        // A wrapper around data_iterator, but allows for iterating over records which match the conditions only.
        class specific_data_iterator {
            class record_wrapper {
                public:
                    ActiveTable* table;
                    record_header* record;

                    inline record_wrapper(ActiveTable* t, record_header* r) : table(t), record(r) {}
                    
                    inline NumericType* get_numeric(std::string_view column_name) {
                        return (NumericType*)(record->data + table->columns.find(column_name)->second->buffer_offset);
                    }

                    // TODO - add operator to get strings
            };

            public:
                query_compiler::CompiledFindQuery* query;
                ActiveTable* table;
                data_iterator iterator;
                record_header* current_record;

                inline specific_data_iterator(ActiveTable* tbl, query_compiler::CompiledFindQuery* q) : table(tbl), query(q), iterator(table->begin()), current_record(*iterator) {}
                inline  __attribute__((always_inline)) specific_data_iterator operator++() {
                    while (!this->iterator.complete) {
                        record_header* record = *this->iterator;
                        if (this->table->verify_record_conditions_match(record, query->conditions, query->conditions_count)) {
                            current_record = record;
                            ++this->iterator;
                            break;
                        } else ++this->iterator;
                    }
                    return *this;
                }

                inline record_wrapper operator*() {
                    return record_wrapper(table, this->current_record);
                }

                inline bool operator!() { return !this->iterator.complete; }
        };

        // Same as above but instead of iterating over individual records, only does by bulk.
        // Useful for erase/update operations.
        class bulk_data_iterator {
            public:
                ActiveTable* table;
                bool complete = false;
                size_t buffer_records_available = BULK_HEADER_READ_COUNT;
                size_t records_byte_offset = 0;

                inline bulk_data_iterator(ActiveTable* tbl) : table(tbl) {}
                bulk_data_iterator operator++() {
                    records_byte_offset += buffer_records_available * table->record_size;
                    
                    if (buffer_records_available != BULK_HEADER_READ_COUNT) {
                        complete = true;
                        return *this;
                    }
                    
                    buffer_records_available = fread_unlocked(table->header_buffer, table->record_size, BULK_HEADER_READ_COUNT, table->data_handle);
                    return *this;
                }

                inline uint32_t operator*() {
                    return buffer_records_available;
                }
                inline bool operator!=(const bulk_data_iterator& _unused) { return !this->complete; }
                inline size_t bulk_byte_offset() { return records_byte_offset; }

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

        inline specific_data_iterator specific_begin(query_compiler::CompiledFindQuery* query) {
            return specific_data_iterator(this, query);
        }

        inline bulk_data_iterator bulk_begin() {
            fseek(this->data_handle, 0, SEEK_SET);
            bulk_data_iterator i = bulk_data_iterator(this);
            i.operator++();
            i.records_byte_offset = 0;
            return i;
        }

        inline bulk_data_iterator bulk_end() {
            return bulk_data_iterator(nullptr);
        }

    public:
        // TODO - make private in the future somehow.
        std::map<std::string, table_column*, std::less<>> columns;
        std::unordered_map<size_t, TablePermissions>* permissions = nullptr;
        table_header header;
        table_column* header_columns;
        std::string_view name;
};

// External table functions.
bool table_exists(const char* name);
void create_table(const char* table_name, table_column* columns, int length);
table_rebuild_statistics rebuild_table(ActiveTable** table);

extern std::unordered_map<std::string, ActiveTable*>* open_tables;