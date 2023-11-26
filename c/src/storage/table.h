#pragma once

#include <cstdio>
#include <mutex>
#include <unordered_map>
#include "../deps/json.hpp"
#include "../permissions/permissions.h"

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

        nlohmann::json find_one_record(nlohmann::json& data, int dynamic_count, int seek_direction, bool limited_results);
        nlohmann::json find_all_records(nlohmann::json& data, int dynamic_count, int limit, int seek_direction, bool limited_results);

        int erase_all_records(nlohmann::json& data, int dynamic_count, int limit);
        int update_all_records(nlohmann::json& data, int dynamic_count, int limit);
        void insert_record(nlohmann::json& data);

        friend table_rebuild_statistics rebuild_table(ActiveTable** table);

    private:
        FILE* data_handle;
        FILE* dynamic_handle;

        std::mutex op_mutex;

        uint32_t hashed_column_count = 0;
        uint32_t record_data_size = 0;
        uint32_t record_size = 0;
        
        bool is_internal;

        void compute_dynamic_hashes(size_t* output, nlohmann::json& data);
        int calculate_offset(int index);
        bool validate_record_conditions(record_header* r_header, size_t* dynamic_hashes, nlohmann::json& conditions);
        void output_numeric_value(nlohmann::json& output, table_column& column, uint8_t* data_area);
        void output_dynamic_value(nlohmann::json& output, table_column& column, uint8_t* data_area);
        void assemble_record_data(record_header* r_header, nlohmann::json& output, nlohmann::json& query, bool limited_results);
        long find_record_location(nlohmann::json& data, int dynamic_count, int seek_direction);

    public:
        // TODO - make private in the future somehow.
        std::map<std::string, table_column> columns;
        std::unordered_map<size_t, TablePermissions>* permissions = nullptr;
        table_header header;
        table_column* header_columns;
};

// External table functions.
bool table_exists(const char* name);
void create_table(const char* table_name, table_column* columns, int length);
table_rebuild_statistics rebuild_table(ActiveTable** table);

extern std::unordered_map<std::string, ActiveTable*>* open_tables;