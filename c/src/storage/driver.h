#pragma once

#include <map>
#include <stdarg.h>
#include <array>
#include <unordered_map>
#include "../deps/json.hpp"

enum types: uint32_t {
    integer,
    float32,
    long64,
    byte,
    string
};

struct table_column {
    char name[32] = {0};
    uint8_t _ = 0; // Name terminator.
    types type;
    uint32_t size;
    uint32_t index;
};

struct table_header {
    uint32_t magic_number;
    char name[32] = {0};
    uint8_t _ = 0; // Name terminator.
    uint32_t num_columns;
    table_column columns[];
};

struct active_table {
    FILE* data_handle;
    FILE* dynamic_handle;
    std::map<std::string, table_column>* columns;
    uint32_t hashed_column_count = 0;
    uint32_t record_data_size = 0;
    uint32_t record_size = 0;
    table_header header;
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

extern std::unordered_map<std::string, active_table*>* open_tables;

bool table_exists(const char* name);

void create_table(const char* table_name, table_column* columns, int length);
active_table* open_table(const char* table_name);
void close_table(const char* table_name);

long find_record_location(const char* table, nlohmann::json& data, int dynamic_count, int seek_direction);
void insert_record(const char* table, nlohmann::json& data);
nlohmann::json find_one_record(const char* table, nlohmann::json& data, int dynamic_count, int seek_direction, bool limited_results);
nlohmann::json find_all_records(const char* table, nlohmann::json& data, int dynamic_count, int limit, int seek_direction, bool limited_results);
int erase_all_records(const char* table, nlohmann::json& data, int dynamic_count, int limit);
int update_all_records(const char* table, nlohmann::json& data, int dynamic_count, int limit);
table_rebuild_statistics rebuild_table(char* table_name);