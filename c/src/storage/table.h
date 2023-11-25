#pragma once

#include <cstdio>
#include <mutex>
#include "driver.h"

#define HASH_SEED 8293236
#define TABLE_MAGIC_NUMBER 3829859236

class ActiveTable {
    public:
        ActiveTable(const char* table_name, bool is_internal);
        ~ActiveTable();

        nlohmann::json find_one_record(nlohmann::json& data, int dynamic_count, int seek_direction, bool limited_results);
        nlohmann::json find_all_records(nlohmann::json& data, int dynamic_count, int limit, int seek_direction, bool limited_results);

        int erase_all_records(nlohmann::json& data, int dynamic_count, int limit);
        int update_all_records(nlohmann::json& data, int dynamic_count, int limit);
        table_rebuild_statistics rebuild_table();

    private:
        FILE* data_handle;
        FILE* dynamic_handle;

        std::mutex op_mutex;

        std::map<std::string, table_column> columns;
        std::unordered_map<size_t, TablePermissions>* permissions = nullptr;

        uint32_t hashed_column_count = 0;
        uint32_t record_data_size = 0;
        uint32_t record_size = 0;
        
        bool is_internal;
        table_header header;

        void compute_dynamic_hashes(size_t* output, nlohmann::json& data);
        int calculate_offset(int index);
        bool validate_record_conditions(record_header* r_header, size_t* dynamic_hashes, nlohmann::json& conditions);
        void output_numeric_value(nlohmann::json& output, table_column& column, uint8_t* data_area);
        void output_dynamic_value(nlohmann::json& output, table_column& column, uint8_t* data_area);
        void assemble_record_data(record_header* header, nlohmann::json& output, nlohmann::json& query, bool limited_results);
        long find_record_location(nlohmann::json& data, int dynamic_count, int seek_direction);
};
