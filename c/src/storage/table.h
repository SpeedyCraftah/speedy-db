#pragma once

#include <cstdio>
#include "driver.h"

class ActiveTable {
    public:
        ActiveTable(const char* table_name, bool is_internal);
        ~ActiveTable();

    private:
        FILE* data_handle;
        FILE* dynamic_handle;

        std::map<std::string, table_column> columns;
        std::unordered_map<size_t, TablePermissions>* permissions = nullptr;

        uint32_t hashed_column_count = 0;
        uint32_t record_data_size = 0;
        uint32_t record_size = 0;
        
        bool is_internal;
        table_header header;
};
