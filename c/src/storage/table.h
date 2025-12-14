#pragma once

#include <cstdio>
#include <mutex>
#include <unordered_map>
#include "../permissions/permissions.h"
#include "../deps/simdjson/simdjson.h"
#include "../deps/rapidjson/document.h"
#include "compiled-query.h"
#include "../logging/logger.h"
#include <map>
#include "../misc/template_utils.h"
#include "table-basic.h"
#include <list>

#define HASH_SEED 8293236
#define TABLE_MAGIC_NUMBER 3829859236

// Some operations use size_t bitfields to take action on certain columns, hence limit has to be that of maximum size_t bits.
#define DB_MAX_PHYSICAL_COLUMNS (sizeof(size_t) * 8)

struct TableRebuildStatistics {
    uint32_t record_count = 0;
    uint32_t dead_record_count = 0;
    uint32_t short_dynamic_count = 0;
};

#define rapidjson_string_view(str) rapidjson::GenericStringRef<char>(str.data(), str.size())

// Forward declarations for iterator functions.
// Under no circumstance should the header table-iterators.h which these live in BE IMPORTED HERE.
namespace table_iterator {
    class data_iterator;
    class specific_data_iterator;
    class bulk_data_iterator;
}

class ActiveTable {
    public:
        ActiveTable(std::string_view table_name, bool is_internal);
        ~ActiveTable();

        bool find_one_record(query_compiler::CompiledFindQuery* query, rapidjson::Document& result);
        void find_many_records(query_compiler::CompiledFindQuery* query, rapidjson::Document& result);

        void insert_record(query_compiler::CompiledInsertQuery* query);
        size_t erase_many_records(query_compiler::CompiledEraseQuery* query);
        size_t update_many_records(query_compiler::CompiledUpdateQuery* query);

        friend TableRebuildStatistics rebuild_table(ActiveTable** table);

        bool is_internal;

        friend class Record;
        friend class table_iterator::data_iterator;
        friend class table_iterator::specific_data_iterator;
        friend class table_iterator::bulk_data_iterator;

    private:
        FILE* data_handle;
        int data_handle_precise;
        int dynamic_handle;

        std::mutex op_mutex;

        TableColumn* impl_flags_column = nullptr;

        // Create record buffer so operations don't need to constantly allocate the same buffer.
        // Needs refactoring with concurrent operations.
        RecordData* header_buffer;

        bool verify_record_conditions_match(RecordData*, query_compiler::QueryComparator* conditions, uint32_t conditions_length);
        void assemble_record_data_to_json(RecordData*, size_t included_columns, rapidjson::Document& output);

        #ifndef __OPTIMIZE__
        bool is_iterator_running = false;
        #endif

    public:
        // TODO - make private in the future somehow.
        std::map<std::string, TableColumn*, std::less<>> columns;
        std::unordered_map<long, TablePermissions>* permissions = nullptr;
        TableHeader header;
        TableColumn* actual_header_columns;
        uint32_t column_count = 0;
        TableColumn* header_columns;
        std::string_view name;
        size_t impl_column_exclusion_bitfield = 0;
};

// External table functions.
bool table_exists(std::string_view name);

class TableCreateColumn {
    public:
        TableCreateColumn(std::string name, ColumnType type) : name(name), type(type) {}
        TableCreateColumn() {};

        std::string name;
        ColumnType type;
        
        friend void create_table(std::string_view table_name, std::list<TableCreateColumn> columns, bool opt_allow_layout_optimization);
    private:
        bool is_implementation = false;
        bool resolved = false;
};

void create_table(std::string_view table_name, std::list<TableCreateColumn> columns, bool opt_allow_layout_optimization);

TableRebuildStatistics rebuild_table(ActiveTable** table);

// Table cache.
extern std::unordered_map<std::string, ActiveTable*, MapStringViewHash, MapStringViewEqual> open_tables;

// Global table locks.
extern std::mutex table_open_mutex;