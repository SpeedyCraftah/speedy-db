#include "table.h"
#include <algorithm>
#include <cstddef>
#include <cstdio>
#include <openssl/types.h>
#include <unordered_map>
#include "../deps/xxh/xxhash.h"
#include "../logging/logger.h"
#include "compiled-query.h"
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include "query-builder.h"
#include "../main.h"
#include "../misc/valid_string.h"
#include "structures/record.h"
#include "structures/types.h"
#include "table-basic.h"
#include "table-iterators.h"

ActiveTable::ActiveTable(std::string_view table_name, bool is_internal = false) : is_internal(is_internal) {
    if (!misc::name_string_legal(table_name)) {
        logerr("Safety check fail! Table with an unsafe name was almost opened");
        std::terminate();
    }

    std::string path = server_config::data_directory;
    path += table_name;
    
    // Create the data paths.
    std::string meta_path = path + "/meta.bin";
    std::string data_path = path + "/data.bin";
    std::string dynamic_path = path + "/dynamic.bin";

    // Open the files in r+w mode.
    FILE* header_handle = fopen(meta_path.c_str(), "r+b");
    fseek(header_handle, 0, SEEK_SET);

    this->data_handle = fopen(data_path.c_str(), "r+b");
    this->data_handle_precise = fileno(this->data_handle);
    fseek(data_handle, 0, SEEK_SET);

    this->dynamic_handle = open(dynamic_path.c_str(), O_RDWR | O_CREAT, 0666);

    // Read the header.
    size_t fread_result = fread_unlocked(&this->header, sizeof(TableHeader), 1, header_handle);
    if (fread_result != 1) {
        logerr("Error or incorrect number of bytes returned from fread_unlocked for table header");
        exit(1);
    }

    if (this->header.created_major_version != DB_SCHEMA_MAJOR_VERSION) {
        logerr("Loaded table with schema version %u, but database only supports version %u", this->header.created_major_version, (uint32_t)DB_SCHEMA_MAJOR_VERSION);
        logerr("Refused to load table because the schema version is incompatible with this version");
        logerr("Ensure your tables are ported to the latest schema version before querying again");
        exit(1);
    }

    // Allocate memory for columns.
    this->actual_header_columns = (TableColumn*)calloc(header.num_columns, sizeof(TableColumn));

    // Read the columns and write them to the struct.
    fread_result = fread_unlocked(this->actual_header_columns, sizeof(TableColumn), header.num_columns, header_handle);
    if (fread_result != header.num_columns) {
        logerr("Error or incorrect number of bytes returned from fread_unlocked for table columns");
        exit(1);
    }

    // Load the columns to the map, except for the implementation columns (we want to track them explicitly).
    uint32_t impl_column_count = 0;
    for (uint32_t i = 0; i < this->header.num_columns; i++) {
        TableColumn& column = this->actual_header_columns[i];
        if (column.is_implementation) {
            this->impl_column_exclusion_bitfield |= (1 >> column.index);
            ++impl_column_count;

            std::string_view column_name(column.name, column.name_length);
            if (column_name == INTERNAL_COLUMN_IMPL_FLAGS_NAME) {
                impl_flags_column = &column;
            }

            continue;
        }

        this->columns[std::string(column.name, column.name_length)] = &this->actual_header_columns[i];
    }

    // Allocate memory for the column array which doesn't contain the internal columns.
    this->header_columns = (TableColumn*)calloc(header.num_columns - impl_column_count, sizeof(TableColumn));

    // Copy the non-internal columns into the array.
    for (uint32_t i = 0; i < header.num_columns; i++) {
        TableColumn& column = this->actual_header_columns[i];
        if (!column.is_implementation) {
            // Spoof the index of the column to not confuse code.
            column.index = this->column_count;

            this->header_columns[this->column_count] = column;
            ++this->column_count;
        }
    }

    // Safety check for important internal columns.
    if (this->impl_flags_column == nullptr) {
        logerr("Safety check fail! Table metadata did not contain mandatory internal column impl_flags");
        std::terminate();
    }

    // Create record buffer.
    this->header_buffer = (RecordData*)malloc(this->header.record_size * BULK_HEADER_READ_COUNT);

    // Set table name.
    this->name = this->header.name;

    // Load the account permissions if table is not internal as internal tables do not have custom permissions.
    if (!this->is_internal) {
        // Create permission map instance.
        this->permissions = new std::unordered_map<long, TablePermissions>();

        // Get permissions table.
        ActiveTable* permissions_table = open_tables["--internal-table-permissions"];

        query_builder::FindQuery<1> query(permissions_table);
        query.add_where_condition("table", query.string_equal_to(this->name));

        /* for future debugging */
        // bool oopsie = false;
        // std::vector<std::tuple<std::string, long, uint8_t>> elements;

        permissions_table->op_mutex.lock();
        TableColumn* permissions_column = permissions_table->columns["permissions"];
        TableColumn* index_column = permissions_table->columns["index"];
        for (Record record : table_iterator::iterate_specific(*permissions_table, query.build())) {
            uint8_t permissions = record.get_numeric(permissions_column)->byte;
            long index = record.get_numeric(index_column)->long64;

            // Safety check to see if a permission entry already exists for this user, and a duplicate was found.
            // This COULD be a debug-only check, but loading tables is rare so the added safety benefit is worth the slight performance cost.
            if (this->permissions->find(index) != this->permissions->end()) {
                logerr("Safety check fail! Loaded table '%s' and user index %ld permission more than once for this user!", this->header.name, index);
                std::terminate();
            }
            
            (*this->permissions)[index] = *(TablePermissions*)&permissions;
        }
        permissions_table->op_mutex.unlock();
    }

    // Close handles which are not needed.
    fclose(header_handle);
}

ActiveTable::~ActiveTable() {
    // Close handles.
    fclose(this->data_handle);
    close(this->dynamic_handle);

    // Free permissions if needed.
    if (this->permissions != nullptr) delete this->permissions;

    // Free columns.
    free(this->actual_header_columns);
    free(this->header_columns);

    // Free record header.
    free(this->header_buffer);
}

std::mutex table_open_mutex;
std::mutex misc_op_mutex;

bool table_exists(std::string_view name) {
    if (!misc::name_string_legal(name)) {
        logerr("Safety check fail! Table with an unsafe name was almost checked for existance");
        std::terminate();
    }

    struct stat info;

    misc_op_mutex.lock();
    std::string path = server_config::data_directory;
    path += name;
    int state = stat(path.c_str(), &info);
    misc_op_mutex.unlock();

    return state == 0;
}

void create_table(std::string_view table_name, const std::vector<TableCreateColumn>& columns, bool opt_allow_padding) {
    if (!misc::name_string_legal(table_name)) {
        logerr("Safety check fail! Table with an unsafe name was almost created");
        std::terminate();
    }

    misc_op_mutex.lock();

    // Create the data path.
    std::string path = server_config::data_directory;
    path += table_name;

    mkdir(path.c_str(), 0777);

    // Append a slash for future paths.
    path.append("/");
    
    // Create paths.
    std::string meta_path = std::string(path).append("meta.bin");
    std::string data_path = std::string(path).append("data.bin");
    std::string dynamic_path = std::string(path).append("dynamic.bin");

    // Create a file containing the table metadata.
    fclose(fopen(meta_path.c_str(), "a"));

    // Create a file containing the table data.
    fclose(fopen(data_path.c_str(), "a"));

    // Create a file containing the table dynamic data.
    fclose(fopen(dynamic_path.c_str(), "a"));

    // Open the file in r+w mode.
    FILE* handle = fopen(meta_path.c_str(), "r+b");

    // Create a store for the physical columns, as well as extra for the internal columns.
    std::vector<TableColumn> physical_columns(columns.size() + 1);
    
    // Add the initial column which will be the flags/metadata for the row.
    TableColumn& preamble_column = physical_columns[0];
    preamble_column.is_implementation = true;
    preamble_column.type = ColumnType::Byte;
    preamble_column.name_length = strlen(INTERNAL_COLUMN_IMPL_FLAGS_NAME);
    memcpy(preamble_column.name, INTERNAL_COLUMN_IMPL_FLAGS_NAME, preamble_column.name_length);

    // Convert the simple column definitions into actual columns.
    for (uint32_t i = 0; i < columns.size(); i++) {
        const TableCreateColumn& column = columns[i];
        TableColumn& physical_column = physical_columns[i + 1];
        physical_column.type = column.type;
        physical_column.is_implementation = false;

        // Copy over the name.
        memcpy(physical_column.name, column.name.c_str(), column.name.length());
        physical_column.name_length = column.name.length();
    }

    // Now, sort the columns based on alignment requirements from smallest to largest.
    std::sort(physical_columns.begin(), physical_columns.end(), [](const TableColumn& c1, const TableColumn& c2) {
        return column_type_alignof(c1.type) < column_type_alignof(c2.type);
    });

    // Perform a final iteration to determine the buffer index and offset for each column (including any padding).
    uint32_t total_buffer_offset = 0;
    for (uint32_t i = 0; i < physical_columns.size(); i++) {
        TableColumn& physical_column = physical_columns[i];
        physical_column.index = i;

        if (opt_allow_padding) {
            size_t column_alignment = column_type_alignof(physical_column.type);

            // If the column isn't aligned, we want to add padding.
            size_t column_align_check = total_buffer_offset % column_alignment;
            if (column_align_check != 0) {
                // Add the padding we need right before the column data.
                total_buffer_offset += column_alignment - column_align_check;
            }
        }

        physical_column.buffer_offset = total_buffer_offset;

        // Move the offset forwards for the next column.
        total_buffer_offset += column_type_sizeof(physical_column.type);
    }
// steal a 4 alignment variable to pair with each string
    if (opt_allow_padding) {
        // Take the value with the strict alignment requirement from the columns (this will be the last entry because we've already sorted the columns!).
        // Records are guaranteed to have at least one internal column so no boundary checks are needed.
        size_t record_align_requirement = column_type_alignof(physical_columns.back().type);
    
        // Now we want to make sure the entire record is aligned, if not we can add final padding.
        size_t record_align_check = total_buffer_offset % record_align_requirement;
        if (record_align_check != 0) {
            total_buffer_offset += record_align_requirement - record_align_check;
        }
    }

    // Create header.
    TableHeader header;
    header.created_major_version = DB_SCHEMA_MAJOR_VERSION;
    header.magic_number = TABLE_MAGIC_NUMBER;
    header.record_size = total_buffer_offset;
    header.num_columns = physical_columns.size();

    // Define the header options.
    header.options.allow_padding = opt_allow_padding;

    // Copy name.
    memcpy(header.name, table_name.data(), table_name.length());

    // Write header to file.
    fwrite_unlocked(&header, sizeof(TableHeader), 1, handle);

    // Write all of the columns.
    fwrite_unlocked(physical_columns.data(), sizeof(TableColumn), physical_columns.size(), handle);

    // Close the file handle.
    fclose(handle);

    misc_op_mutex.unlock();
}

TableRebuildStatistics rebuild_table(ActiveTable** table_var) {
    ActiveTable* table = *table_var;
    bool is_internal = table->is_internal;

    TableRebuildStatistics stats;

    misc_op_mutex.lock();

    // Seek to start.
    fseek(table->data_handle, 0, SEEK_SET);

    // Create temporary paths.
    // Create the data path.
    std::string path = server_config::data_directory + table->header.name + "/";
    
    // Create paths.
    std::string old_data_path = std::string(path).append("data.bin");
    std::string old_dynamic_path = std::string(path).append("dynamic.bin");
    std::string new_data_path = std::string(path).append("data.new.bin");
    std::string new_dynamic_path = std::string(path).append("dynamic.new.bin");

    // Create a file containing the table data.
    fclose(fopen(new_data_path.c_str(), "a"));

    // Create a file containing the table dynamic data.
    fclose(fopen(new_dynamic_path.c_str(), "a"));

    // Open the files in r+w mode.
    FILE* new_data_handle = fopen(new_data_path.c_str(), "r+b");
    FILE* new_dynamic_handle = fopen(new_dynamic_path.c_str(), "r+b");

    // Seek the new handles to correct position.
    fseek(new_dynamic_handle, 0, SEEK_END);
    fseek(new_data_handle, 0, SEEK_END);

    while (true) {
        // Read the header of the record.
        size_t fields_read = fread_unlocked(table->header_buffer, 1, table->header.record_size, table->data_handle);
        if (fields_read == (size_t)-1) {
            logerr("Error return from fread_unlocked while reading records for table rebuild");
            exit(1);
        }

        // End of file has been reached as read didn't return a record size.
        if (fields_read != (size_t)table->header.record_size) break;
        
        Record record(*table, table->header_buffer);
        
        // If the block is empty, skip to the next one.
        if (!record.get_flags()->active) {
            stats.dead_record_count++;
            continue;
        }

        stats.record_count++;
        
        // Check if any dynamic columns need rebuilding.
        for (uint32_t i = 0; i < table->header.num_columns; i++) {
            TableColumn& column = table->actual_header_columns[i];

            // If the column is dynamic.
            if (column.type == ColumnType::String) {
                // Get information about the dynamic data.
                HashedColumnData* entry = record.get_hashed(&column);

                // Allocate space for the dynamic data loading.
                DynamicRecord* dynamic_data = (DynamicRecord*)malloc(sizeof(DynamicRecord) + entry->size);

                // Read the dynamic data to the allocated space.
                ssize_t pread_result = pread(table->dynamic_handle, dynamic_data, sizeof(DynamicRecord) + entry->size, entry->record_location);
                if (pread_result != (ssize_t)(sizeof(DynamicRecord) + entry->size)) {
                    /* Will be improved after disk read overhaul */
                    logerr("Error or incorrect number of bytes returned from pread for dynamic string rebuild");
                    exit(1);
                }

                // Update short string statistic if short.
                if (entry->size != dynamic_data->physical_size - sizeof(DynamicRecord)) stats.short_dynamic_count++;

                // Update the record data for both records.
                dynamic_data->record_location = ftell(new_data_handle);
                dynamic_data->physical_size = entry->size + sizeof(DynamicRecord);
                entry->record_location = ftell(new_dynamic_handle);

                // Write the dynamic data to the new file.
                size_t fwrite_result = fwrite_unlocked(dynamic_data, 1, sizeof(DynamicRecord) + entry->size, new_dynamic_handle);
                if (fwrite_result != sizeof(DynamicRecord) + entry->size) {
                    /* Will be improved after disk read overhaul */
                    logerr("Error or incorrect number of bytes returned from fwrite_unlocked for dynamic string rebuild update");
                    exit(1);
                }

                // Free the allocated dynamic data as it is not needed anymore.
                free(dynamic_data);
            }
        }

        // Write the record to the new data file.
        size_t fwrite_result = fwrite_unlocked((RecordData*)record, 1, table->header.record_size, new_data_handle);
        if (fwrite_result != table->header.record_size) {
            /* Will be improved after disk read overhaul */
            logerr("Error or incorrect number of bytes returned from fwrite_unlocked for record insert in table rebuild");
            exit(1);
        }

        fseek(new_dynamic_handle, 0, SEEK_END);
        fseek(new_data_handle, 0, SEEK_END);
    }

    // Close new files.
    fclose(new_data_handle);
    fclose(new_dynamic_handle);

    // Temporarily copy table name.
    std::string safe_table_name = std::string(table->header.name);

    // Close the table.
    open_tables.erase(table->header.name);
    delete table;

    // Delete old data files.
    remove(old_data_path.c_str());
    remove(old_dynamic_path.c_str());

    // Remove new files to old ones.
    rename(new_data_path.c_str(), old_data_path.c_str());
    rename(new_dynamic_path.c_str(), old_dynamic_path.c_str());

    // Unlock mutex again.
    misc_op_mutex.unlock();

    // Reopen the table and replace variable with new pointer.
    *table_var = new ActiveTable(safe_table_name.c_str(), is_internal);
    open_tables[table->header.name] = *table_var;

    return stats;
}