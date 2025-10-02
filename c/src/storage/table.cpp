#include "table.h"
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
#include "structures/types.h"

// TODO - add mutex
// TODO - preallocate record header for each handle instead of mallocing and freeing on every query

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
    std::string permissions_path = path + "/permissions.bin";

    // Open the files in r+w mode.
    FILE* header_handle = fopen(meta_path.c_str(), "r+b");
    fseek(header_handle, 0, SEEK_SET);

    FILE* permissions_handle = fopen(permissions_path.c_str(), "r+b");
    fseek(permissions_handle, 0, SEEK_SET);

    this->data_handle = fopen(data_path.c_str(), "r+b");
    this->data_handle_precise = fileno(this->data_handle);
    fseek(data_handle, 0, SEEK_SET);

    this->dynamic_handle = open(dynamic_path.c_str(), O_RDWR | O_CREAT, 0666);

    // Read the header.
    size_t fread_result = fread_unlocked(&this->header, 1, sizeof(table_header), header_handle);
    if (fread_result != sizeof(table_header)) {
        logerr("Error or incorrect number of bytes returned from fread_unlocked for table header");
        exit(1);
    }

    this->record_size += sizeof(record_header);

    // Allocate memory for columns.
    this->header_columns = (table_column*)calloc(1, sizeof(table_column) * header.num_columns);

    // Read the columns and write them to the struct.
    fread_result = fread_unlocked(this->header_columns, 1, sizeof(table_column) * header.num_columns, header_handle);
    if (fread_result != sizeof(table_column) * header.num_columns) {
        logerr("Error or incorrect number of bytes returned from fread_unlocked for table columns");
        exit(1);
    }

    // Load the columns to the map.
    for (uint32_t i = 0; i < this->header.num_columns; i++) {
        table_column& column = this->header_columns[i];

        // Keep index order intact.
        column.index = i;

        // Add buffer offset location (location of record in buffer).
        column.buffer_offset = this->record_data_size;

        // Add sizes.
        this->record_data_size += column.size == 0 ? sizeof(hashed_entry) : column.size;
        this->hashed_column_count += column.size == 0 ? 1 : 0;

        this->columns[this->header_columns[i].name] = &this->header_columns[i];
    }

    this->record_size += this->record_data_size;

    // Create record buffer.
    this->header_buffer = (record_header*)malloc(this->record_size * BULK_HEADER_READ_COUNT);

    // Set table name.
    this->name = this->header.name;

    // Load the account permissions if table is not internal as internal tables do not have custom permissions.
    if (!this->is_internal) {
        // Create permission map instance.
        this->permissions = new std::unordered_map<long, TablePermissions>();

        // Get permissions table.
        ActiveTable* permissions_table = open_tables["--internal-table-permissions"];

        query_builder::find_query<1> query(permissions_table);
        query.add_where_condition("table", query.string_equal_to(this->name));

        /* for future debugging */
        // bool oopsie = false;
        // std::vector<std::tuple<std::string, long, uint8_t>> elements;

        permissions_table->op_mutex.lock();
        for (auto it = permissions_table->specific_begin(query.build()); !it; ++it) {
            auto record = *it;
            
            uint8_t permissions = record.get_numeric("permissions")->byte;
            long index = record.get_numeric("index")->long64;

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
    fclose(permissions_handle);
}

ActiveTable::~ActiveTable() {
    // Close handles.
    fclose(this->data_handle);
    close(this->dynamic_handle);

    // Free permissions if needed.
    if (this->permissions != nullptr) delete this->permissions;

    // Free columns.
    free(this->header_columns);

    // Free record header.
    free(this->header_buffer);
}

ActiveTable::data_iterator ActiveTable::data_iterator::operator++() {
    buffer_index++;

    if (buffer_index >= buffer_records_available) {
        buffer_index = 0;
        buffer_records_available = request_bulk_records();
    }

    complete = (buffer_index >= buffer_records_available);

    return *this;
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

void create_table(std::string_view table_name, table_column* columns, int length) {
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
    std::string permissions_path = std::string(path).append("permissions.bin");

    // Create a file containing the table metadata.
    fclose(fopen(meta_path.c_str(), "a"));

    // Create a file containing the table data.
    fclose(fopen(data_path.c_str(), "a"));

    // Create a file containing the table dynamic data.
    fclose(fopen(dynamic_path.c_str(), "a"));

    // Create a file containing the table account permissions.
    fclose(fopen(permissions_path.c_str(), "a"));

    // Open the file in r+w mode.
    FILE* handle = fopen(meta_path.c_str(), "r+b");
    
    // Create header.
    table_header header;
    header.magic_number = TABLE_MAGIC_NUMBER;
    header.num_columns = length;

    // Copy name.
    memcpy(header.name, table_name.data(), table_name.length());

    // Write header to file.
    fwrite_unlocked(&header, 1, sizeof(table_header), handle);

    // Write all of the columns.
    fwrite_unlocked(columns, 1, sizeof(table_column) * length, handle);

    // Close the file handle.
    fclose(handle);

    misc_op_mutex.unlock();
}

table_rebuild_statistics rebuild_table(ActiveTable** table_var) {
    ActiveTable* table = *table_var;
    bool is_internal = table->is_internal;
    record_header* header = (record_header*)malloc(table->record_size);

    table_rebuild_statistics stats;

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
        size_t fields_read = fread_unlocked(header, 1, table->record_size, table->data_handle);
        if (fields_read == (size_t)-1) {
            logerr("Error return from fread_unlocked while reading records for table rebuild");
            exit(1);
        }

        // End of file has been reached as read didn't return a record size.
        if (fields_read != (size_t)table->record_size) break;

        // If the block is empty, skip to the next one.
        if ((header->flags & record_flags::active) == 0) {
            stats.dead_record_count++;
            continue;
        }

        stats.record_count++;
        
        // Check if any dynamic columns need rebuilding.
        for (uint32_t i = 0; i < table->header.num_columns; i++) {
            table_column& column = table->header_columns[i];

            // Get the location of the column in the buffer.
            uint8_t* base = header->data + column.buffer_offset;

            // If the column is dynamic.
            if (column.type == ColumnType::String) {
                // Get information about the dynamic data.
                hashed_entry* entry = (hashed_entry*)base;

                // Allocate space for the dynamic data loading.
                dynamic_record* dynamic_data = (dynamic_record*)malloc(sizeof(dynamic_record) + entry->size);

                // Read the dynamic data to the allocated space.
                ssize_t pread_result = pread(table->dynamic_handle, dynamic_data, sizeof(dynamic_record) + entry->size, entry->record_location);
                if (pread_result != (ssize_t)(sizeof(dynamic_record) + entry->size)) {
                    /* Will be improved after disk read overhaul */
                    logerr("Error or incorrect number of bytes returned from pread for dynamic string rebuild");
                    exit(1);
                }

                // Update short string statistic if short.
                if (entry->size != dynamic_data->physical_size - sizeof(dynamic_record)) stats.short_dynamic_count++;

                // Update the record data for both records.
                dynamic_data->record_location = ftell(new_data_handle);
                dynamic_data->physical_size = entry->size + sizeof(dynamic_record);
                entry->record_location = ftell(new_dynamic_handle);

                // Write the dynamic data to the new file.
                size_t fwrite_result = fwrite_unlocked(dynamic_data, 1, sizeof(dynamic_record) + entry->size, new_dynamic_handle);
                if (fwrite_result != sizeof(dynamic_record) + entry->size) {
                    /* Will be improved after disk read overhaul */
                    logerr("Error or incorrect number of bytes returned from fwrite_unlocked for dynamic string rebuild update");
                    exit(1);
                }

                // Free the allocated dynamic data as it is not needed anymore.
                free(dynamic_data);
            }
        }

        // Write the record to the new data file.
        size_t fwrite_result = fwrite_unlocked(header, 1, table->record_size, new_data_handle);
        if (fwrite_result != table->record_size) {
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

    free(header);

    return stats;
}