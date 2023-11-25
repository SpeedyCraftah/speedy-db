#include "table.h"
#include "driver.h"
#include <cstddef>
#include <unordered_map>
#include "../deps/xxh/xxhash.h"

// TODO - add mutex
// TODO - preallocate record header for each handle instead of mallocing and freeing on every query

ActiveTable::ActiveTable(const char* table_name, bool is_internal = false) : is_internal(is_internal) {
    // Create the data paths.
    std::string path = std::string("./data/").append(table_name);
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
    fseek(data_handle, 0, SEEK_SET);

    this->dynamic_handle = fopen(dynamic_path.c_str(), "r+b");
    fseek(dynamic_handle, 0, SEEK_SET);

    // Read the header.
    fread_unlocked(&this->header, 1, sizeof(table_header), header_handle);

    this->record_size += sizeof(record_header);

    // Load the columns to the map.
    for (int i = 0; i < this->header.num_columns; i++) {
        table_column& column = this->header.columns[i];

        // Keep index order intact.
        column.index = i;

        // Add sizes.
        this->record_data_size += column.size == 0 ? sizeof(hashed_entry) : column.size;
        this->hashed_column_count += column.size == 0 ? 1 : 0;

        this->columns[this->header.columns[i].name] = this->header.columns[i];
    }

    this->record_size += this->record_data_size;

    // Load the account permissions if table is not internal as internal tables do not have custom permissions.
    if (this->is_internal) {
        // Create permission map instance.
        this->permissions = new std::unordered_map<size_t, TablePermissions>();

        // todo
    }

    // Add table to the map.
    // TODO - see if this is a bad idea
    (*open_tables)[table_name] = this;

    // Close handles which are not needed.
    fclose(header_handle);
    fclose(permissions_handle);
}

ActiveTable::~ActiveTable() {
    // Close handles.
    fclose(this->data_handle);
    fclose(this->dynamic_handle);

    // Free permissions if needed.
    if (this->permissions != nullptr) delete this->permissions;

    // TODO - see if this is a bad idea
    // Remove table from map.
    open_tables->erase(this->header.name);
}

void ActiveTable::compute_dynamic_hashes(size_t* output, nlohmann::json& data) {
    for (auto& item : data.items()) {
        table_column& column = this->columns[item.key()];

        // If column is not a string, it does not need a hash.
        if (column.type != types::string || !item.value().is_string()) continue;
        
        // Hash the string using a special accelerated hashing algorithm.
        std::string d = item.value();
        output[column.index] = XXH64(d.c_str(), d.length(), HASH_SEED);
    }
}

int ActiveTable::calculate_offset(int index) {
    if (index == 0) return 0;

    int total = 0;
    for (int i = 0; i < index; i++) {
        table_column& column = this->header.columns[i];
        total += column.type == types::string ? sizeof(hashed_entry) : column.size;
    }

    return total;
}

bool ActiveTable::validate_record_conditions(record_header* r_header, size_t* dynamic_hashes, nlohmann::json& conditions) {
    for (auto& item : conditions.items()) {
        // Get the column name.
        std::string column_n = item.key();

        // Get the relevant column.
        table_column& column = this->columns[column_n];

        // Get the location of the column in the buffer.
        uint8_t* base = r_header->data + calculate_offset(column.index);

        // If the column is dynamic.
        if (column.type == types::string) {
            // Get information about the dynamic data.
            hashed_entry* entry = (hashed_entry*)base;

            if (item.value().is_object()) {
                if (item.value().contains("contains")) {
                    // Grab the string include.
                    std::string column_d = item.value()["contains"];

                    // Check if the size is smaller.
                    if (column_d.size() > entry->size - 1) return false;

                    // Allocate space for the dynamic data loading.
                    char* dynamic_data = (char*)malloc(entry->size);
                    
                    // Seek to the dynamic data.
                    fseek(this->dynamic_handle, entry->record_location + sizeof(dynamic_record), SEEK_SET);

                    // Read the dynamic data to the allocated space.
                    fread_unlocked(dynamic_data, 1, entry->size, this->dynamic_handle);                    

                    // Check if the string contains the item.
                    bool match_result = strstr(dynamic_data, column_d.c_str()) != NULL;

                    // Free the allocated dynamic data as it is not needed anymore.
                    free(dynamic_data);

                    // Check if the data does not contain the string.
                    if (match_result == false) return false;
                }
            } else {
                // Grab the string contents of the condition.
                std::string column_d = item.value();

                // Check if the lengths match.
                if (entry->size != column_d.size() + 1) return false;

                // Check if the hashes match.
                if (entry->hash != dynamic_hashes[column.index]) return false;

                // Allocate space for the dynamic data loading.
                char* dynamic_data = (char*)malloc(column_d.size() + 1);
                
                // Seek to the dynamic data.
                fseek(this->dynamic_handle, entry->record_location + sizeof(dynamic_record), SEEK_SET);

                // Read the dynamic data to the allocated space.
                fread_unlocked(dynamic_data, 1, column_d.size() + 1, this->dynamic_handle);

                // Compare the data character by character to 100% confirm they are a match.
                bool match_result = column_d.compare(dynamic_data);

                // Free the allocated dynamic data as it is not needed anymore.
                free(dynamic_data);

                // Check if the data is not a match.
                if (match_result != 0) return false;
            }

        // If the type is a numeric value.
        } else if (column.type <= types::byte) {
            // If the value is an object, it is an advanced condition.
            if (item.value().is_object()) {
                nlohmann::json& column_d = item.value();

                // Check for the relevant mathematical conditions.
                if (column_d.contains("greater_than")) {
                    auto d = column_d["greater_than"];

                    if (
                        (column.type == types::byte && *(int8_t*)base <= (int8_t)d) ||
                        (column.type == types::integer && *(int*)base <= (int)d) ||
                        (column.type == types::float32 && *(float*)base <= (float)d) ||
                        (column.type == types::long64 && *(long*)base <= (long)d)
                    ) return false;
                } else if (column_d.contains("greater_than_equal_to")) {
                    auto d = column_d["greater_than_equal_to"];

                    if (
                        (column.type == types::byte && *(int8_t*)base < (int8_t)d) ||
                        (column.type == types::integer && *(int*)base < (int)d) ||
                        (column.type == types::float32 && *(float*)base < (float)d) ||
                        (column.type == types::long64 && *(long*)base < (long)d)
                    ) return false;
                }
                
                if (column_d.contains("less_than")) {
                    auto d = column_d["less_than"];

                    if (
                        (column.type == types::byte && *(int8_t*)base >= (int8_t)d) ||
                        (column.type == types::integer && *(int*)base >= (int)d) ||
                        (column.type == types::float32 && *(float*)base >= (float)d) ||
                        (column.type == types::long64 && *(long*)base >= (long)d)
                    ) return false;
                } else if (column_d.contains("less_than_equal_to")) {
                    auto d = column_d["less_than_equal_to"];

                    if (
                        (column.type == types::byte && *(int8_t*)base > (int8_t)d) ||
                        (column.type == types::integer && *(int*)base > (int)d) ||
                        (column.type == types::float32 && *(float*)base > (float)d) ||
                        (column.type == types::long64 && *(long*)base > (long)d)
                    ) return false;
                }
            } else {
                // Perform a simple number equality compare.
                if (
                    (column.type == types::byte && *base != (uint8_t)item.value()) ||
                    (column.type == types::integer && *(int*)base != (int)item.value()) ||
                    (column.type == types::float32 && *(float*)base != (float)item.value()) ||
                    (column.type == types::long64 && *(long*)base != (long)item.value())
                ) return false;
            }
        }
    }

    // All conditions have been passed.
    return true;
}

void ActiveTable::output_numeric_value(nlohmann::json& output, table_column& column, uint8_t* data_area) {
    // Convert and assemble the numeric value.
    if (column.type == types::byte) output[column.name] = *(uint8_t*)data_area;
    else if (column.type == types::float32) output[column.name] = *(float*)data_area;
    else if (column.type == types::integer) output[column.name] = *(int*)data_area;
    else if (column.type == types::long64) output[column.name] = *(long*)data_area;
}

void ActiveTable::output_dynamic_value(nlohmann::json& output, table_column& column, uint8_t* data_area) {
    hashed_entry* entry = (hashed_entry*)data_area;
                
    // Seek to the dynamic data.
    fseek(this->dynamic_handle, entry->record_location + sizeof(dynamic_record), SEEK_SET);
    char* buffer = (char*)malloc(entry->size);

    // Read the dynamic data.
    fread_unlocked(buffer, 1, entry->size, this->dynamic_handle);

    // Store the dynamic data and free the buffer.
    output[column.name] = buffer;
    free(buffer);
}

void ActiveTable::assemble_record_data(record_header* header, nlohmann::json& output, nlohmann::json& query, bool limited_results) {
    if (limited_results) {
        // Only populate the requested fields.
        for (auto& column_n : query["return"]) {
            table_column& column = this->columns[column_n];
            uint8_t* data_area = header->data + calculate_offset(column.index);

            // If the column is dynamic.
            if (column.type == types::string) output_dynamic_value(output, column, data_area);
            
            // If the column is numeric.
            else output_numeric_value(output, column, data_area);
        }
    } else {
        for (int i = 0; i < this->header.num_columns; i++) {
            table_column& column = this->header.columns[i];
            uint8_t* data_area = header->data + calculate_offset(column.index);

            // If the column is dynamic.
            if (column.type == types::string) output_dynamic_value(output, column, data_area);
            
            // If the column is numeric.
            else output_numeric_value(output, column, data_area);
        }
    }
}

table_rebuild_statistics ActiveTable::rebuild_table() {
    record_header* header = (record_header*)malloc(this->record_size);

    table_rebuild_statistics stats;

    this->op_mutex.lock();

    // Seek to start.
    fseek(this->data_handle, 0, SEEK_SET);

    // Create temporary paths.
    // Create the data path.
    std::string path = std::string("./data/").append(this->header.name).append("/");
    
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
        size_t header_location = ftell(this->data_handle);

        // Read the header of the record.
        int fields_read = fread_unlocked(header, 1, this->record_size, this->data_handle);

        // End of file has been reached as read has failed.
        if (fields_read != this->record_size) break;

        // If the block is empty, skip to the next one.
        if ((header->flags & record_flags::active) == 0) {
            stats.dead_record_count++;
            continue;
        }

        stats.record_count++;
        
        // Check if any dynamic columns need rebuilding.
        for (uint32_t i = 0; i < this->header.num_columns; i++) {
            table_column& column = this->header.columns[i];

            // Get the location of the column in the buffer.
            uint8_t* base = header->data + calculate_offset(column.index);

            // If the column is dynamic.
            if (column.type == types::string) {
                // Get information about the dynamic data.
                hashed_entry* entry = (hashed_entry*)base;

                // Allocate space for the dynamic data loading.
                dynamic_record* dynamic_data = (dynamic_record*)malloc(sizeof(dynamic_record) + entry->size + 1);
                
                // Seek to the dynamic data.
                fseek(this->dynamic_handle, entry->record_location, SEEK_SET);

                // Read the dynamic data to the allocated space.
                fread_unlocked(dynamic_data, 1, sizeof(dynamic_record) + entry->size + 1, this->dynamic_handle);

                // Update short string statistic if short.
                if (entry->size + 1 != dynamic_data->physical_size - sizeof(dynamic_record)) stats.short_dynamic_count++;

                // Update the record data for both records.
                dynamic_data->record_location = ftell(new_data_handle);
                dynamic_data->physical_size = entry->size + 1 + sizeof(dynamic_record);
                entry->record_location = ftell(new_dynamic_handle);

                // Write the dynamic data to the new file.
                fwrite_unlocked(dynamic_data, 1, sizeof(dynamic_record) + entry->size + 1, new_dynamic_handle);

                // Free the allocated dynamic data as it is not needed anymore.
                free(dynamic_data);
            }
        }

        // Write the record to the new data file.
        fwrite(header, 1, this->record_size, new_data_handle);

        fseek(new_dynamic_handle, 0, SEEK_END);
        fseek(new_data_handle, 0, SEEK_END);
    }

    // Close new files.
    fclose(new_data_handle);
    fclose(new_dynamic_handle);

    // Temporarily copy table name.
    std::string safe_table_name = std::string(this->header.name);

    // Unlock mutex for table close.
    this->op_mutex.unlock();

    // Close the table.
    close_table(safe_table_name.c_str());

    // Acquire mutex again.
    this->op_mutex.lock();

    // Delete old data files.
    remove(old_data_path.c_str());
    remove(old_dynamic_path.c_str());

    // Remove new files to old ones.
    rename(new_data_path.c_str(), old_data_path.c_str());
    rename(new_dynamic_path.c_str(), old_dynamic_path.c_str());

    // Unlock mutex again.
    this->op_mutex.unlock();

    // Reopen the table.
    open_table(safe_table_name.c_str());

    free(header);

    return stats;
}