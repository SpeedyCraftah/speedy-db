#include "table.h"
#include "../deps/xxh/xxhash.h"

int ActiveTable::erase_all_records(nlohmann::json& data, int dynamic_count, int limit) {
    bool has_limit = limit == 0 ? false : true;

    record_header* header = (record_header*)malloc(this->record_size);

    int count = 0;

    this->op_mutex.lock();

    // Seek to start.
    fseek(this->data_handle, 0, SEEK_SET);

    // Create a hash of all dynamic columns.
    size_t dynamic_column_hashes[this->header.num_columns];
    if (dynamic_count != 0) compute_dynamic_hashes(dynamic_column_hashes, data["where"]);

    while (has_limit == false || limit != 0) {
        size_t header_location = ftell(this->data_handle);

        // Read the header of the record.
        int fields_read = fread_unlocked(header, 1, this->record_size, this->data_handle);

        // End of file has been reached as read has failed.
        if (fields_read != this->record_size) {
            free(header);
            this->op_mutex.unlock();
            
            return count;
        }

        // If the block is empty, skip to the next one.
        if ((header->flags & record_flags::active) == 0) {
            continue;
        }

        // Check if record passes the conditions.
        if (!validate_record_conditions(header, dynamic_column_hashes, data["where"])) continue;

        // Record has passed the condition.

        // Mark the record as deleted and mark for optimization.
        header->flags &= ~record_flags::active;
        header->flags |= record_flags::available_optimisation;

        // Write the flag header.
        size_t next_record = ftell(this->data_handle);
        fseek(this->data_handle, header_location, SEEK_SET);
        fwrite_unlocked(&header->flags, 1, sizeof(header->flags), this->data_handle);
        fseek(this->data_handle, next_record, SEEK_SET);
        
        ++count;

        if (has_limit) {
            --limit;
            if (limit == 0) break;
        }
    }

    free(header);
    this->op_mutex.unlock();

    return count;
}

int ActiveTable::update_all_records(nlohmann::json& data, int dynamic_count, int limit) {
    bool has_limit = limit == 0 ? false : true;

    record_header* header = (record_header*)malloc(this->record_size);

    int count = 0;

    this->op_mutex.lock();

    // Seek to start.
    fseek(this->data_handle, 0, SEEK_SET);

    // Create a hash of all dynamic columns.
    size_t dynamic_column_hashes[this->header.num_columns];
    if (dynamic_count != 0) compute_dynamic_hashes(dynamic_column_hashes, data["where"]);

    while (has_limit == false || limit != 0) {
        size_t header_location = ftell(this->data_handle);

        // Read the header of the record.
        int fields_read = fread_unlocked(header, 1, this->record_size, this->data_handle);

        // End of file has been reached as read has failed.
        if (fields_read != this->record_size) {
            free(header);
            this->op_mutex.unlock();
            
            return count;
        }

        // If the block is empty, skip to the next one.
        if ((header->flags & record_flags::active) == 0) {
            continue;
        }

        uint8_t* base_data = header->data;

        // Check if record passes the conditions.
        if (!validate_record_conditions(header, dynamic_column_hashes, data["where"])) continue;

        // Record has passed the condition.

        for (auto& item : data["changes"].items()) {
            std::string column_n = item.key();
            table_column& column = this->columns[column_n];
            uint8_t* base = base_data + calculate_offset(column.index);

            if (column.type == types::string) {
                hashed_entry* entry = (hashed_entry*)base;
                std::string column_d = item.value();

                // Hash the dynamic data and store it.
                entry->hash = XXH64(column_d.c_str(), column_d.length(), HASH_SEED);

                // Update the length.
                entry->size = column_d.length() + 1;

                // Load the string header.
                dynamic_record* column_header = (dynamic_record*)malloc(sizeof(dynamic_record));
                
                // Seek to string header.
                fseek(this->dynamic_handle, entry->record_location, SEEK_SET);

                // Read the string.
                fread_unlocked(column_header, 1, sizeof(dynamic_record), this->dynamic_handle);

                // If new data can be updated in the current block.
                if (column_d.length() + 1 <= column_header->physical_size - sizeof(dynamic_record)) {
                    // Write the new string.
                    fwrite_unlocked(column_d.c_str(), 1, column_d.length() + 1, this->dynamic_handle);

                    if (column_d.length() + 1 != column_header->physical_size - sizeof(dynamic_record)) {
                        // Mark the record for possible optimisation.
                        header->flags |= record_flags::available_optimisation;
                    }
                } else {
                    // String needs to be relocated.

                    // Seek to end.
                    fseek(this->dynamic_handle, 0, SEEK_END);

                    // Allocate space for new string block.
                    dynamic_record* dynam_record = (dynamic_record*)malloc(sizeof(dynamic_record) + column_d.length() + 1);
                    dynam_record->physical_size = sizeof(dynamic_record) + column_d.length() + 1;
                    dynam_record->record_location = ftell(this->dynamic_handle);

                    // Copy over the string.
                    memcpy(dynam_record->data, column_d.c_str(), column_d.length() + 1);

                    // Write the new data.
                    fwrite_unlocked(dynam_record, 1, sizeof(dynamic_record) + column_d.length() + 1, this->dynamic_handle);

                    // Update the record data.
                    entry->record_location = dynam_record->record_location;

                    free(dynam_record);
                }

                free(column_header);
            } else if (column.type == types::byte) {
                *(uint8_t*)(base) = item.value();
            } else if (column.type == types::integer) {
                *(int*)(base) = item.value();
            } else if (column.type == types::float32) {
                *(float*)(base) = item.value();
            } else if (column.type == types::long64) {
                *(long*)(base) = item.value();
            }
        }

        // Write the updated data.
        fseek(this->data_handle, header_location, SEEK_SET);
        fwrite_unlocked(header, 1, this->record_size, this->data_handle);

        ++count;

        if (has_limit) {
            --limit;
            if (limit == 0) break;
        }
    }

    free(header);
    this->op_mutex.unlock();

    return count;
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