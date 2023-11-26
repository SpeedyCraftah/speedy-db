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

void ActiveTable::insert_record(nlohmann::json& data) {
    // Create a temporary buffer for the new record and set the flags.
    record_header* header = (record_header*)malloc(this->record_size);
    header->flags = record_flags::active | record_flags::dirty;

    this->op_mutex.lock();

    // Seek to the end.
    fseek(this->data_handle, 0, SEEK_END);

    for (int i = 0; i < this->header.num_columns; i++) {
        // Fetch the column and find the data area for it.
        table_column& column = this->header_columns[i];
        uint8_t* data_area = header->data + calculate_offset(column.index);

        // If the column is dynamic.
        if (column.type == types::string) {
            std::string d = data[column.name];

            hashed_entry* entry = (hashed_entry*)data_area;

            // Store the size.
            entry->size = d.length() + 1;

            // Hash the dynamic data and store it.
            entry->hash = XXH64(d.c_str(), d.length(), HASH_SEED);

            // Store dynamic data location.
            fseek(this->dynamic_handle, 0, SEEK_END);
            entry->record_location = ftell(this->dynamic_handle);

            dynamic_record* dynam_record = (dynamic_record*)malloc(sizeof(dynamic_record) + d.length() + 1);
            dynam_record->record_location = ftell(this->data_handle);
            dynam_record->physical_size = d.length() + 1 + sizeof(dynamic_record);
            
            // Write the data.
            memcpy(dynam_record->data, d.c_str(), d.length() + 1);

            // Write the dynamic record.
            fseek(this->dynamic_handle, 0, SEEK_END);
            fwrite_unlocked(dynam_record, 1, sizeof(dynamic_record) + d.length() + 1, this->dynamic_handle);
            fseek(this->dynamic_handle, 0, SEEK_SET);

            free(dynam_record);
        } else {
            if (column.type == types::byte) *(int8_t*)data_area = data[column.name];
            else if (column.type == types::integer) *(int*)data_area = data[column.name];
            else if (column.type == types::float32) *(float*)data_area = data[column.name];
            else if (column.type == types::long64) *(long*)data_area = data[column.name];
        }
    }

    // Seek to the end.
    fseek(this->data_handle, 0, SEEK_END);

    // Write to the file.
    fwrite_unlocked(header, 1, this->record_size, this->data_handle);

    // Seek back to the start.
    fseek(this->data_handle, 0, SEEK_SET);

    this->op_mutex.unlock();

    // Free memory buffer.
    free(header);
}