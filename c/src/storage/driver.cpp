#include "driver.h"
#include <cstddef>
#include <cstring>
#include <sys/types.h>
#include <sys/stat.h>
#include <stdio.h>
#include <stdlib.h>
#include "../logging/logger.h"
#include <mutex>
#include "../deps/xxh/xxhash.h"

// FIX BUG WHERE EMPTY ENTRIES RETURN AN EMPTY OBJECT NOT NULL

std::mutex mutex;

#define HASH_SEED 8293236
#define TABLE_MAGIC_NUMBER 3829859236

bool table_exists(const char* name) {
    struct stat info;

    mutex.lock();
    std::string path =  "./data/";
    path += name;
    int state = stat(path.c_str(), &info);
    mutex.unlock();

    return state == 0;
}

void create_table(const char* table_name, table_column* columns, int length) {
    mutex.lock();

    // Create the data path.
    std::string path = std::string("./data/").append(table_name);
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
    
    // Create header.
    table_header header;
    header.magic_number = TABLE_MAGIC_NUMBER;
    header.num_columns = length;

    // Copy name.
    strcpy(header.name, table_name);

    // Write header to file.
    fwrite_unlocked(&header, 1, sizeof(table_header), handle);

    // Write all of the columns.
    fwrite_unlocked(columns, 1, sizeof(table_column) * length, handle);

    // Close the file handle.
    fclose(handle);

    mutex.unlock();
}

active_table* open_table(const char* table_name) {
    mutex.lock();

    // Create the data paths.
    std::string path = std::string("./data/").append(table_name);
    std::string meta_path = path + "/meta.bin";
    std::string data_path = path + "/data.bin";
    std::string dynamic_path = path + "/dynamic.bin";

    // Open the files in r+w mode.
    FILE* header_handle = fopen(meta_path.c_str(), "r+b");
    fseek(header_handle, 0, SEEK_SET);
    
    FILE* data_handle = fopen(data_path.c_str(), "r+b");
    fseek(data_handle, 0, SEEK_SET);

    FILE* dynamic_handle = fopen(dynamic_path.c_str(), "r+b");
    fseek(dynamic_handle, 0, SEEK_SET);

    // Create header output.
    table_header header;

    // Read the header.
    fread_unlocked(&header, 1, sizeof(table_header), header_handle);

    // Create an active table struct which also holds the columns.
    active_table* table = (active_table*)calloc(sizeof(active_table) + (sizeof(table_column) * header.num_columns), 1);

    // Copy the handle and header to the struct.
    table->data_handle = data_handle;
    table->dynamic_handle = dynamic_handle;
    table->header = header;

    // Read the columns and write them to the struct.
    fread_unlocked(table->header.columns, 1, sizeof(table_column) * header.num_columns, header_handle);

    // Create a map to hold the columns.
    table->columns = new std::map<std::string, table_column>();

    table->record_size += sizeof(record_header);

    // Load the columns to the map.
    for (int i = 0; i < table->header.num_columns; i++) {
        table_column& column = table->header.columns[i];

        // Keep index order intact.
        column.index = i;

        // Add sizes.
        table->record_data_size += column.size == 0 ? sizeof(hashed_entry) : column.size;
        table->hashed_column_count += column.size == 0 ? 1 : 0;

        (*table->columns)[table->header.columns[i].name] = table->header.columns[i];
    }

    table->record_size += table->record_data_size;
    
    // Add to the map.
    (*open_tables)[table_name] = table;

    // Close header handle.
    fclose(header_handle);

    mutex.unlock();

    return table;
}

void close_table(const char* table_name) {
    // Lock as threads may be writing to the table.
    mutex.lock();

    active_table* table = (*open_tables)[table_name];

    // Close file handles.
    fclose(table->data_handle);
    fclose(table->dynamic_handle);

    // Free the table.
    free(table->columns);
    free(table);

    // Remove from map.
    open_tables->erase(table_name);

    mutex.unlock();
}

// Hashes any dynamic data provided in a where statement to accelerate seeking.
void compute_dynamic_hashes(active_table* table, size_t* output, nlohmann::json& data) {
    for (auto& item : data.items()) {
        table_column& column = (*table->columns)[item.key()];

        // If column is not a string, it does not need a hash.
        if (column.type != types::string || !item.value().is_string()) continue;
        
        // Hash the string using a special accelerated hashing algorithm.
        std::string d = item.value();
        output[column.index] = XXH64(d.c_str(), d.length(), HASH_SEED);
    }
}

inline int calculate_offset(active_table* table, int index) {
    if (index == 0) return 0;

    int total = 0;
    for (int i = 0; i < index; i++) {
        table_column& column = table->header.columns[i];
        total += column.type == types::string ? sizeof(hashed_entry) : column.size;
    }

    return total;
}

void output_numeric_value(nlohmann::json& output, table_column& column, uint8_t* data_area) {
    // Convert and assemble the numeric value.
    if (column.type == types::byte) output[column.name] = *(uint8_t*)data_area;
    else if (column.type == types::float32) output[column.name] = *(float*)data_area;
    else if (column.type == types::integer) output[column.name] = *(int*)data_area;
    else if (column.type == types::long64) output[column.name] = *(long*)data_area;
}

void output_dynamic_value(nlohmann::json& output, table_column& column, active_table* table, uint8_t* data_area) {
    hashed_entry* entry = (hashed_entry*)data_area;
                
    // Seek to the dynamic data.
    fseek(table->dynamic_handle, entry->record_location + sizeof(dynamic_record), SEEK_SET);
    char* buffer = (char*)malloc(entry->size);

    // Read the dynamic data.
    fread_unlocked(buffer, 1, entry->size, table->dynamic_handle);

    // Store the dynamic data and free the buffer.
    output[column.name] = buffer;
    free(buffer);
}

// Validates whether a record satisfies the conditions or not.
bool validate_record_conditions(active_table* table, record_header* header, size_t* dynamic_hashes, nlohmann::json& conditions) {
    for (auto& item : conditions.items()) {
        // Get the column name.
        std::string column_n = item.key();

        // Get the relevant column.
        table_column& column = (*table->columns)[column_n];

        // Get the location of the column in the buffer.
        uint8_t* base = header->data + calculate_offset(table, column.index);

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
                    fseek(table->dynamic_handle, entry->record_location + sizeof(dynamic_record), SEEK_SET);

                    // Read the dynamic data to the allocated space.
                    fread_unlocked(dynamic_data, 1, entry->size, table->dynamic_handle);                    

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
                fseek(table->dynamic_handle, entry->record_location + sizeof(dynamic_record), SEEK_SET);

                // Read the dynamic data to the allocated space.
                fread_unlocked(dynamic_data, 1, column_d.size() + 1, table->dynamic_handle);

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

// Finds the location of a record and returns the address.
long find_record_location(const char* table_name, nlohmann::json& data, int dynamic_count, int seek_direction) {
    active_table* table = (*open_tables)[table_name];
    record_header* header = (record_header*)malloc(table->record_size);

    long end_seek;

    // Prepare the seek position.
    if (seek_direction == 1) fseek(table->data_handle, 0, SEEK_SET);
    else {
        // If seeking is backwards, set the end seek location.
        fseek(table->data_handle, 0, SEEK_END);
        end_seek = ftell(table->data_handle) - table->record_size;
        fseek(table->data_handle, end_seek, SEEK_SET);
    }

    // Create a hash of all dynamic columns.
    size_t dynamic_column_hashes[table->header.num_columns];
    if (dynamic_count != 0) compute_dynamic_hashes(table, dynamic_column_hashes, data);

    while (true) {
        // If the end has been reached for backwards seeking, return no results.
        if (seek_direction == -1 && end_seek < 0) {
            free(header);
            return -1;
        }

        // Read the header of the record.
        int fields_read = fread_unlocked(header, 1, table->record_size, table->data_handle);

        // End of file has been reached as read has failed.
        if (fields_read != table->record_size) {
            // Free header and return no results.
            free(header);
            return -1;
        }

        if (seek_direction == -1) {
            // Seek to next record.
            end_seek -= table->record_size;
            fseek(table->data_handle, end_seek, SEEK_SET);
        }

        // If the block is empty, skip to the next one.
        if ((header->flags & record_flags::active) == 0) continue;

        // Check if record passes the conditions.
        if (!validate_record_conditions(table, header, dynamic_column_hashes, data)) continue;

        // Record has passed the condition.
        break;
    }

    // Free the header as it is not needed anymore.
    free(header);

    // Return the location.
    if (seek_direction == 1) return ftell(table->data_handle) - table->record_size;
    else return end_seek + table->record_size;
}

void insert_record(const char* table_name, nlohmann::json& data) {
    active_table* table = (*open_tables)[table_name];

    // Create a temporary buffer for the new record and set the flags.
    record_header* header = (record_header*)malloc(table->record_size);
    header->flags = record_flags::active | record_flags::dirty;

    mutex.lock();

    // Seek to the end.
    fseek(table->data_handle, 0, SEEK_END);

    for (int i = 0; i < table->header.num_columns; i++) {
        // Fetch the column and find the data area for it.
        table_column& column = table->header.columns[i];
        uint8_t* data_area = header->data + calculate_offset(table, column.index);

        // If the column is dynamic.
        if (column.type == types::string) {
            std::string d = data[column.name];

            hashed_entry* entry = (hashed_entry*)data_area;

            // Store the size.
            entry->size = d.length() + 1;

            // Hash the dynamic data and store it.
            entry->hash = XXH64(d.c_str(), d.length(), HASH_SEED);

            // Store dynamic data location.
            fseek(table->dynamic_handle, 0, SEEK_END);
            entry->record_location = ftell(table->dynamic_handle);

            dynamic_record* dynam_record = (dynamic_record*)malloc(sizeof(dynamic_record) + d.length() + 1);
            dynam_record->record_location = ftell(table->data_handle);
            dynam_record->physical_size = d.length() + 1 + sizeof(dynamic_record);
            
            // Write the data.
            memcpy(dynam_record->data, d.c_str(), d.length() + 1);

            // Write the dynamic record.
            fseek(table->dynamic_handle, 0, SEEK_END);
            fwrite_unlocked(dynam_record, 1, sizeof(dynamic_record) + d.length() + 1, table->dynamic_handle);
            fseek(table->dynamic_handle, 0, SEEK_SET);

            free(dynam_record);
        } else {
            if (column.type == types::byte) *(int8_t*)data_area = data[column.name];
            else if (column.type == types::integer) *(int*)data_area = data[column.name];
            else if (column.type == types::float32) *(float*)data_area = data[column.name];
            else if (column.type == types::long64) *(long*)data_area = data[column.name];
        }
    }

    // Seek to the end.
    fseek(table->data_handle, 0, SEEK_END);

    // Write to the file.
    fwrite_unlocked(header, 1, table->record_size, table->data_handle);

    // Seek back to the start.
    fseek(table->data_handle, 0, SEEK_SET);

    mutex.unlock();

    // Free memory buffer.
    free(header);
}

void assemble_record_data(active_table* table, record_header* header, nlohmann::json& output, nlohmann::json& query, bool limited_results) {
    if (limited_results) {
        // Only populate the requested fields.
        for (auto& column_n : query["return"]) {
            table_column& column = (*table->columns)[column_n];
            uint8_t* data_area = header->data + calculate_offset(table, column.index);

            // If the column is dynamic.
            if (column.type == types::string) output_dynamic_value(output, column, table, data_area);
            
            // If the column is numeric.
            else output_numeric_value(output, column, data_area);
        }
    } else {
        for (int i = 0; i < table->header.num_columns; i++) {
            table_column& column = table->header.columns[i];
            uint8_t* data_area = header->data + calculate_offset(table, column.index);

            // If the column is dynamic.
            if (column.type == types::string) output_dynamic_value(output, column, table, data_area);
            
            // If the column is numeric.
            else output_numeric_value(output, column, data_area);
        }
    }
}

nlohmann::json find_one_record(const char* table_name, nlohmann::json& data, int dynamic_count, int seek_direction, bool limited_results) {
    active_table* table = (*open_tables)[table_name];
    record_header* header = (record_header*)malloc(table->record_size);

    nlohmann::json output_data = nlohmann::json::object_t();

    mutex.lock();

    long end_seek;

    // Seek to start.
    if (seek_direction == 1) fseek(table->data_handle, 0, SEEK_SET);
    else {
        fseek(table->data_handle, 0, SEEK_END);
        end_seek = ftell(table->data_handle) - table->record_size;
        fseek(table->data_handle, end_seek, SEEK_SET);
    }

    // Create a hash of all dynamic columns.
    size_t dynamic_column_hashes[table->header.num_columns];
    if (dynamic_count != 0) compute_dynamic_hashes(table, dynamic_column_hashes, data["where"]);

    while (true) {
        if (seek_direction == -1 && end_seek < 0) break;

        // Read the header of the record.
        int fields_read = fread_unlocked(header, 1, table->record_size, table->data_handle);

        // End of file has been reached as read has failed.
        if (fields_read != table->record_size) {
            free(header);
            mutex.unlock();
            
            return nlohmann::json();
        }

        if (seek_direction == -1) {
            end_seek -= table->record_size;
            fseek(table->data_handle, end_seek, SEEK_SET);
        }

        // If the block is empty, skip to the next one.
        if ((header->flags & record_flags::active) == 0) {
            continue;
        }

        // Check if record passes the conditions.
        if (!validate_record_conditions(table, header, dynamic_column_hashes, data["where"])) continue;

        // Record has passed the condition.
        break;
    }

    // Assemble and fetch all columns.
    assemble_record_data(table, header, output_data, data, limited_results);
    
    free(header);
    mutex.unlock();

    return output_data;
}

nlohmann::json find_all_records(const char* table_name, nlohmann::json& data, int dynamic_count, int limit, int seek_direction, bool limited_results) {
    bool has_limit = limit == 0 ? false : true;

    active_table* table = (*open_tables)[table_name];
    record_header* header = (record_header*)malloc(table->record_size);

    nlohmann::json output_data = nlohmann::json::array();

    mutex.lock();

    long end_seek;

    // Seek to start.
    if (seek_direction == 1) fseek(table->data_handle, 0, SEEK_SET);
    else {
        fseek(table->data_handle, 0, SEEK_END);
        end_seek = ftell(table->data_handle) - table->record_size;
        fseek(table->data_handle, end_seek, SEEK_SET);
    }

    // Create a hash of all dynamic columns.
    size_t dynamic_column_hashes[table->header.num_columns];
    if (dynamic_count != 0) compute_dynamic_hashes(table, dynamic_column_hashes, data["where"]);

    // If there is a position to start at.
    if (data.contains("seek_where")) {
        long location = find_record_location(table_name, data["seek_where"], dynamic_count, seek_direction);

        // Start at the beginning if pivot could not be found.
        if (location != -1) {
            end_seek = location;
            fseek(table->data_handle, location, SEEK_SET);
        }
    }

    while (has_limit == false || limit != 0) {
        if (seek_direction == -1 && end_seek < 0) break;

        // Read the header.
        int fields_read = fread_unlocked(header, 1, table->record_size, table->data_handle);

        // End of file has been reached as read has failed.
        if (fields_read != table->record_size) {
            free(header);
            mutex.unlock();
            
            return output_data;
        }

        if (seek_direction == -1) {
            // Seek to next record.
            end_seek -= table->record_size;
            fseek(table->data_handle, end_seek, SEEK_SET);
        }

        // If the block is empty, skip to the next one.
        if ((header->flags & record_flags::active) == 0) {
            continue;
        }

        // Check if record passes the conditions.
        if (!validate_record_conditions(table, header, dynamic_column_hashes, data["where"])) continue;

        // Record has passed the condition.

        // Prepare the record to be populated.
        nlohmann::json output = nlohmann::json::object_t();

        // Assemble and fetch all columns.
        assemble_record_data(table, header, output, data, limited_results);

        // Push the record object to the output results array.
        output_data.push_back(output);

        // Decrement the limit and if it has been reached, finish.
        if (has_limit) {
            --limit;
            if (limit == 0) break;
        }
    }

    free(header);
    mutex.unlock();

    return output_data;
}

int erase_all_records(const char* table_name, nlohmann::json& data, int dynamic_count, int limit) {
    bool has_limit = limit == 0 ? false : true;

    active_table* table = (*open_tables)[table_name];
    record_header* header = (record_header*)malloc(table->record_size);

    int count = 0;

    mutex.lock();

    // Seek to start.
    fseek(table->data_handle, 0, SEEK_SET);

    // Create a hash of all dynamic columns.
    size_t dynamic_column_hashes[table->header.num_columns];
    if (dynamic_count != 0) compute_dynamic_hashes(table, dynamic_column_hashes, data["where"]);

    while (has_limit == false || limit != 0) {
        size_t header_location = ftell(table->data_handle);

        // Read the header of the record.
        int fields_read = fread_unlocked(header, 1, table->record_size, table->data_handle);

        // End of file has been reached as read has failed.
        if (fields_read != table->record_size) {
            free(header);
            mutex.unlock();
            
            return count;
        }

        // If the block is empty, skip to the next one.
        if ((header->flags & record_flags::active) == 0) {
            continue;
        }

        // Check if record passes the conditions.
        if (!validate_record_conditions(table, header, dynamic_column_hashes, data["where"])) continue;

        // Record has passed the condition.

        // Mark the record as deleted and mark for optimization.
        header->flags &= ~record_flags::active;
        header->flags |= record_flags::available_optimisation;

        // Write the flag header.
        size_t next_record = ftell(table->data_handle);
        fseek(table->data_handle, header_location, SEEK_SET);
        fwrite_unlocked(&header->flags, 1, sizeof(header->flags), table->data_handle);
        fseek(table->data_handle, next_record, SEEK_SET);
        
        ++count;

        if (has_limit) {
            --limit;
            if (limit == 0) break;
        }
    }

    free(header);
    mutex.unlock();

    return count;
}

int update_all_records(const char* table_name, nlohmann::json& data, int dynamic_count, int limit) {
    bool has_limit = limit == 0 ? false : true;

    active_table* table = (*open_tables)[table_name];
    record_header* header = (record_header*)malloc(table->record_size);

    int count = 0;

    mutex.lock();

    // Seek to start.
    fseek(table->data_handle, 0, SEEK_SET);

    // Create a hash of all dynamic columns.
    size_t dynamic_column_hashes[table->header.num_columns];
    if (dynamic_count != 0) compute_dynamic_hashes(table, dynamic_column_hashes, data["where"]);

    while (has_limit == false || limit != 0) {
        size_t header_location = ftell(table->data_handle);

        // Read the header of the record.
        int fields_read = fread_unlocked(header, 1, table->record_size, table->data_handle);

        // End of file has been reached as read has failed.
        if (fields_read != table->record_size) {
            free(header);
            mutex.unlock();
            
            return count;
        }

        // If the block is empty, skip to the next one.
        if ((header->flags & record_flags::active) == 0) {
            continue;
        }

        uint8_t* base_data = header->data;

        // Check if record passes the conditions.
        if (!validate_record_conditions(table, header, dynamic_column_hashes, data["where"])) continue;

        // Record has passed the condition.

        for (auto& item : data["changes"].items()) {
            std::string column_n = item.key();
            table_column& column = (*table->columns)[column_n];
            uint8_t* base = base_data + calculate_offset(table, column.index);

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
                fseek(table->dynamic_handle, entry->record_location, SEEK_SET);

                // Read the string.
                fread_unlocked(column_header, 1, sizeof(dynamic_record), table->dynamic_handle);

                // If new data can be updated in the current block.
                if (column_d.length() + 1 <= column_header->physical_size - sizeof(dynamic_record)) {
                    // Write the new string.
                    fwrite_unlocked(column_d.c_str(), 1, column_d.length() + 1, table->dynamic_handle);

                    if (column_d.length() + 1 != column_header->physical_size - sizeof(dynamic_record)) {
                        // Mark the record for possible optimisation.
                        header->flags |= record_flags::available_optimisation;
                    }
                } else {
                    // String needs to be relocated.

                    // Seek to end.
                    fseek(table->dynamic_handle, 0, SEEK_END);

                    // Allocate space for new string block.
                    dynamic_record* dynam_record = (dynamic_record*)malloc(sizeof(dynamic_record) + column_d.length() + 1);
                    dynam_record->physical_size = sizeof(dynamic_record) + column_d.length() + 1;
                    dynam_record->record_location = ftell(table->dynamic_handle);

                    // Copy over the string.
                    memcpy(dynam_record->data, column_d.c_str(), column_d.length() + 1);

                    // Write the new data.
                    fwrite_unlocked(dynam_record, 1, sizeof(dynamic_record) + column_d.length() + 1, table->dynamic_handle);

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
        fseek(table->data_handle, header_location, SEEK_SET);
        fwrite_unlocked(header, 1, table->record_size, table->data_handle);

        ++count;

        if (has_limit) {
            --limit;
            if (limit == 0) break;
        }
    }

    free(header);
    mutex.unlock();

    return count;
}

table_rebuild_statistics rebuild_table(char* table_name) {
    active_table* table = (*open_tables)[table_name];
    record_header* header = (record_header*)malloc(table->record_size);

    table_rebuild_statistics stats;

    mutex.lock();

    // Seek to start.
    fseek(table->data_handle, 0, SEEK_SET);

    // Create temporary paths.
    // Create the data path.
    std::string path = std::string("./data/").append(table_name).append("/");
    
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

    while (true) {
        size_t header_location = ftell(table->data_handle);

        // Read the header of the record.
        int fields_read = fread_unlocked(header, 1, table->record_size, table->data_handle);

        // End of file has been reached as read has failed.
        if (fields_read != table->record_size) break;

        // If the block is empty, skip to the next one.
        if ((header->flags & record_flags::active) == 0) {
            stats.dead_record_count++;
            continue;
        }

        stats.record_count++;
        
        // Check if any dynamic columns need rebuilding.
        for (uint32_t i = 0; i < table->header.num_columns; i++) {
            table_column& column = table->header.columns[i];

            // Get the location of the column in the buffer.
            uint8_t* base = header->data + calculate_offset(table, column.index);

            // If the column is dynamic.
            if (column.type == types::string) {
                // Get information about the dynamic data.
                hashed_entry* entry = (hashed_entry*)base;

                // Allocate space for the dynamic data loading.
                dynamic_record* dynamic_data = (dynamic_record*)malloc(sizeof(dynamic_record) + entry->size + 1);
                
                // Seek to the dynamic data.
                fseek(table->dynamic_handle, entry->record_location, SEEK_SET);

                // Read the dynamic data to the allocated space.
                fread_unlocked(dynamic_data, 1, sizeof(dynamic_record) + entry->size + 1, table->dynamic_handle);

                // Update short string statistic if short.
                if (entry->size != dynamic_data->physical_size - sizeof(dynamic_record)) stats.short_dynamic_count++;

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
        fwrite(header, 1, table->record_size, new_data_handle);
    }
    
    // Temporarily copy table name.
    std::string safe_table_name = std::string(table_name);

    // Close the table.
    close_table(safe_table_name.c_str());

    // Delete old data files.
    remove(old_data_path.c_str());
    remove(old_dynamic_path.c_str());

    // Remove new files to old ones.
    rename(new_data_path.c_str(), old_data_path.c_str());
    rename(new_dynamic_path.c_str(), old_dynamic_path.c_str());

    // Reopen the table.
    open_table(safe_table_name.c_str());

    mutex.unlock();

    free(header);
    fclose(new_data_handle);
    fclose(new_dynamic_handle);

    return stats;
}