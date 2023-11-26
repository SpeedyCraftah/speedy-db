#include "table.h"

nlohmann::json ActiveTable::find_one_record(nlohmann::json& data, int dynamic_count, int seek_direction, bool limited_results) {
    record_header* r_header = (record_header*)malloc(this->record_size);

    nlohmann::json output_data = nlohmann::json::object_t();

    this->op_mutex.lock();

    long end_seek;

    // Seek to start.
    if (seek_direction == 1) fseek(this->data_handle, 0, SEEK_SET);
    else {
        fseek(this->data_handle, 0, SEEK_END);
        end_seek = ftell(this->data_handle) - this->record_size;
        fseek(this->data_handle, end_seek, SEEK_SET);
    }

    // Create a hash of all dynamic columns.
    size_t dynamic_column_hashes[this->header.num_columns];
    if (dynamic_count != 0) compute_dynamic_hashes(dynamic_column_hashes, data["where"]);

    while (true) {
        if (seek_direction == -1 && end_seek < 0) break;

        // Read the header of the record.
        int fields_read = fread_unlocked(r_header, 1, this->record_size, this->data_handle);

        // End of file has been reached as read has failed.
        if (fields_read != this->record_size) {
            free(r_header);
            this->op_mutex.unlock();
            return nlohmann::json();
        }

        if (seek_direction == -1) {
            end_seek -= this->record_size;
            fseek(this->data_handle, end_seek, SEEK_SET);
        }

        // If the block is empty, skip to the next one.
        if ((r_header->flags & record_flags::active) == 0) {
            continue;
        }

        // Check if record passes the conditions.
        if (!validate_record_conditions(r_header, dynamic_column_hashes, data["where"])) continue;

        // Record has passed the condition.
        break;
    }

    // Assemble and fetch all columns.
    assemble_record_data(r_header, output_data, data, limited_results);
    
    free(r_header);
    this->op_mutex.unlock();

    return output_data;
}

nlohmann::json ActiveTable::find_all_records(nlohmann::json& data, int dynamic_count, int limit, int seek_direction, bool limited_results) {
    bool has_limit = limit == 0 ? false : true;

    record_header* header = (record_header*)malloc(this->record_size);

    nlohmann::json output_data = nlohmann::json::array();

    this->op_mutex.lock();

    long end_seek;

    // Seek to start.
    if (seek_direction == 1) fseek(this->data_handle, 0, SEEK_SET);
    else {
        fseek(this->data_handle, 0, SEEK_END);
        end_seek = ftell(this->data_handle) - this->record_size;
        fseek(this->data_handle, end_seek, SEEK_SET);
    }

    // Create a hash of all dynamic columns.
    size_t dynamic_column_hashes[this->header.num_columns];
    if (dynamic_count != 0) compute_dynamic_hashes(dynamic_column_hashes, data["where"]);

    // If there is a position to start at.
    if (data.contains("seek_where")) {
        long location = find_record_location(data["seek_where"], dynamic_count, seek_direction);

        // Start at the beginning if pivot could not be found.
        if (location != -1) {
            end_seek = location;
            fseek(this->data_handle, location, SEEK_SET);
        }
    }

    while (has_limit == false || limit != 0) {
        if (seek_direction == -1 && end_seek < 0) break;

        // Read the header.
        int fields_read = fread_unlocked(header, 1, this->record_size, this->data_handle);

        // End of file has been reached as read has failed.
        if (fields_read != this->record_size) {
            free(header);
            this->op_mutex.unlock();
            
            return output_data;
        }

        if (seek_direction == -1) {
            // Seek to next record.
            end_seek -= this->record_size;
            fseek(this->data_handle, end_seek, SEEK_SET);
        }

        // If the block is empty, skip to the next one.
        if ((header->flags & record_flags::active) == 0) {
            continue;
        }

        // Check if record passes the conditions.
        if (!validate_record_conditions(header, dynamic_column_hashes, data["where"])) continue;

        // Record has passed the condition.

        // Prepare the record to be populated.
        nlohmann::json output = nlohmann::json::object_t();

        // Assemble and fetch all columns.
        assemble_record_data(header, output, data, limited_results);

        // Push the record object to the output results array.
        output_data.push_back(output);

        // Decrement the limit and if it has been reached, finish.
        if (has_limit) {
            --limit;
            if (limit == 0) break;
        }
    }

    free(header);
    this->op_mutex.unlock();

    return output_data;
}