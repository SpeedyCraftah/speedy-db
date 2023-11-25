#include "table.h"

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