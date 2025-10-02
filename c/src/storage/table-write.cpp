#include "compiled-query.h"
#include "table.h"
#include <cstdio>
#include <unistd.h>

void ActiveTable::insert_record(query_compiler::CompiledInsertQuery* query) {
    this->op_mutex.lock();

    TableRecordHeader* r_header = this->header_buffer;
    
    // Set default flags.
    r_header->flags = TableRecordFlags::active | TableRecordFlags::dirty;

    // Seek to end of data file.
    fseek(this->data_handle, 0, SEEK_END);

    for (uint32_t i = 0; i < this->header.num_columns; i++) {
        TableColumn& column = this->header_columns[i];
        uint8_t* data_area = r_header->data + column.buffer_offset;

        // If the column is dynamic.
        if (column.type == ColumnType::String) {
            query_compiler::InsertColumn::String& column_data = query->values[i].info.as<query_compiler::InsertColumn::String>();
            TableHashedEntry* entry = (TableHashedEntry*)data_area;

            size_t data_length = column_data.data.length();

            // Store the size and hash of string.
            entry->size = data_length;
            entry->hash = column_data.data_hash;

            // Store dynamic data location.
            entry->record_location = lseek(this->dynamic_handle, 0, SEEK_END);

            // TODO - make this a preallocated buffer/stack instead depending on size?
            DynamicRecord* dynam_record = (DynamicRecord*)malloc(sizeof(DynamicRecord) + data_length);
            dynam_record->record_location = ftell(this->data_handle);
            dynam_record->physical_size = data_length + sizeof(DynamicRecord);

            // Write the data.
            // TODO - directly write string instead of copying to dynam_record? test performance
            memcpy(dynam_record->data, column_data.data.data(), data_length);

            // Write the dynamic record.
            ssize_t write_result = write(this->dynamic_handle, dynam_record, sizeof(DynamicRecord) + data_length);
            if (write_result != (ssize_t)(sizeof(DynamicRecord) + data_length)) {
                /* Will be improved after disk read overhaul */
                logerr("Error or incorrect number of bytes returned from write for dynamic string");
                exit(1);
            }

            free(dynam_record);
        } 
        
        // Column is numeric.
        else {
            query_compiler::InsertColumn::Numeric& column_data = query->values[i].info.as<query_compiler::InsertColumn::Numeric>();
            switch (column.type) {
                case ColumnType::Byte: *(int8_t*)data_area = *(int8_t*)&column_data.data; break;
                case ColumnType::Long64: *(long*)data_area = *(long*)&column_data.data; break;
                
                // Rest are 4 byte long values.
                default: *(uint32_t*)data_area = *(uint32_t*)&column_data.data; break;
            }
        }
    }

    // Write to the file.
    fwrite_unlocked(r_header, 1, this->record_size, this->data_handle);

    this->op_mutex.unlock();
}

size_t ActiveTable::erase_many_records(query_compiler::CompiledEraseQuery* query) {
    this->op_mutex.lock();

    size_t count = 0;
    for (auto it = this->bulk_begin(), end = this->bulk_end(); it != end; ++it) {
        uint32_t available = *it;

        bool changes_made = false;
        for (uint32_t i = 0; i < available; i++) {
            TableRecordHeader* r_header = reinterpret_cast<TableRecordHeader*>(reinterpret_cast<uint8_t*>(this->header_buffer) + (i * this->record_size));
            
            // If the block is empty, skip to the next one.
            if ((r_header->flags & TableRecordFlags::active) == 0) {
                continue;
            }

            // Check if record matches conditions.
            if (verify_record_conditions_match(r_header, query->conditions, query->conditions_count)) {
                // Mark the record as deleted and mark for optimisation.
                r_header->flags &= ~TableRecordFlags::active;
                r_header->flags |= TableRecordFlags::available_optimisation;

                count++;
                changes_made = true;
                if (query->limit != 0 && count == query->limit) break;
            }
        }

        // Write the updated records in bulk with precise handle (if any).
        if (changes_made) {
            ssize_t write_result = pwrite(this->data_handle_precise, this->header_buffer, available * this->record_size, it.bulk_byte_offset());
            if (write_result != (ssize_t)(available * this->record_size)) {
                /* Will be improved after disk read overhaul */
                logerr("Error or incorrect number of bytes returned from write for dynamic string");
                exit(1);
            }
        }
    }

    fflush(this->data_handle);

    this->op_mutex.unlock();
    return count;
}

size_t ActiveTable::update_many_records(query_compiler::CompiledUpdateQuery* query) {
    this->op_mutex.lock();

    size_t count = 0;
    for (auto it = this->bulk_begin(), end = this->bulk_end(); it != end; ++it) {
        uint32_t available = *it;

        bool changes_made = false;
        for (uint32_t i = 0; i < available; i++) {
            TableRecordHeader* r_header = reinterpret_cast<TableRecordHeader*>(reinterpret_cast<uint8_t*>(this->header_buffer) + (i * this->record_size));
            
            // If the block is empty, skip to the next one.
            if ((r_header->flags & TableRecordFlags::active) == 0) {
                continue;
            }

            // Check if record matches conditions.
            if (verify_record_conditions_match(r_header, query->conditions, query->conditions_count)) {
                for (uint32_t j = 0; j < query->changes_count; j++) {
                    query_compiler::UpdateSet& generic_update = query->changes[j];
                    TableColumn& column = this->header_columns[generic_update.column_index];
                    uint8_t* record_data = r_header->data + column.buffer_offset;

                    switch (generic_update.op) {
                        case query_compiler::UpdateChangesOp::NUMERIC_SET: {
                            query_compiler::UpdateSet::Numeric& update = generic_update.info.as<query_compiler::UpdateSet::Numeric>();
                            
                            switch (column.type) {
                                case ColumnType::Byte: *record_data = *(uint8_t*)&update.new_value; break;
                                case ColumnType::Float32: *(float*)record_data = *(float*)&update.new_value; break;
                                case ColumnType::Integer: *(int*)record_data = *(int*)&update.new_value; break;
                                case ColumnType::Long64: *(long*)record_data = *(long*)&update.new_value; break;
                                default: {};
                            }

                            break;
                        }

                        case query_compiler::UpdateChangesOp::STRING_SET: {
                            query_compiler::UpdateSet::String& update = generic_update.info.as<query_compiler::UpdateSet::String>();
                            TableHashedEntry* entry = (TableHashedEntry*)record_data;
                            
                            // Update the parameters.
                            entry->hash = update.new_value_hash;
                            entry->size = update.new_value.size();

                            // Load the string header.
                            DynamicRecord column_header;

                            // Read the string block header.
                            ssize_t pread_result = pread(this->dynamic_handle, &column_header, sizeof(DynamicRecord), entry->record_location);
                            if (pread_result != sizeof(DynamicRecord)) {
                                /* Will be improved after disk read overhaul */
                                logerr("Error or incorrect number of bytes returned from pread for dynamic string");
                                exit(1);
                            }

                            // If new data can fit in current block.
                            if (update.new_value.size() <= column_header.physical_size - sizeof(DynamicRecord)) {
                                // Write the new string.
                                ssize_t pwrite_result = pwrite(this->dynamic_handle, update.new_value.data(), update.new_value.size(), entry->record_location + sizeof(DynamicRecord));
                                if (pwrite_result != (ssize_t)update.new_value.size()) {
                                    /* Will be improved after disk read overhaul */
                                    logerr("Error or incorrect number of bytes returned from pwrite for dynamic string");
                                    exit(1);
                                }
                            } 
                            
                            // Needs relocation.
                            else {
                                // Allocate space for new string block.
                                DynamicRecord* dynam_record = (DynamicRecord*)malloc(sizeof(DynamicRecord) + update.new_value.size());
                                dynam_record->physical_size = sizeof(DynamicRecord) + update.new_value.size();
                                dynam_record->record_location = lseek(this->dynamic_handle, 0, SEEK_END);

                                // Copy over string.
                                memcpy(dynam_record->data, update.new_value.data(), update.new_value.size());

                                // Write the new data.
                                ssize_t write_result = write(this->dynamic_handle, dynam_record, sizeof(DynamicRecord) + update.new_value.size());
                                if (write_result != (ssize_t)(sizeof(DynamicRecord) + update.new_value.size())) {
                                    /* Will be improved after disk read overhaul */
                                    logerr("Error or incorrect number of bytes returned from write for dynamic string");
                                    exit(1);
                                }

                                // Update the record data.
                                entry->record_location = dynam_record->record_location;

                                free(dynam_record);
                            }

                            break;
                        }
                    }
                }

                count++;
                changes_made = true;
                if (query->limit != 0 && count == query->limit) break;
            }
        }

        if (changes_made) {
            ssize_t pwrite_result = pwrite(this->data_handle_precise, this->header_buffer, available * this->record_size, it.bulk_byte_offset());
            if (pwrite_result != (ssize_t)(available * this->record_size)) {
                /* Will be improved after disk read overhaul */
                logerr("Error or incorrect number of bytes returned from pwrite for record updates");
                exit(1);
            }
        }
    }

    fflush(this->data_handle);

    this->op_mutex.unlock();
    return count;
}