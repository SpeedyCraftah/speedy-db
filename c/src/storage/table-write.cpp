#include "compiled-query.h"
#include "structures/record.h"
#include "table-basic.h"
#include "table-iterators.h"
#include "table.h"
#include <cstdio>
#include <unistd.h>

void ActiveTable::insert_record(query_compiler::CompiledInsertQuery* query) {
    this->op_mutex.lock();

    Record record = Record(*this, this->header_buffer);
    
    // Set default flags.
    *record.get_flags() = RecordFlags::Active;

    // Seek to end of data file.
    fseek(this->data_handle, 0, SEEK_END);

    for (uint32_t i = 0; i < this->column_count; i++) {
        TableColumn& column = this->header_columns[i];

        // If the column is dynamic.
        if (column.type == ColumnType::String) {
            query_compiler::InsertColumn::String& column_data = query->values[i].info.as<query_compiler::InsertColumn::String>();
            HashedColumnData* entry = record.get_hashed(&column);

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
            NumericColumnData* data = record.get_numeric(&column);

            switch (column.type) {
                case ColumnType::Byte: data->byte = column_data.data.byte; break;
                case ColumnType::Long64: data->long64 = column_data.data.long64; break;
                
                // Rest are 4 byte long values.
                default: data->unsigned32_raw = column_data.data.unsigned32_raw; break;
            }
        }
    }

    // Write to the file.
    fwrite_unlocked((RecordData*)record, 1, this->header.record_size, this->data_handle);

    this->op_mutex.unlock();
}

size_t ActiveTable::erase_many_records(query_compiler::CompiledEraseQuery* query) {
    this->op_mutex.lock();

    size_t count = 0;
    for (auto info : table_iterator::iterate_bulk(*this)) {
        bool changes_made = false;
        for (uint32_t i = 0; i < info.available; i++) {
            Record record = Record(*this, this->header_buffer + (i * this->header.record_size));
            
            // If the block is empty, skip to the next one.
            if ((*record.get_flags() & RecordFlags::Active) == 0) {
                continue;
            }

            // Check if record matches conditions.
            if (verify_record_conditions_match((RecordData*)record, query->conditions, query->conditions_count)) {
                // Mark the record as deleted.
                *record.get_flags() &= ~RecordFlags::Active;

                count++;
                changes_made = true;
                if (query->limit != 0 && count == query->limit) break;
            }
        }

        // Write the updated records in bulk with precise handle (if any).
        if (changes_made) {
            ssize_t write_result = pwrite(this->data_handle_precise, this->header_buffer, info.available * this->header.record_size, info.byte_offset);
            if (write_result != (ssize_t)(info.available * this->header.record_size)) {
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
    for (auto info : table_iterator::iterate_bulk(*this)) {
        bool changes_made = false;
        for (uint32_t i = 0; i < info.available; i++) {
            Record record = Record(*this, this->header_buffer + (i * this->header.record_size));
            
            // If the block is empty, skip to the next one.
            if ((*record.get_flags() & RecordFlags::Active) == 0) {
                continue;
            }

            // Check if record matches conditions.
            if (verify_record_conditions_match((RecordData*)record, query->conditions, query->conditions_count)) {
                for (uint32_t j = 0; j < query->changes_count; j++) {
                    query_compiler::UpdateSet& generic_update = query->changes[j];

                    switch (generic_update.op) {
                        case query_compiler::UpdateChangesOp::NUMERIC_SET: {
                            query_compiler::UpdateSet::Numeric& update = generic_update.info.as<query_compiler::UpdateSet::Numeric>();
                            NumericColumnData* data = record.get_numeric(generic_update.column);
                            
                            switch (generic_update.column->type) {
                                case ColumnType::Byte: data->byte = update.new_value.byte; break;
                                case ColumnType::Long64: data->long64 = update.new_value.long64; break;
                                
                                // Rest are 4 byte long values.
                                default: data->unsigned32_raw = update.new_value.unsigned32_raw; break;
                            }

                            break;
                        }

                        case query_compiler::UpdateChangesOp::STRING_SET: {
                            query_compiler::UpdateSet::String& update = generic_update.info.as<query_compiler::UpdateSet::String>();
                            HashedColumnData* entry = record.get_hashed(generic_update.column);
                            
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
            ssize_t pwrite_result = pwrite(this->data_handle_precise, this->header_buffer, info.available * this->header.record_size, info.byte_offset);
            if (pwrite_result != (ssize_t)(info.available * this->header.record_size)) {
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