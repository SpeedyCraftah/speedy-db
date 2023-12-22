#include "compiled-query.h"
#include "table.h"
#include <cstdio>

void ActiveTable::insert_record(query_compiler::CompiledInsertQuery* query) {
    this->op_mutex.lock();

    record_header* r_header = this->header_buffer;
    
    // Set default flags.
    r_header->flags = record_flags::active | record_flags::dirty;

    // Seek to end of data file.
    fseek(this->data_handle, 0, SEEK_END);

    for (uint32_t i = 0; i < this->header.num_columns; i++) {
        table_column& column = this->header_columns[i];
        uint8_t* data_area = r_header->data + column.buffer_offset;

        // If the column is dynamic.
        if (column.type == types::string) {
            query_compiler::StringInsertColumn* column_data = reinterpret_cast<query_compiler::StringInsertColumn*>(&query->values[i]);
            hashed_entry* entry = (hashed_entry*)data_area;

            size_t data_length = column_data->data.length();

            // Store the size and hash of string.
            entry->size = data_length;
            entry->hash = column_data->data_hash;

            // Store dynamic data location.
            fseek(this->dynamic_handle, 0, SEEK_END);
            entry->record_location = ftell(this->dynamic_handle);

            // TODO - make this a preallocated buffer/stack instead depending on size?
            dynamic_record* dynam_record = (dynamic_record*)malloc(sizeof(dynamic_record) + data_length);
            dynam_record->record_location = ftell(this->data_handle);
            dynam_record->physical_size = data_length + sizeof(dynamic_record);

            // Write the data.
            // TODO - directly write string instead of copying to dynam_record? test performance
            memcpy(dynam_record->data, column_data->data.data(), data_length);

            // Write the dynamic record.
            fseek(this->dynamic_handle, 0, SEEK_END);
            fwrite_unlocked(dynam_record, 1, sizeof(dynamic_record) + data_length, this->dynamic_handle);
            fseek(this->dynamic_handle, 0, SEEK_SET);

            free(dynam_record);
        } 
        
        // Column is numeric.
        // TODO - test if memcpy is faster/slower than dereferencing and copying.
        else {
            query_compiler::NumericInsertColumn* column_data = reinterpret_cast<query_compiler::NumericInsertColumn*>(&query->values[i]);
            switch (column.type) {
                case types::byte: *(int8_t*)data_area = *(int8_t*)&column_data->data; break;
                case types::long64: *(long*)data_area = *(long*)&column_data->data; break;
                
                // Rest are 4 byte long values.
                default: *(uint32_t*)data_area = *(uint32_t*)&column_data->data; break;
            }
        }
    }

    // Write to the file.
    fwrite_unlocked(r_header, 1, this->record_size, this->data_handle);

    this->op_mutex.unlock();
}