#include "compiled-query.h"
#include "table-reusable-types.h"
#include "table.h"

bool ActiveTable::verify_record_conditions_match(record_header* record, query_compiler::QueryComparison* conditions, uint32_t conditions_length) {
    // Go over all conditions for record.
    for (uint32_t i = 0; i < conditions_length; i++) {
        query_compiler::QueryComparison& generic_cmp = conditions[i];
        table_column& column = this->header_columns[generic_cmp.generic.column_index];
        NumericType* data = (NumericType*)(record->data + column.buffer_offset);

        switch (generic_cmp.generic.op) {
            case query_compiler::where_compare_op::NUMERIC_EQUAL: {
                query_compiler::NumericQueryComparison& cmp =  generic_cmp.numeric;
                
                switch (column.type) {
                    case types::byte: if (cmp.comparator.byte != data->byte) return false; break;
                    case types::long64: if (cmp.comparator.long64 != data->long64) return false; break;

                    // Guaranteed to be 4 bytes in length.
                    default: if (cmp.comparator.unsigned32_raw != data->unsigned32_raw) return false; break;
                }

                break;
            }

            case query_compiler::where_compare_op::NUMERIC_GREATER_THAN: {
                query_compiler::NumericQueryComparison& cmp =  generic_cmp.numeric;
                switch (column.type) {
                    case types::byte: if (cmp.comparator.byte >= data->byte) return false; break;
                    case types::float32: if (cmp.comparator.float32 >= data->float32) return false; break;
                    case types::long64: if (cmp.comparator.long64 >= data->long64) return false; break;
                    case types::integer: if (cmp.comparator.int32 >= data->int32) return false; break;
                    default: {};
                }

                break;
            }

            case query_compiler::where_compare_op::NUMERIC_GREATER_THAN_EQUAL_TO: {
                query_compiler::NumericQueryComparison& cmp =  generic_cmp.numeric;
                switch (column.type) {
                    case types::byte: if (cmp.comparator.byte > data->byte) return false; break;
                    case types::float32: if (cmp.comparator.float32 > data->float32) return false; break;
                    case types::long64: if (cmp.comparator.long64 > data->long64) return false; break;
                    case types::integer: if (cmp.comparator.int32 > data->int32) return false; break;
                    default: {};
                }

                break;
            }

            case query_compiler::where_compare_op::NUMERIC_LESS_THAN: {
                query_compiler::NumericQueryComparison& cmp =  generic_cmp.numeric;
                switch (column.type) {
                    case types::byte: if (cmp.comparator.byte <= data->byte) return false; break;
                    case types::float32: if (cmp.comparator.float32 <= data->float32) return false; break;
                    case types::long64: if (cmp.comparator.long64 <= data->long64) return false; break;
                    case types::integer: if (cmp.comparator.int32 <= data->int32) return false; break;
                    default: {};
                }

                break;
            }

            case query_compiler::where_compare_op::NUMERIC_LESS_THAN_EQUAL_TO: {
                query_compiler::NumericQueryComparison& cmp =  generic_cmp.numeric;
                switch (column.type) {
                    case types::byte: if (cmp.comparator.byte < data->byte) return false; break;
                    case types::float32: if (cmp.comparator.float32 < data->float32) return false; break;
                    case types::long64: if (cmp.comparator.long64 < data->long64) return false; break;
                    case types::integer: if (cmp.comparator.int32 < data->int32) return false; break;
                    default: {};
                }

                break;
            }

            case query_compiler::where_compare_op::STRING_EQUAL: {
                query_compiler::StringQueryComparison& cmp = generic_cmp.string;
                hashed_entry* entry = (hashed_entry*)data;

                if (entry->hash != cmp.comparator_hash) return false;
                if (entry->size != cmp.comparator.size()) return false;

                // Allocate space for the dynamic data loading.
                char* dynamic_data = (char*)malloc(entry->size);
                
                // Read the dynamic data to the allocated space.
                pread(this->dynamic_handle, dynamic_data, entry->size, entry->record_location + sizeof(dynamic_record));

                // Compare the data character by character to 100% confirm they are a match.
                // Safe to use memcmp since they are guaranteed to be same size.
                int match_result = memcmp(dynamic_data, cmp.comparator.data(), entry->size);

                // Free the allocated dynamic data as it is not needed anymore.
                free(dynamic_data);

                // Check if the data is not a match.
                if (match_result != 0) return false;

                break;
            }

            case query_compiler::where_compare_op::STRING_CONTAINS: {
                query_compiler::StringQueryComparison& cmp = generic_cmp.string;
                hashed_entry* entry = (hashed_entry*)data;

                // Allocate space for the dynamic data loading.
                char* dynamic_data = (char*)malloc(entry->size);
                std::string_view dynamic_data_sv = std::string_view(dynamic_data, entry->size);
                
                // Read the dynamic data to the allocated space.
                pread(this->dynamic_handle, dynamic_data, entry->size, entry->record_location + sizeof(dynamic_record));

                // Check if the string contains the item.
                size_t match_result = dynamic_data_sv.find(cmp.comparator);

                // Free the allocated dynamic data as it is not needed anymore.
                free(dynamic_data);

                // Check if the data does not contain the string.
                if (match_result == std::string_view::npos) return false;

                break;
            }
        }
    }

    return true;
}

void ActiveTable::assemble_record_data_to_json(record_header* record, size_t included_columns, rapidjson::Document& output) {
    output.SetObject();
    for (uint32_t i = 0; i < this->header.num_columns; i++) {
        if ((included_columns & (1 << i)) == 0) continue;
        table_column& column = this->header_columns[i];
        std::string_view column_name(column.name, column.name_length);

        NumericType* data = (NumericType*)(record->data + column.buffer_offset);
        switch (column.type) {
            case types::string: {
                hashed_entry* entry = (hashed_entry*)data;
                
                char* buffer = (char*)output.GetAllocator().Malloc(entry->size);
                std::string_view buffer_sv(buffer, entry->size);

                // Read the dynamic data.
                pread(this->dynamic_handle, buffer, entry->size, entry->record_location + sizeof(dynamic_record));

                // Store the dynamic data and free the buffer.
                output.AddMember(rapidjson_string_view(column_name), rapidjson_string_view(buffer_sv), output.GetAllocator());
                
                break;
            }

            case types::byte: output.AddMember(rapidjson_string_view(column_name), data->byte, output.GetAllocator()); break;
            case types::float32: output.AddMember(rapidjson_string_view(column_name), data->float32, output.GetAllocator()); break;
            case types::integer: output.AddMember(rapidjson_string_view(column_name), data->int32, output.GetAllocator()); break;
            case types::long64: output.AddMember(rapidjson_string_view(column_name), data->long64, output.GetAllocator()); break;
        }
    }
}

bool ActiveTable::find_one_record(query_compiler::CompiledFindQuery* query, rapidjson::Document& result) {
    this->op_mutex.lock();

    size_t offset_counter = query->offset;
    for (record_header* r_header : *this) {
        // If the block is empty, skip to the next one.
        if ((r_header->flags & record_flags::active) == 0) continue;

        // Check if record matches conditions.
        if (verify_record_conditions_match(r_header, query->conditions, query->conditions_count)) {
            // Ignore matched records until offset runs out.
            if (offset_counter != 0) {
                --offset_counter;
                continue;
            }

            assemble_record_data_to_json(r_header, query->columns_returned, result);

            this->op_mutex.unlock();
            return true;
        }
    }

    this->op_mutex.unlock();
    return false;
}

void ActiveTable::find_many_records(query_compiler::CompiledFindQuery* query, rapidjson::Document& result) {
    this->op_mutex.lock();
    result.SetArray();

    size_t offset_counter = query->offset;
    for (record_header* r_header : *this) {
        // If the block is empty, skip to the next one.
        if ((r_header->flags & record_flags::active) == 0) continue;

        // Check if record matches conditions.
        if (verify_record_conditions_match(r_header, query->conditions, query->conditions_count)) {
            // Ignore matched records until offset runs out.
            if (offset_counter != 0) {
                --offset_counter;
                continue;
            }

            rapidjson::Document record(&result.GetAllocator());
            assemble_record_data_to_json(r_header, query->columns_returned, record);

            result.PushBack(record, result.GetAllocator());
            if (query->limit != 0 && result.Size() == query->limit) break;
        }
    }

    this->op_mutex.unlock();
}