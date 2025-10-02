#include "compiled-query.h"
#include "table.h"
#include <string_view>

using namespace query_compiler;

bool ActiveTable::verify_record_conditions_match(TableRecordHeader* record, query_compiler::QueryComparator* conditions, uint32_t conditions_length) {
    // Go over all conditions for record.
    for (uint32_t i = 0; i < conditions_length; i++) {
        query_compiler::QueryComparator& generic_cmp = conditions[i];
        TableColumn& column = this->header_columns[generic_cmp.column_index];
        NumericColumnData* data = (NumericColumnData*)(record->data + column.buffer_offset);

        switch (generic_cmp.op) {
            case query_compiler::WhereComparoOp::NUMERIC_EQUAL: {
                query_compiler::QueryComparator::Numeric& cmp = generic_cmp.info.as<query_compiler::QueryComparator::Numeric>();
                
                switch (column.type) {
                    case ColumnType::Byte: if ((cmp.comparator.byte != data->byte) ^ generic_cmp.negated) return false; break;
                    case ColumnType::Long64: if ((cmp.comparator.long64 != data->long64) ^ generic_cmp.negated) return false; break;

                    // Guaranteed to be 4 bytes in length.
                    default: if ((cmp.comparator.unsigned32_raw != data->unsigned32_raw) ^ generic_cmp.negated) return false; break;
                }

                break;
            }

            case query_compiler::WhereComparoOp::NUMERIC_GREATER_THAN: {
                query_compiler::QueryComparator::Numeric& cmp =  generic_cmp.info.as<query_compiler::QueryComparator::Numeric>();
                switch (column.type) {
                    case ColumnType::Byte: if ((cmp.comparator.byte >= data->byte) ^ generic_cmp.negated) return false; break;
                    case ColumnType::Float32: if ((cmp.comparator.float32 >= data->float32) ^ generic_cmp.negated) return false; break;
                    case ColumnType::Long64: if ((cmp.comparator.long64 >= data->long64) ^ generic_cmp.negated) return false; break;
                    case ColumnType::Integer: if ((cmp.comparator.int32 >= data->int32) ^ generic_cmp.negated) return false; break;
                    default: {};
                }

                break;
            }

            case query_compiler::WhereComparoOp::NUMERIC_GREATER_THAN_EQUAL_TO: {
                query_compiler::QueryComparator::Numeric& cmp =  generic_cmp.info.as<query_compiler::QueryComparator::Numeric>();
                switch (column.type) {
                    case ColumnType::Byte: if ((cmp.comparator.byte > data->byte) ^ generic_cmp.negated) return false; break;
                    case ColumnType::Float32: if ((cmp.comparator.float32 > data->float32) ^ generic_cmp.negated) return false; break;
                    case ColumnType::Long64: if ((cmp.comparator.long64 > data->long64) ^ generic_cmp.negated) return false; break;
                    case ColumnType::Integer: if ((cmp.comparator.int32 > data->int32) ^ generic_cmp.negated) return false; break;
                    default: {};
                }

                break;
            }

            case query_compiler::WhereComparoOp::NUMERIC_LESS_THAN: {
                query_compiler::QueryComparator::Numeric& cmp =  generic_cmp.info.as<query_compiler::QueryComparator::Numeric>();
                switch (column.type) {
                    case ColumnType::Byte: if ((cmp.comparator.byte <= data->byte) ^ generic_cmp.negated) return false; break;
                    case ColumnType::Float32: if ((cmp.comparator.float32 <= data->float32) ^ generic_cmp.negated) return false; break;
                    case ColumnType::Long64: if ((cmp.comparator.long64 <= data->long64) ^ generic_cmp.negated) return false; break;
                    case ColumnType::Integer: if ((cmp.comparator.int32 <= data->int32) ^ generic_cmp.negated) return false; break;
                    default: {};
                }

                break;
            }

            case query_compiler::WhereComparoOp::NUMERIC_LESS_THAN_EQUAL_TO: {
                query_compiler::QueryComparator::Numeric& cmp =  generic_cmp.info.as<query_compiler::QueryComparator::Numeric>();
                switch (column.type) {
                    case ColumnType::Byte: if ((cmp.comparator.byte < data->byte) ^ generic_cmp.negated) return false; break;
                    case ColumnType::Float32: if ((cmp.comparator.float32 < data->float32) ^ generic_cmp.negated) return false; break;
                    case ColumnType::Long64: if ((cmp.comparator.long64 < data->long64) ^ generic_cmp.negated) return false; break;
                    case ColumnType::Integer: if ((cmp.comparator.int32 < data->int32) ^ generic_cmp.negated) return false; break;
                    default: {};
                }

                break;
            }

            case query_compiler::WhereComparoOp::NUMERIC_IN_LIST: {
                query_compiler::QueryComparator::NumericInList& cmp = generic_cmp.info.as<query_compiler::QueryComparator::NumericInList>();
                
                NumericColumnData value;

                switch (column.type) {
                    case ColumnType::Byte: value.byte = data->byte; break;
                    case ColumnType::Long64: value.long64 = data->long64; break;

                    // Guaranteed to be 4 bytes in length.
                    default: value.unsigned32_raw = data->unsigned32_raw; break;
                }

                if ((!cmp.list.contains(value.unsigned64_raw)) ^ generic_cmp.negated) return false;

                break;
            }

            case query_compiler::WhereComparoOp::STRING_EQUAL: {
                query_compiler::QueryComparator::String& cmp = generic_cmp.info.as<query_compiler::QueryComparator::String>();
                TableHashedEntry* entry = (TableHashedEntry*)data;

                bool condition_passed = false;

                {
                    // Perform some heuristic checks before expensive comparison of string.
                    if (entry->hash != cmp.comparator_hash) goto eq_eval_finished;
                    if (entry->size != cmp.comparator.size()) goto eq_eval_finished;

                    // Allocate space for the dynamic data loading.
                    char* dynamic_data = (char*)malloc(entry->size);
                    
                    // Read the dynamic data to the allocated space.
                    ssize_t pread_result = pread(this->dynamic_handle, dynamic_data, entry->size, entry->record_location + sizeof(DynamicRecord));
                    if (pread_result != entry->size) {
                        /* Will be improved after disk read overhaul */
                        logerr("Error or incorrect number of bytes returned from pread for dynamic string");
                        exit(1);
                    }

                    // Compare the data character by character to 100% confirm they are a match.
                    // Safe to use memcmp since they are guaranteed to be same size.
                    int match_result = memcmp(dynamic_data, cmp.comparator.data(), entry->size);

                    // Free the allocated dynamic data as it is not needed anymore.
                    free(dynamic_data);

                    // Check if the data is not a match.
                    if (match_result != 0) goto eq_eval_finished;

                    // String is a match.
                    condition_passed = true;
                }

                eq_eval_finished:
                if (!condition_passed ^ generic_cmp.negated) return false;

                break;
            }

            case query_compiler::WhereComparoOp::STRING_CONTAINS: {
                query_compiler::QueryComparator::String& cmp = generic_cmp.info.as<query_compiler::QueryComparator::String>();
                TableHashedEntry* entry = (TableHashedEntry*)data;

                bool condition_passed = false;

                {
                    // Perform some heuristic checks before expensive comparison of string.
                    if (cmp.comparator.length() > entry->size) goto cont_eval_finished;

                    // Allocate space for the dynamic data loading.
                    char* dynamic_data = (char*)malloc(entry->size);
                    std::string_view dynamic_data_sv = std::string_view(dynamic_data, entry->size);
                    
                    // Read the dynamic data to the allocated space.
                    ssize_t pread_result = pread(this->dynamic_handle, dynamic_data, entry->size, entry->record_location + sizeof(DynamicRecord));
                    if (pread_result != entry->size) {
                        /* Will be improved after disk read overhaul */
                        logerr("Error or incorrect number of bytes returned from pread for dynamic string");
                        exit(1);
                    }

                    // Check if the string contains the item.
                    size_t match_result = dynamic_data_sv.find(cmp.comparator);

                    // Free the allocated dynamic data as it is not needed anymore.
                    free(dynamic_data);

                    // Check if the data does not contain the string.
                    if (match_result == std::string_view::npos) goto cont_eval_finished;

                    // String is a match.
                    condition_passed = true;
                }

                cont_eval_finished:
                if (!condition_passed ^ generic_cmp.negated) return false;

                break;
            }

            case query_compiler::WhereComparoOp::STRING_IN_LIST: {
                query_compiler::QueryComparator::StringInList& cmp = generic_cmp.info.as<query_compiler::QueryComparator::StringInList>();
                TableHashedEntry* entry = (TableHashedEntry*)data;

                bool condition_passed = false;

                {
                    // Perform some heuristic checks before expensive hash checks.
                    if (entry->size > cmp.longest_string_length || entry->size < cmp.shortest_string_length) goto list_eval_finished;

                    auto list_entry = cmp.list.find(entry->hash);

                    // Check if the column's hash is in the list.
                    if (list_entry == cmp.list.end()) goto list_eval_finished;

                    // Allocate space for the dynamic data loading.
                    char* dynamic_data = (char*)malloc(entry->size);
                    std::string_view ro_dynamic_string = std::string_view(dynamic_data, entry->size);

                    // Read the dynamic data to the allocated space.
                    ssize_t pread_result = pread(this->dynamic_handle, dynamic_data, entry->size, entry->record_location + sizeof(DynamicRecord));
                    if (pread_result != entry->size) {
                        /* Will be improved after disk read overhaul */
                        logerr("Error or incorrect number of bytes returned from pread for dynamic string");
                        exit(1);
                    }

                    // Check if the hash matches equal to any of the actual string values.
                    if (list_entry->second.is_single()) {
                        condition_passed = list_entry->second.get_single() == ro_dynamic_string;
                    } else {
                        for (const std::string_view& key : list_entry->second) {
                            if (key == ro_dynamic_string) {
                                condition_passed = true;
                                break;
                            }
                        }
                    }

                    // Free the allocated dynamic data as it is not needed anymore.
                    free(dynamic_data);
                }

                list_eval_finished:
                if (!condition_passed ^ generic_cmp.negated) return false;

                break;
            }
        }
    }

    return true;
}

void ActiveTable::assemble_record_data_to_json(TableRecordHeader* record, size_t included_columns, rapidjson::Document& output) {
    output.SetObject();
    for (uint32_t i = 0; i < this->header.num_columns; i++) {
        if ((included_columns & (1 << i)) == 0) continue;
        TableColumn& column = this->header_columns[i];
        std::string_view column_name(column.name, column.name_length);

        NumericColumnData* data = (NumericColumnData*)(record->data + column.buffer_offset);
        switch (column.type) {
            case ColumnType::String: {
                TableHashedEntry* entry = (TableHashedEntry*)data;
                
                char* buffer = (char*)output.GetAllocator().Malloc(entry->size);
                std::string_view buffer_sv(buffer, entry->size);

                // Read the dynamic data.
                ssize_t pread_result = pread(this->dynamic_handle, buffer, entry->size, entry->record_location + sizeof(DynamicRecord));
                if (pread_result != entry->size) {
                    /* Will be improved after disk read overhaul */
                    logerr("Error or incorrect number of bytes returned from pread for dynamic string");
                    exit(1);
                }

                // Store the dynamic data and free the buffer.
                output.AddMember(rapidjson_string_view(column_name), rapidjson_string_view(buffer_sv), output.GetAllocator());
                
                break;
            }

            case ColumnType::Byte: output.AddMember(rapidjson_string_view(column_name), data->byte, output.GetAllocator()); break;
            case ColumnType::Float32: output.AddMember(rapidjson_string_view(column_name), data->float32, output.GetAllocator()); break;
            case ColumnType::Integer: output.AddMember(rapidjson_string_view(column_name), data->int32, output.GetAllocator()); break;
            case ColumnType::Long64: output.AddMember(rapidjson_string_view(column_name), data->long64, output.GetAllocator()); break;
        }
    }
}

bool ActiveTable::find_one_record(query_compiler::CompiledFindQuery* query, rapidjson::Document& result) {
    this->op_mutex.lock();

    size_t offset_counter = query->offset;
    for (TableRecordHeader* r_header : *this) {
        // If the block is empty, skip to the next one.
        if ((r_header->flags & TableRecordFlags::active) == 0) continue;

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
    for (TableRecordHeader* r_header : *this) {
        // If the block is empty, skip to the next one.
        if ((r_header->flags & TableRecordFlags::active) == 0) continue;

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