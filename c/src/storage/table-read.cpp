#include "compiled-query.h"
#include "structures/record.h"
#include "table-basic.h"
#include "table.h"
#include <exception>
#include <iterator>
#include <memory>
#include <string_view>
#include "table-iterators.h"

using namespace query_compiler;

bool ActiveTable::verify_record_conditions_match(RecordData* record_data, query_compiler::QueryComparator* conditions, uint32_t conditions_length) {
    // Construct the record abstraction.
    // This is not passed in directly via parameters to encourage the compiler to optimize it away.
    Record record(*this, record_data);

    // Go over all conditions for record.
    for (uint32_t i = 0; i < conditions_length; i++) {
        query_compiler::QueryComparator& generic_cmp = conditions[i];

        switch (generic_cmp.op) {
            case query_compiler::WhereCompareOp::NUMERIC_EQUAL: {
                query_compiler::QueryComparator::Numeric& cmp = generic_cmp.info.as<query_compiler::QueryComparator::Numeric>();
                NumericColumn numeric = record.get_numeric(generic_cmp.column);
                
                if (!numeric.cmp_eq(cmp) ^ generic_cmp.negated) return false;
                break;
            }

            case query_compiler::WhereCompareOp::NUMERIC_GREATER_THAN: {
                query_compiler::QueryComparator::Numeric& cmp =  generic_cmp.info.as<query_compiler::QueryComparator::Numeric>();
                NumericColumn numeric = record.get_numeric(generic_cmp.column);

                if (!numeric.cmp_gt(cmp) ^ generic_cmp.negated) return false;
                break;
            }

            case query_compiler::WhereCompareOp::NUMERIC_GREATER_THAN_EQUAL_TO: {
                query_compiler::QueryComparator::Numeric& cmp =  generic_cmp.info.as<query_compiler::QueryComparator::Numeric>();
                NumericColumn numeric = record.get_numeric(generic_cmp.column);

                if (!numeric.cmp_gte(cmp) ^ generic_cmp.negated) return false;
                break;
            }

            case query_compiler::WhereCompareOp::NUMERIC_LESS_THAN: {
                query_compiler::QueryComparator::Numeric& cmp =  generic_cmp.info.as<query_compiler::QueryComparator::Numeric>();
                NumericColumn numeric = record.get_numeric(generic_cmp.column);

                if (!numeric.cmp_lt(cmp) ^ generic_cmp.negated) return false;
                break;
            }

            case query_compiler::WhereCompareOp::NUMERIC_LESS_THAN_EQUAL_TO: {
                query_compiler::QueryComparator::Numeric& cmp =  generic_cmp.info.as<query_compiler::QueryComparator::Numeric>();
                NumericColumn numeric = record.get_numeric(generic_cmp.column);

                if (!numeric.cmp_lte(cmp) ^ generic_cmp.negated) return false;
                break;
            }

            case query_compiler::WhereCompareOp::NUMERIC_IN_LIST: {
                query_compiler::QueryComparator::NumericInList& cmp = generic_cmp.info.as<query_compiler::QueryComparator::NumericInList>();
                NumericColumn numeric = record.get_numeric(generic_cmp.column);
                
                NumericColumnData value = numeric.to_owned_data();
                if ((!cmp.list.contains(value.unsigned64_raw)) ^ generic_cmp.negated) return false;

                break;
            }

            case query_compiler::WhereCompareOp::STRING_EQUAL: {
                query_compiler::QueryComparator::String& cmp = generic_cmp.info.as<query_compiler::QueryComparator::String>();
                HashedColumnData* entry = record.get_hashed(generic_cmp.column);

                bool condition_passed = false;

                {
                    // Perform some heuristic checks before expensive comparison of string.
                    if (entry->hash != cmp.comparator_hash) goto eq_eval_finished;
                    if (entry->size != cmp.comparator.size()) goto eq_eval_finished;

                    speedystd::simple_string dynamic_data = record.load_dynamic(generic_cmp.column);

                    // Compare the data character by character to 100% confirm they are a match.
                    // Safe to use memcmp since they are guaranteed to be same size.
                    int match_result = memcmp(dynamic_data.data(), cmp.comparator.data(), entry->size);

                    // Check if the data is not a match.
                    if (match_result != 0) goto eq_eval_finished;

                    // String is a match.
                    condition_passed = true;
                }

                eq_eval_finished:
                if (!condition_passed ^ generic_cmp.negated) return false;

                break;
            }

            case query_compiler::WhereCompareOp::STRING_CONTAINS: {
                query_compiler::QueryComparator::String& cmp = generic_cmp.info.as<query_compiler::QueryComparator::String>();
                HashedColumnData* entry = record.get_hashed(generic_cmp.column);

                bool condition_passed = false;

                {
                    // Perform some heuristic checks before expensive comparison of string.
                    if (cmp.comparator.length() > entry->size) goto cont_eval_finished;

                    speedystd::simple_string dynamic_data = record.load_dynamic(generic_cmp.column);

                    // Check if the string contains the item.
                    size_t match_result = dynamic_data.as_string_view().find(cmp.comparator);

                    // Check if the data does not contain the string.
                    if (match_result == std::string_view::npos) goto cont_eval_finished;

                    // String is a match.
                    condition_passed = true;
                }

                cont_eval_finished:
                if (!condition_passed ^ generic_cmp.negated) return false;

                break;
            }

            case query_compiler::WhereCompareOp::STRING_IN_LIST: {
                query_compiler::QueryComparator::StringInList& cmp = generic_cmp.info.as<query_compiler::QueryComparator::StringInList>();
                HashedColumnData* entry = record.get_hashed(generic_cmp.column);

                bool condition_passed = false;

                {
                    // Perform some heuristic checks before expensive hash checks.
                    if (entry->size > cmp.longest_string_length || entry->size < cmp.shortest_string_length) goto list_eval_finished;

                    auto list_entry = cmp.list.find(entry->hash);

                    // Check if the column's hash is in the list.
                    if (list_entry == cmp.list.end()) goto list_eval_finished;

                    speedystd::simple_string dynamic_data = record.load_dynamic(generic_cmp.column);

                    // Check if the hash matches equal to any of the actual string values.
                    if (list_entry->second.is_single()) {
                        condition_passed = list_entry->second.get_single() == (std::string_view)dynamic_data;
                    } else {
                        for (const std::string_view& key : list_entry->second) {
                            if (key == (std::string_view)dynamic_data) {
                                condition_passed = true;
                                break;
                            }
                        }
                    }
                }

                list_eval_finished:
                if (!condition_passed ^ generic_cmp.negated) return false;

                break;
            }
        }
    }

    return true;
}

void ActiveTable::assemble_record_data_to_json(RecordData* record_data, size_t included_columns, rapidjson::Document& output) {
    Record record(*this, record_data);

    output.SetObject();
    for (uint32_t i = 0; i < this->column_count; i++) {
        if ((included_columns & (1 << i)) == 0) continue;
        
        TableColumn& column = this->header_columns[i];
        std::string_view column_name(column.name, column.name_length);

        switch (column.type) {
            case ColumnType::String: {
                HashedColumnData* entry = record.get_hashed(&column);
                
                char* buffer = (char*)output.GetAllocator().Malloc(entry->size);

                // Read the dynamic data directly into the rapidjson buffer.
                record.load_dynamic_into(entry, buffer);

                // Add the dynamic data column result into the object.
                output.AddMember(rapidjson_string_view(column_name), rapidjson_string_view(std::string_view(buffer, entry->size)), output.GetAllocator());
                
                break;
            }

            case ColumnType::Byte: output.AddMember(rapidjson_string_view(column_name), record.get_numeric_raw(&column)->byte, output.GetAllocator()); break;
            case ColumnType::Float32: output.AddMember(rapidjson_string_view(column_name), record.get_numeric_raw(&column)->float32, output.GetAllocator()); break;
            case ColumnType::Integer: output.AddMember(rapidjson_string_view(column_name), record.get_numeric_raw(&column)->int32, output.GetAllocator()); break;
            case ColumnType::Long64: output.AddMember(rapidjson_string_view(column_name), record.get_numeric_raw(&column)->long64, output.GetAllocator()); break;
        }
    }
}

bool ActiveTable::find_one_record(query_compiler::CompiledFindQuery* query, rapidjson::Document& result) {
    std::lock_guard<std::mutex> lock(op_mutex);

    size_t offset_counter = query->offset;
    for (Record record : table_iterator::iterate_specific(*this, query)) {
        // Ignore matched records until offset runs out.
        if (offset_counter != 0) {
            --offset_counter;
            continue;
        }

        assemble_record_data_to_json((RecordData*)record, query->columns_returned, result);
        return true;
    }

    return false;
}

void ActiveTable::find_many_records(query_compiler::CompiledFindQuery* query, rapidjson::Document& result) {
    std::lock_guard<std::mutex> lock(op_mutex);
    result.SetArray();

    // Sorted results.
    // This needs a separate implementation for best efficiency.
    if (query->result_sort != ResultSortMode::NONE) {
        std::list<std::unique_ptr<RecordData[]>> records;

        // Offsets work a little differently in sorted queries.
        // We want to get the top limit + offset records, then remove offset of the first resulting records.
        size_t temp_limit = query->limit + query->offset;

        for (Record record : table_iterator::iterate_specific(*this, query)) {
            // Insert the first entry.
            if (records.size() == 0) {
                records.push_front(record.to_owned_data());
            }
            
            // Any other entry.
            else {
                NumericColumn key = record.get_numeric(query->result_sort_column);

                // Ascending logic.
                if (query->result_sort == ResultSortMode::ASCENDING) {
                    NumericColumn tail_record = Record(*this, records.back().get()).get_numeric(query->result_sort_column);

                    // If the limit was reached, we can make some assumptions to eliminate records we know won't appear in the results.
                    // By doing this we can prevent wasted work due to allocations and copying of rows that will simply be pruned later.
                    if (query->limit != 0 && records.size() == temp_limit) {
                        if (key.cmp_gte(tail_record)) continue;
                    } 
                    
                    // Limit wasn't reached yet, we can do some additional inserts.
                    else {
                        // If the record is larger or equal to the tail, push it to the end.
                        if (key.cmp_gte(tail_record)) {
                            records.push_back(record.to_owned_data());
                            goto done;
                        }
                    }

                    NumericColumn top_record = Record(*this, records.front().get()).get_numeric(query->result_sort_column);

                    // If the record is smaller or equal to the top entry, push it to the front.
                    if (key.cmp_lte(top_record)) {
                        records.push_front(record.to_owned_data());
                        goto done;
                    }

                    // This record will go somewhere in-between the list, hence we will need to find a suitable position.
                    for (auto pos = std::next(records.begin()); pos != records.end(); ++pos) {
                        NumericColumn list_record = Record(*this, pos->get()).get_numeric(query->result_sort_column);
                        if (key.cmp_lte(list_record)) {
                            records.insert(pos, record.to_owned_data());
                            goto done;
                        }
                    }

                    #if !defined(__OPTIMIZE__)
                        puts("Debug build check: impossible fallthrough while sorting!");
                        std::terminate();
                    #else
                        __builtin_unreachable();
                    #endif
                }
                
                // Descending logic.
                else if (query->result_sort == ResultSortMode::DESCENDING) {
                    NumericColumn tail_record = Record(*this, records.back().get()).get_numeric(query->result_sort_column);

                    // If the limit was reached, we can make some assumptions to eliminate records we know won't appear in the results.
                    if (query->limit != 0 && records.size() == temp_limit) {
                        if (key.cmp_lte(tail_record)) continue;
                    }

                    // Limit wasn't reached yet, we can do some additional inserts.
                    else {
                        // If the record is smaller or equal to the tail, push it to the end.
                        if (key.cmp_lte(tail_record)) {
                            records.push_back(record.to_owned_data());
                            goto done;
                        }
                    }

                    NumericColumn top_record = Record(*this, records.front().get()).get_numeric(query->result_sort_column);

                    // If the record is larger or equal to the top entry, push it to the front.
                    if (key.cmp_gte(top_record)) {
                        records.push_front(record.to_owned_data());
                        goto done;
                    }

                    // This record will go somewhere in-between the list, hence we will need to find a suitable position.
                    for (auto pos = std::next(records.begin()); pos != records.end(); ++pos) {
                        NumericColumn list_record = Record(*this, pos->get()).get_numeric(query->result_sort_column);
                        if (key.cmp_gte(list_record)) {
                            records.insert(pos, record.to_owned_data());
                            goto done;
                        }
                    }

                    #if !defined(__OPTIMIZE__)
                        puts("Debug build check: impossible fallthrough while sorting!");
                        std::terminate();
                    #else
                        __builtin_unreachable();
                    #endif
                }

                done:
                // If the query has a limit and the list has exceeded it, remove the last entry.
                if (query->limit != 0 && records.size() > temp_limit) records.pop_back();
            }
        }

        // We're done collecting the records, we can now serialize and return them.

        auto record_pos = records.begin();
        if (query->offset != 0) {
            // If the offset is more than what we have, we have nothing to return.
            if (query->offset >= records.size()) return;

            // Offset the first specified entries.
            std::advance(record_pos, query->offset);
        }

        for (; record_pos != records.end(); ++record_pos) {
            rapidjson::Document record_object(&result.GetAllocator());
            assemble_record_data_to_json(record_pos->get(), query->columns_returned, record_object);

            result.PushBack(record_object, result.GetAllocator());
        }
    }

    // No sorting results.
    else {
        size_t offset_counter = query->offset;
        for (Record record : table_iterator::iterate_specific(*this, query)) {
            // Ignore matched records until offset runs out.
            if (offset_counter != 0) {
                --offset_counter;
                continue;
            }
    
            rapidjson::Document record_object(&result.GetAllocator());
            assemble_record_data_to_json((RecordData*)record, query->columns_returned, record_object);
    
            result.PushBack(record_object, result.GetAllocator());
            if (query->limit != 0 && result.Size() == query->limit) break;
        }
    }
}