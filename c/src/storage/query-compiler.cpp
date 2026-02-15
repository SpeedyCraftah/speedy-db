#include "query-compiler.h"
#include "compiled-query.h"
#include "table-basic.h"
#include "table.h"
#include <memory>
#include <string_view>
#include "../deps/xxh/xxhash.h"
#include "../misc/constants.h"
#include "../deps/simdjson/simdjson.h"

using simdjson::ondemand::json_type;

#define MAX_VARIABLE_OPERATION_COUNT 20

namespace query_compiler {
    const rapidjson::GenericStringRef<char> error_text[] = {
        "A column that has been specified does not exist.",
        "A column that has been specified for results filtering does not exist.",
        "Your query has too many compare operations, reduce the amount of WHERE conditions and try again.",
        "Your query has too many update operations, reduce the amount of CHANGES and try again.",
        "The advanced condition specified does not exist or appear to be supported.",
        "The option specified for a setting does not fit the acceptable parameters.",
        "Your query contains duplicates of the same column which is not allowed for this query.",
        "Your query does not contain all of the table columns which is required for this query.",
        "A column that has been specified for results sorting does not exist.",
        "A results sorting column can only be of a numeric type."
    };

    NumericColumnData parse_numeric_sj_value(ColumnType column_type, simdjson::ondemand::value& value) {
        NumericColumnData numeric;

        switch (column_type) {
            case ColumnType::Integer: numeric.int32 = (int)value.get_int64(); break;
            case ColumnType::Float32: numeric.float32 = (float)value.get_double(); break;
            default: numeric.unsigned64_raw = value.get_uint64(); break;
        }

        return numeric;
    }

    // Reusable functions.
    uint32_t parse_conditions(ActiveTable* table, QueryComparator conditions[], simdjson::ondemand::object& conditions_object) {
        // Iterate over the conditional queries and count queries.
        uint32_t conditions_count = 0;
        for (auto condition : conditions_object) {
            std::string_view key = condition.unescaped_key();
            auto value = condition.value();

            auto column_find = table->columns.find(key);
            if (column_find == table->columns.end()) throw query_compiler::exception(error::COLUMN_NOT_FOUND);
            TableColumn* column = column_find->second;
            
            // Determine the condition type.
            auto type = value.type().value();
            
            // Advanced comparison.
            if (type == json_type::object) {
                simdjson::ondemand::object cmp_object = value.get_object();

                // String advanced queries.
                if (column->type == ColumnType::String) {
                    for (auto advanced_condition : cmp_object) {
                        QueryComparator& cmp = conditions[conditions_count];
                        cmp.column = column;

                        std::string_view advanced_key = advanced_condition.unescaped_key();
                        auto advanced_value = advanced_condition.value();

                        // If the condition should be negated.
                        if (advanced_key.starts_with('!')) {
                            cmp.negated = true;

                            // Remove the condition modifier before comparing operation names.
                            advanced_key.remove_prefix(1);
                        } else cmp.negated = false;

                        if (advanced_key == "contains") {
                            QueryComparator::String& cmp_info = cmp.info.set_as<QueryComparator::String>();
                            std::string_view comparator = advanced_value.get_string();

                            cmp.op = WhereCompareOp::STRING_CONTAINS;
                            cmp_info.comparator = comparator;
                        } else if (advanced_key == "==") {
                            QueryComparator::String& cmp_info = cmp.info.set_as<QueryComparator::String>();
                            std::string_view comparator = advanced_value.get_string();

                            cmp.op = WhereCompareOp::STRING_EQUAL;
                            cmp_info.comparator = comparator;
                            cmp_info.comparator_hash = XXH64(comparator.data(), comparator.length(), HASH_SEED);
                        } else if (advanced_key == "in") {
                            // TODO: Add version for short lists which uses a simple array instead of a hash structure.
                            QueryComparator::StringInList& cmp_info = cmp.info.set_as<QueryComparator::StringInList>();
                            cmp.op = WhereCompareOp::STRING_IN_LIST;
                            cmp_info.shortest_string_length = UINT32T_MAX;
                            cmp_info.longest_string_length = 0;
                            
                            simdjson::ondemand::array keys = advanced_value.get_array();

                            // Count the array and reserve an estimated amount of elements to avoid re-allocations.
                            cmp_info.list.reserve(keys.count_elements());

                            for (std::string_view key : keys) {
                                size_t hash = XXH64(key.data(), key.length(), HASH_SEED);

                                auto it = cmp_info.list.find(hash);
                                if (it != cmp_info.list.end()) {
                                    // Don't include any duplicates.
                                    if (it->second.is_single()) {
                                        if (it->second.get_single() == key) continue;
                                    } else {
                                        for (const std::string_view& existing_key : it->second) {
                                            if (existing_key == key) goto skip_string;
                                        }
                                    }

                                    it->second.add(key);
                                } else {
                                    cmp_info.list.emplace(XXH64(key.data(), key.length(), HASH_SEED), speedystd::short_store(key));
                                }

                                // Re-compute longest and shortest string values.
                                if (key.length() > cmp_info.longest_string_length) cmp_info.longest_string_length = key.length();
                                if (key.length() < cmp_info.shortest_string_length) cmp_info.shortest_string_length = key.length();

                                skip_string: continue;
                            }
                        } else throw query_compiler::exception(query_compiler::error::INVALID_CONDITION);

                        conditions_count++;
                        if (conditions_count >= MAX_VARIABLE_OPERATION_COUNT) throw query_compiler::exception(error::TOO_MANY_CMP_OPS);
                    }
                } 
                
                // Numeric advanced queries.
                else {
                    for (auto advanced_condition : cmp_object) {
                        QueryComparator& cmp = conditions[conditions_count];
                        cmp.column = column;

                        std::string_view advanced_key = advanced_condition.unescaped_key();
                        auto advanced_value = advanced_condition.value();

                        // If the condition should be negated.
                        if (advanced_key.starts_with('!')) {
                            cmp.negated = true;

                            // Remove the condition modifier before comparing operation names.
                            advanced_key.remove_prefix(1);
                        } else cmp.negated = false;
                        
                        // Queries which require a different cmp_info structure.
                        if (advanced_key == "in") {
                            // TODO: Add version for short lists which uses a simple array instead of a hash structure.
                            QueryComparator::NumericInList& cmp_info = cmp.info.set_as<QueryComparator::NumericInList>();
                            cmp.op = WhereCompareOp::NUMERIC_IN_LIST;

                            simdjson::ondemand::array keys = advanced_value.get_array();

                            // Count the array and reserve an estimated amount of elements to avoid re-allocations.
                            cmp_info.list.reserve(keys.count_elements());

                            for (simdjson::ondemand::value key : keys) {
                                NumericColumnData numeric = parse_numeric_sj_value(column->type, key);
                                cmp_info.list.insert(numeric.unsigned64_raw);
                            }
                        }
                        
                        // Numeric comparisons with basic comparators.
                        else {
                            if (advanced_key == "<") cmp.op = WhereCompareOp::NUMERIC_LESS_THAN;
                            else if (advanced_key == ">") cmp.op = WhereCompareOp::NUMERIC_GREATER_THAN;
                            else if (advanced_key == "<=") cmp.op = WhereCompareOp::NUMERIC_LESS_THAN_EQUAL_TO;
                            else if (advanced_key == ">=") cmp.op = WhereCompareOp::NUMERIC_GREATER_THAN_EQUAL_TO;
                            else if (advanced_key == "==") cmp.op = WhereCompareOp::NUMERIC_EQUAL;
                            else throw query_compiler::exception(query_compiler::error::INVALID_CONDITION);
                            
                            QueryComparator::Numeric& cmp_info = cmp.info.set_as<QueryComparator::Numeric>();
                            
                            // Parse the number.
                            cmp_info.comparator = parse_numeric_sj_value(column->type, advanced_value.value());
                        }

                        conditions_count++;
                        if (conditions_count >= MAX_VARIABLE_OPERATION_COUNT) throw query_compiler::exception(error::TOO_MANY_CMP_OPS);
                    }
                }
            }
            
            // Direct comparison.
            else {
                switch (column->type) {
                    case ColumnType::String: {
                        QueryComparator& cmp = conditions[conditions_count];
                        QueryComparator::String& cmp_info = cmp.info.set_as<QueryComparator::String>();

                        std::string_view comparator = value.get_string();

                        cmp.op = WhereCompareOp::STRING_EQUAL;
                        cmp.negated = false;
                        cmp.column = column;
                        cmp_info.comparator = comparator;
                        cmp_info.comparator_hash = XXH64(comparator.data(), comparator.length(), HASH_SEED);

                        break;
                    }

                    // Standard numbers with no specific requirements.
                    default: {
                        QueryComparator& cmp = conditions[conditions_count];
                        QueryComparator::Numeric& cmp_info = cmp.info.set_as<QueryComparator::Numeric>();

                        cmp.op = WhereCompareOp::NUMERIC_EQUAL;
                        cmp.negated = false;
                        cmp.column = column;

                        // Parse the number.
                        cmp_info.comparator = parse_numeric_sj_value(column->type, value.value());

                        break;
                    }
                }

                conditions_count++;
                if (conditions_count >= MAX_VARIABLE_OPERATION_COUNT) throw query_compiler::exception(error::TOO_MANY_CMP_OPS);
            }
        }

        return conditions_count;
    }


    CompiledFindQuery* compile_find_query(ActiveTable* table, simdjson::ondemand::object& query_object) {
        // TODO - use existing buffer?
        std::unique_ptr<CompiledFindQuery> compiled_query(new CompiledFindQuery);
        
        simdjson::ondemand::object conditions_object = query_object["where"];

        // Create buffer which should cover every condition operation.
        compiled_query->conditions = new QueryComparator[MAX_VARIABLE_OPERATION_COUNT];

        // Process all WHERE conditions.
        compiled_query->conditions_count = parse_conditions(table, compiled_query->conditions, conditions_object);

        // Check for query return limits (ignored on findOne).
        // TODO - check before checking for limit, performance penalty on invalid elements.
        if (query_object["limit"].get(compiled_query->limit) == simdjson::error_code::INCORRECT_TYPE) throw simdjson::simdjson_error(simdjson::error_code::INCORRECT_TYPE);

        // Check for record offset.
        if (query_object["offset"].get(compiled_query->offset) == simdjson::error_code::INCORRECT_TYPE) throw simdjson::simdjson_error(simdjson::error_code::INCORRECT_TYPE);

        // Check for any sorting preferences.
        simdjson::ondemand::object sort_options;
        simdjson::error_code sort_options_result = query_object["sort"].get(sort_options);
        if (sort_options_result == simdjson::error_code::SUCCESS) {
            // Get the direction of the sort.
            int8_t direction = sort_options["order"].get_int64();
            if (direction != 1 && direction != -1) throw query_compiler::exception(error::INVALID_OPTION_SETTING);
            compiled_query->result_sort = (ResultSortMode)direction;
            
            // Get the column to use as a key.
            std::string_view column_name = sort_options["column"];
            auto column_find = table->columns.find(column_name);
            if (column_find == table->columns.end()) throw query_compiler::exception(error::SORT_COLUMN_NOT_FOUND);
            compiled_query->result_sort_column = column_find->second;

            // At the moment only numeric columns are supported.
            if (!column_type_is_numeric(column_find->second->type)) throw query_compiler::exception(error::SORT_COLUMN_NOT_NUMERIC);
        } else if (sort_options_result == simdjson::error_code::INCORRECT_TYPE) throw simdjson::simdjson_error(simdjson::error_code::INCORRECT_TYPE);

        // Check for any return columns.
        simdjson::ondemand::array return_columns;
        simdjson::error_code return_columns_result = query_object["return"].get(return_columns);
        if (return_columns_result == simdjson::error_code::SUCCESS) {
            // Return no columns by default.
            size_t filtered_columns = 0;

            for (std::string_view column_name : return_columns) {
                auto column_find = table->columns.find(column_name);
                if (column_find == table->columns.end()) throw query_compiler::exception(error::COLUMN_NOT_FOUND);
                TableColumn* f_column = column_find->second;

                // Set the bit for the column.
                filtered_columns |= (1 << f_column->index);
            }

            compiled_query->columns_returned = filtered_columns;
        } else if (return_columns_result == simdjson::error_code::INCORRECT_TYPE) throw simdjson::simdjson_error(simdjson::error_code::INCORRECT_TYPE);

        return compiled_query.release();
    }

    CompiledInsertQuery* compile_insert_query(ActiveTable* table, simdjson::ondemand::object& query_object) {
        std::unique_ptr<CompiledInsertQuery> compiled_query(new CompiledInsertQuery);

        // Allocate columns.length size of buffer since all columns are required at the moment for inserts.
        compiled_query->values = new InsertColumn[table->column_count];

        // Iterate over the columns specified (with internal columns already 'specified').
        size_t columns_iterated = 0;
        for (auto column_data : query_object) {
            std::string_view column_name = column_data.unescaped_key();
            auto value = column_data.value();

            auto column_find = table->columns.find(column_name);
            if (column_find == table->columns.end()) throw query_compiler::exception(error::COLUMN_NOT_FOUND);
            TableColumn* column = column_find->second;

            // Check if column has already been iterated.
            size_t column_bit = (1 << column->index);
            if ((columns_iterated & column_bit) != 0) throw query_compiler::exception(error::DUPLICATE_COLUMNS);

            // Set bit to indicate it has been iterated.
            columns_iterated |= column_bit;

            // Set the column values.
            switch (column->type) {
                case ColumnType::String: {
                    if (value.type() != json_type::string) throw simdjson::simdjson_error(simdjson::error_code::INCORRECT_TYPE);

                    InsertColumn& val = compiled_query->values[column->index];
                    val.info.set_as<InsertColumn::String>();

                    InsertColumn::String& val_info = val.info.as<InsertColumn::String>();

                    std::string_view data = value.get_string();
                    
                    val_info.data = data;
                    val_info.data_hash = XXH64(data.data(), data.length(), HASH_SEED);

                    break;
                }

                default: {
                    InsertColumn& val = compiled_query->values[column->index];
                    InsertColumn::Numeric& val_info = val.info.set_as<InsertColumn::Numeric>();

                    // Set data depending on type.
                    switch (column->type) {
                        case ColumnType::Float32: val_info.data.float32 = (float)value.get_double(); break;
                        case ColumnType::Integer: val_info.data.int32 = (int)value.get_int64(); break;
                        default: val_info.data.unsigned64_raw = value.get_uint64(); break;
                    }

                    break;
                }
            }
        }

        // Check if all columns have been specified.
        if (columns_iterated != (UINT64T_MAX >> (64 - table->column_count))) throw query_compiler::exception(error::UNSPECIFIED_COLUMNS);

        // Return query.
        return compiled_query.release();
    }

    CompiledEraseQuery* compile_erase_query(ActiveTable* table, simdjson::ondemand::object& query_object) {
        std::unique_ptr<CompiledEraseQuery> compiled_query(new CompiledEraseQuery);
        
        simdjson::ondemand::object conditions_object = query_object["where"];

        // Create buffer which should cover every condition operation.
        compiled_query->conditions = new QueryComparator[MAX_VARIABLE_OPERATION_COUNT];

        // Process all WHERE conditions.
        compiled_query->conditions_count = parse_conditions(table, compiled_query->conditions, conditions_object);

        // Check for query return limits.
        // TODO - check before checking for limit, performance penalty on invalid elements.
        if (query_object["limit"].get(compiled_query->limit) == simdjson::error_code::INCORRECT_TYPE) throw simdjson::simdjson_error(simdjson::error_code::INCORRECT_TYPE);

        return compiled_query.release();
    }

    CompiledUpdateQuery* compile_update_query(ActiveTable* table, simdjson::ondemand::object& query_object) {
        std::unique_ptr<CompiledUpdateQuery> compiled_query(new CompiledUpdateQuery);
        
        simdjson::ondemand::object conditions_object = query_object["where"];

        // Create buffer which should cover every condition operation.
        compiled_query->conditions = new QueryComparator[MAX_VARIABLE_OPERATION_COUNT];

        // Process all WHERE conditions.
        compiled_query->conditions_count = parse_conditions(table, compiled_query->conditions, conditions_object);

        // Create buffer to hold every possible update operation.
        // TODO - reduce this to only required columns.
        compiled_query->changes = new UpdateSet[table->column_count];

        simdjson::ondemand::object updates_object = query_object["changes"];

        // Process all update conditions.
        // Not a copy of INSERT as updates have different operations and queries planned for future.
        size_t columns_iterated = 0;
        uint32_t updates_count = 0;
        for (auto column_data : updates_object) {
            std::string_view column_name = column_data.unescaped_key();
            auto value = column_data.value();

            auto column_find = table->columns.find(column_name);
            if (column_find == table->columns.end()) throw query_compiler::exception(error::COLUMN_NOT_FOUND);
            TableColumn* column = column_find->second;

            // Check if column has already been iterated.
            size_t column_bit = (1 << column->index);
            if ((columns_iterated & column_bit) != 0) throw query_compiler::exception(error::DUPLICATE_COLUMNS);

            // Set bit to indicate it has been iterated.
            columns_iterated |= column_bit;

            // Needed for future advanced expressions.
            json_type value_type = value.type();

            switch (column->type) {
                case ColumnType::String: {
                    if (value_type != json_type::string) throw simdjson::simdjson_error(simdjson::error_code::INCORRECT_TYPE);

                    std::string_view data = value.get_string();

                    UpdateSet& update = compiled_query->changes[updates_count];
                    UpdateSet::String& update_info = update.info.set_as<UpdateSet::String>();
                    update.op = UpdateChangesOp::STRING_SET;
                    update.column = column;
                    update_info.new_value = data;
                    update_info.new_value_hash = XXH64(data.data(), data.length(), HASH_SEED);

                    break;
                }

                default: {
                    UpdateSet& update = compiled_query->changes[updates_count];
                    UpdateSet::Numeric& update_info = update.info.set_as<UpdateSet::Numeric>();
                    update.op = UpdateChangesOp::NUMERIC_SET;
                    update.column = column;
                    
                    // Set data depending on type.
                    switch (column->type) {
                        case ColumnType::Float32: update_info.new_value.float32 = (float)value.get_double(); break;
                        case ColumnType::Integer: update_info.new_value.int32 = (int)value.get_int64(); break;
                        default: update_info.new_value.unsigned64_raw = value.get_uint64(); break;
                    }

                    break;
                }
            }

            updates_count++;
        }

        compiled_query->changes_count = updates_count;

        // Check for query return limits.
        // TODO - check before checking for limit, performance penalty on invalid elements.
        if (query_object["limit"].get(compiled_query->limit) == simdjson::error_code::INCORRECT_TYPE) throw simdjson::simdjson_error(simdjson::error_code::INCORRECT_TYPE);

        return compiled_query.release();
    }
};