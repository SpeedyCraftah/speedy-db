#include "query-compiler.h"
#include "table.h"
#include <memory>
#include <string_view>
#include <exception>
#include "../deps/xxh/xxhash.h"

using simdjson::fallback::ondemand::json_type;
using simdjson::fallback::number_type;

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
        "Your query does not contain all of the table columns which is required for this query."
    };

    // Reusable functions.
    uint32_t parse_conditions(ActiveTable* table, GenericQueryComparison conditions[], simdjson::ondemand::object& conditions_object) {
        // Iterate over the conditional queries and count queries.
        uint32_t conditions_count = 0;
        for (auto condition : conditions_object) {
            std::string_view key = condition.unescaped_key();
            auto value = condition.value();

            auto column_find = table->columns.find(key);
            if (column_find == table->columns.end()) throw query_compiler::exception(error::COLUMN_NOT_FOUND);
            table_column* column = column_find->second;
            
            // Determine the condition type.
            auto type = value.type().value();
            
            // Advanced comparison.
            // TODO - add equal comparison for advanced.
            if (type == json_type::object) {
                simdjson::ondemand::object cmp_object = value.get_object();

                // String key compare operations are expensive, only check for possible combinations.
                if (column->type == types::string) {
                    for (auto advanced_condition : cmp_object) {
                        std::string_view advanced_key = advanced_condition.unescaped_key();
                        auto advanced_value = advanced_condition.value();

                        if (advanced_key == "contains") {
                            // We don't get type checking with raw_json, so manually do it.
                            if (advanced_value.type().value() != json_type::string) throw simdjson::simdjson_error(simdjson::error_code::INCORRECT_TYPE);
                                
                            std::string_view comparator = advanced_value.raw_json();

                            // Slice out quotes from beginning and end.
                            comparator.remove_prefix(1);
                            comparator.remove_suffix(1);

                            StringQueryComparison* cmp = reinterpret_cast<StringQueryComparison*>(&conditions[conditions_count]);
                            cmp->op = where_compare_op::STRING_CONTAINS;
                            cmp->column_index = column->index;
                            cmp->comparator = comparator;
                            cmp->comparator_hash = XXH64(comparator.data(), comparator.length(), HASH_SEED);
                        } else throw query_compiler::exception(query_compiler::error::INVALID_CONDITION);

                        conditions_count++;
                        if (conditions_count >= MAX_VARIABLE_OPERATION_COUNT) throw query_compiler::exception(error::TOO_MANY_CMP_OPS);
                    }
                } else {
                    // TODO - shorten LT/LG ops to >/< for efficiency ?
                    for (auto advanced_condition : cmp_object) {
                        std::string_view advanced_key = advanced_condition.unescaped_key();
                        auto advanced_value = advanced_condition.value();

                        size_t buffer = 0;

                        NumericQueryComparison* cmp = reinterpret_cast<NumericQueryComparison*>(&conditions[conditions_count]);
                        cmp->column_index = column->index;
                        
                        // Compiler checks lengths when comparing strings, no further optimisation needed.
                        if (advanced_key == "less_than") cmp->op = where_compare_op::NUMERIC_LESS_THAN;
                        else if (advanced_key == "greater_than") cmp->op = where_compare_op::NUMERIC_GREATER_THAN;
                        else if (advanced_key == "less_than_equal_to") cmp->op = where_compare_op::NUMERIC_LESS_THAN_EQUAL_TO;
                        else if (advanced_key == "greater_than_equal_to") cmp->op = where_compare_op::NUMERIC_GREATER_THAN_EQUAL_TO;
                        else throw query_compiler::exception(query_compiler::error::INVALID_CONDITION);

                        switch (column->type) {
                            case types::integer: *((int*)&buffer) = (int)advanced_value.get_int64(); break;
                            case types::float32: *((float*)&buffer) = (float)advanced_value.get_double(); break;
                            default: buffer = advanced_value.get_uint64(); break;
                        }

                        cmp->comparator = buffer;

                        conditions_count++;
                        if (conditions_count >= MAX_VARIABLE_OPERATION_COUNT) throw query_compiler::exception(error::TOO_MANY_CMP_OPS);
                    }
                }
            }
            
            // Direct comparison.
            else {
                switch (column->type) {
                    case types::string: {
                        StringQueryComparison* cmp = reinterpret_cast<StringQueryComparison*>(&conditions[conditions_count]);

                        // We don't get type checking with raw_json, so manually do it.
                        if (type != json_type::string) throw simdjson::simdjson_error(simdjson::error_code::INCORRECT_TYPE);

                        std::string_view comparator = value.raw_json();

                        // Slice out quotes from beginning and end.
                        comparator.remove_prefix(1);
                        comparator.remove_suffix(1);

                        cmp->op = where_compare_op::STRING_EQUAL;
                        cmp->column_index = column->index;
                        cmp->comparator = comparator;
                        cmp->comparator_hash = XXH64(comparator.data(), comparator.length(), HASH_SEED);

                        break;
                    }

                    // Standard numbers with no specific requirements.
                    default: {
                        size_t buffer = 0;

                        // Correctly cast binary values based on type.
                        switch (value.get_number_type()) {
                            case number_type::floating_point_number: *((float*)&buffer) = (float)value.get_double(); break;
                            case number_type::signed_integer: *((int*)&buffer) = (int)value.get_int64(); break;
                            default: buffer = value.get_uint64(); break;
                        }

                        NumericQueryComparison* cmp = reinterpret_cast<NumericQueryComparison*>(&conditions[conditions_count]);
                        cmp->op = where_compare_op::NUMERIC_EQUAL;
                        cmp->column_index = column->index;
                        cmp->comparator = buffer;

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
        std::unique_ptr<GenericQueryComparison[]> conditions(new GenericQueryComparison[MAX_VARIABLE_OPERATION_COUNT]);
        compiled_query->conditions = conditions.get();

        // Process all WHERE conditions.
        compiled_query->conditions_count = parse_conditions(table, conditions.get(), conditions_object);

        // Check for query return limits (ignored on findOne).
        // TODO - check before checking for limit, performance penalty on invalid elements.
        if (query_object["limit"].get(compiled_query->limit) == simdjson::error_code::INCORRECT_TYPE) throw simdjson::simdjson_error(simdjson::error_code::INCORRECT_TYPE);

        // Check for seek direction.
        // TODO - change query setting to boolean.
        // TODO - check before checking for direction, performance penalty on invalid elements.
        intmax_t seek_direction;
        if (query_object["seek_direction"].get(seek_direction) == simdjson::error_code::SUCCESS) {
            if (seek_direction != 1 && seek_direction != -1) throw query_compiler::exception(error::INVALID_OPTION_SETTING);
            compiled_query->seek_direction = seek_direction == 1;
        }

        simdjson::ondemand::array return_columns;
        if (query_object["return"].get(return_columns) == simdjson::error_code::SUCCESS) {
            // Return no columns by default.
            size_t filtered_columns = 0;

            for (std::string_view column_name : return_columns) {
                auto column_find = table->columns.find(column_name);
                if (column_find == table->columns.end()) throw query_compiler::exception(error::COLUMN_NOT_FOUND);
                table_column* f_column = column_find->second;

                // Set the bit for the column.
                filtered_columns |= (1 << f_column->index);
            }

            compiled_query->columns_returned = filtered_columns;
        }

        // Check for a seek_where statement.
        simdjson::ondemand::object seek_where_conditions;
        if (query_object["seek_where"].get(seek_where_conditions) == simdjson::error_code::SUCCESS) {
            std::unique_ptr<GenericQueryComparison[]> seek_conditions(new GenericQueryComparison[MAX_VARIABLE_OPERATION_COUNT]);
            compiled_query->seek_conditions = seek_conditions.get();

            // Process the conditions.
            compiled_query->seek_conditions_count = parse_conditions(table, seek_conditions.get(), seek_where_conditions);

            seek_conditions.release();
        } 

        // Prevent smart pointers from deallocating.
        conditions.release();

        return compiled_query.release();
    }

    CompiledInsertQuery* compile_insert_query(ActiveTable* table, simdjson::ondemand::object& query_object) {
        std::unique_ptr<CompiledInsertQuery> compiled_query(new CompiledInsertQuery);

        // Allocate columns.length size of buffer since all columns are required at the moment for inserts.
        std::unique_ptr<GenericInsertColumn[]> columns_inserted(new GenericInsertColumn[table->header.num_columns]); 
        compiled_query->values = columns_inserted.get();

        // Iterate over the columns specified.
        size_t columns_iterated = 0;
        for (auto column_data : query_object) {
            std::string_view column_name = column_data.unescaped_key();
            auto value = column_data.value();

            auto column_find = table->columns.find(column_name);
            if (column_find == table->columns.end()) throw query_compiler::exception(error::COLUMN_NOT_FOUND);
            table_column* column = column_find->second;

            // Check if column has already been iterated.
            size_t column_bit = (1 << column->index);
            if ((columns_iterated & column_bit) != 0) throw query_compiler::exception(error::DUPLICATE_COLUMNS);

            // Set bit to indicate it has been iterated.
            columns_iterated |= column_bit;

            // Set the column values.
            switch (column->type) {
                case types::string: {
                    if (value.type() != json_type::string) throw simdjson::simdjson_error(simdjson::error_code::INCORRECT_TYPE);

                    std::string_view data = value.raw_json();
                    data.remove_prefix(1);
                    data.remove_suffix(1);

                    StringInsertColumn* val = reinterpret_cast<StringInsertColumn*>(&columns_inserted[column->index]);
                    val->data = data;

                    break;
                }

                default: {
                    size_t buffer = 0;
                    
                    // Set data depending on type.
                    switch (column->type) {
                        case types::float32: *((float*)&buffer) = (float)value.get_double(); break;
                        case types::integer: *((int*)&buffer) = (int)value.get_int64(); break;
                        default: buffer = value.get_uint64(); break;
                    }

                    NumericInsertColumn* val = reinterpret_cast<NumericInsertColumn*>(&columns_inserted[column->index]);
                    val->data = buffer;

                    break;
                }
            }
        }

        // Check if all columns have been specified.
        // TODO - might overflow/act unexpectedly if column count is 64.
        if (columns_iterated != (1 << table->header.num_columns) - 1) throw query_compiler::exception(error::UNSPECIFIED_COLUMNS);

        // Release smart pointers.
        columns_inserted.release();

        // Return query.
        return compiled_query.release();
    }

    CompiledEraseQuery* compile_erase_query(ActiveTable* table, simdjson::ondemand::object& query_object) {
        std::unique_ptr<CompiledEraseQuery> compiled_query(new CompiledEraseQuery);
        
        simdjson::ondemand::object conditions_object = query_object["where"];

        // Create buffer which should cover every condition operation.
        std::unique_ptr<GenericQueryComparison[]> conditions(new GenericQueryComparison[MAX_VARIABLE_OPERATION_COUNT]);
        compiled_query->conditions = conditions.get();

        // Process all WHERE conditions.
        compiled_query->conditions_count = parse_conditions(table, conditions.get(), conditions_object);

        // Check for query return limits.
        // TODO - check before checking for limit, performance penalty on invalid elements.
        if (query_object["limit"].get(compiled_query->limit) == simdjson::error_code::INCORRECT_TYPE) throw simdjson::simdjson_error(simdjson::error_code::INCORRECT_TYPE);

        // Check for seek direction.
        // TODO - change query setting to boolean.
        // TODO - check before checking for direction, performance penalty on invalid elements.
        intmax_t seek_direction;
        if (query_object["seek_direction"].get(seek_direction) == simdjson::error_code::SUCCESS) {
            if (seek_direction != 1 && seek_direction != -1) throw query_compiler::exception(error::INVALID_OPTION_SETTING);
            compiled_query->seek_direction = seek_direction == 1;
        }

        // Prevent smart pointers from deallocating.
        conditions.release();

        return compiled_query.release();
    }

    CompiledUpdateQuery* compile_update_query(ActiveTable* table, simdjson::ondemand::object& query_object) {
        std::unique_ptr<CompiledUpdateQuery> compiled_query(new CompiledUpdateQuery);
        
        simdjson::ondemand::object conditions_object = query_object["where"];

        // Create buffer which should cover every condition operation.
        std::unique_ptr<GenericQueryComparison[]> conditions(new GenericQueryComparison[MAX_VARIABLE_OPERATION_COUNT]);
        compiled_query->conditions = conditions.get();

        // Process all WHERE conditions.
        compiled_query->conditions_count = parse_conditions(table, conditions.get(), conditions_object);

        // Create buffer to hold every possible update operation.
        std::unique_ptr<GenericUpdate[]> updates(new GenericUpdate[table->header.num_columns]);
        compiled_query->changes = updates.get();

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
            table_column* column = column_find->second;

            // Check if column has already been iterated.
            size_t column_bit = (1 << column->index);
            if ((columns_iterated & column_bit) != 0) throw query_compiler::exception(error::DUPLICATE_COLUMNS);

            // Set bit to indicate it has been iterated.
            columns_iterated |= column_bit;

            // Needed for future advanced expressions.
            json_type value_type = value.type();

            switch (column->type) {
                case types::string: {
                    if (value_type != json_type::string) throw simdjson::simdjson_error(simdjson::error_code::INCORRECT_TYPE);

                    std::string_view data = value.raw_json();
                    data.remove_prefix(1);
                    data.remove_suffix(1);

                    StringUpdateSet* update = reinterpret_cast<StringUpdateSet*>(&updates[updates_count]);
                    update->op = update_changes_op::STRING_SET;
                    update->column_index = column->index;
                    update->new_value = data;
                    update->new_value_hash = XXH64(data.data(), data.length(), HASH_SEED);

                    break;
                }

                default: {
                    size_t buffer = 0;
                    
                    // Set data depending on type.
                    switch (column->type) {
                        case types::float32: *((float*)&buffer) = (float)value.get_double(); break;
                        case types::integer: *((int*)&buffer) = (int)value.get_int64(); break;
                        default: buffer = value.get_uint64(); break;
                    }

                    NumericUpdateSet* update = reinterpret_cast<NumericUpdateSet*>(&updates[updates_count]);
                    update->op = update_changes_op::NUMERIC_SET;
                    update->column_index = column->index;
                    update->new_value = buffer;

                    break;
                }
            }

            updates_count++;
        }

        compiled_query->changes_count = updates_count;

        // Check for query return limits.
        // TODO - check before checking for limit, performance penalty on invalid elements.
        if (query_object["limit"].get(compiled_query->limit) == simdjson::error_code::INCORRECT_TYPE) throw simdjson::simdjson_error(simdjson::error_code::INCORRECT_TYPE);

        // Check for seek direction.
        // TODO - change query setting to boolean.
        // TODO - check before checking for direction, performance penalty on invalid elements.
        intmax_t seek_direction;
        if (query_object["seek_direction"].get(seek_direction) == simdjson::error_code::SUCCESS) {
            if (seek_direction != 1 && seek_direction != -1) throw query_compiler::exception(error::INVALID_OPTION_SETTING);
            compiled_query->seek_direction = seek_direction == 1;
        }

        // Prevent smart pointers from deallocating.
        conditions.release();
        updates.release();

        return compiled_query.release();
    }
};