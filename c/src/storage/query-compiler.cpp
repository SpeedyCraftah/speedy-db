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
    class exception : public std::exception {
        public:
            exception(query_compiler::error errorCode) : errorCode_(errorCode) {};
            query_compiler::error error() const {
                return errorCode_;
            }

        private:
            query_compiler::error errorCode_;
    };

    CompiledFindQuery* compile_find_query(ActiveTable* table, simdjson::ondemand::document& query_object) {
        // TODO - use existing buffer?
        std::unique_ptr<CompiledFindQuery> compiled_query(new CompiledFindQuery);
        
        simdjson::ondemand::object conditions_object = query_object["where"];

        // Create buffer which should cover every condition operation.
        std::unique_ptr<GenericQueryComparison[]> conditions(new GenericQueryComparison[MAX_VARIABLE_OPERATION_COUNT]);
        compiled_query->conditions = conditions.get();

        // Iterate over the conditional queries and count queries.
        uint32_t conditions_count = 0;
        for (auto condition : conditions_object) {
            std::string_view key = condition.unescaped_key();
            auto value = condition.value();

            auto column_find = table->columns.find(key);
            if (column_find == table->columns.end()) throw query_compiler::exception(error::COLUMN_NOT_FOUND);
            table_column& column = column_find->second;
            
            // Determine the condition type.
            auto type = value.type().value();
            
            // Advanced comparison.
            // TODO - add equal comparison for advanced.
            if (type == json_type::object) {
                simdjson::ondemand::object cmp_object = value.get_object();

                // String key compare operations are expensive, only check for possible combinations.
                if (column.type == types::string) {
                    for (auto advanced_condition : cmp_object) {
                        std::string_view advanced_key = condition.unescaped_key();
                        auto advanced_value = condition.value();

                        if (advanced_key == "contains") {
                            // We don't get type checking with raw_json, so manually do it.
                            if (advanced_value.type().value() != json_type::string) throw simdjson::simdjson_error(simdjson::error_code::INCORRECT_TYPE);
                                
                            std::string_view comparator = advanced_value.raw_json();

                            // Slice out quotes from beginning and end.
                            comparator.remove_prefix(1);
                            comparator.remove_suffix(1);

                            StringQueryComparison* cmp = reinterpret_cast<StringQueryComparison*>(&conditions[conditions_count]);
                            cmp->op = where_compare_op::STRING_CONTAINS;
                            cmp->column_index = column.index;
                            cmp->comparator = comparator;
                            cmp->comparator_hash = XXH64(comparator.data(), comparator.length(), HASH_SEED);
                        } else throw query_compiler::exception(query_compiler::error::INVALID_CONDITION);

                        conditions_count++;
                        if (conditions_count >= MAX_VARIABLE_OPERATION_COUNT) throw query_compiler::exception(error::TOO_MANY_CMP_OPS);
                    }
                } else {
                    // TODO - shorten LT/LG ops to >/< for efficiency ?
                    for (auto advanced_condition : cmp_object) {
                        std::string_view advanced_key = condition.unescaped_key();
                        auto advanced_value = condition.value();

                        size_t buffer = 0;

                        NumericQueryComparison* cmp = reinterpret_cast<NumericQueryComparison*>(&conditions[conditions_count]);
                        cmp->column_index = column.index;
                        
                        // Compiler checks lengths when comparing strings, no further optimisation needed.
                        if (advanced_key == "less_than") cmp->op = where_compare_op::NUMERIC_LESS_THAN;
                        else if (advanced_key == "greater_than") cmp->op = where_compare_op::NUMERIC_GREATER_THAN;
                        else if (advanced_key == "less_than_equal_to") cmp->op = where_compare_op::NUMERIC_LESS_THAN_EQUAL_TO;
                        else if (advanced_key == "greater_than_equal_to") cmp->op = where_compare_op::NUMERIC_GREATER_THAN_EQUAL_TO;
                        else throw query_compiler::exception(query_compiler::error::INVALID_CONDITION);

                        switch (column.type) {
                            case types::integer: *((int*)&buffer) = (int)advanced_value.get_int64(); break;
                            case types::float32: *((float*)&buffer) = (float)advanced_value.get_double(); break;
                            default: buffer = advanced_value; break;
                        }

                        cmp->comparator = buffer;

                        conditions_count++;
                        if (conditions_count >= MAX_VARIABLE_OPERATION_COUNT) throw query_compiler::exception(error::TOO_MANY_CMP_OPS);
                    }
                }
            }
            
            // Direct comparison.
            else {
                switch (column.type) {
                    case types::string: {
                        StringQueryComparison* cmp = reinterpret_cast<StringQueryComparison*>(&conditions[conditions_count]);

                        // We don't get type checking with raw_json, so manually do it.
                        if (type != json_type::string) throw simdjson::simdjson_error(simdjson::error_code::INCORRECT_TYPE);

                        std::string_view comparator = value.raw_json();

                        // Slice out quotes from beginning and end.
                        comparator.remove_prefix(1);
                        comparator.remove_suffix(1);

                        cmp->op = where_compare_op::STRING_EQUAL;
                        cmp->column_index = column.index;
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
                            case number_type::signed_integer: *((int*)&buffer) = (int)value; break;
                            default: buffer = value.get_uint64(); break;
                        }

                        NumericQueryComparison* cmp = reinterpret_cast<NumericQueryComparison*>(&conditions[conditions_count]);
                        cmp->op = where_compare_op::NUMERIC_EQUAL;
                        cmp->column_index = column.index;
                        cmp->comparator = buffer;

                        break;
                    }
                }

                conditions_count++;
                if (conditions_count >= MAX_VARIABLE_OPERATION_COUNT) throw query_compiler::exception(error::TOO_MANY_CMP_OPS);
            }
        }

        // Check for query return limits (ignored on findOne).
        // TODO - check before checking for limit, performance penalty on invalid elements.
        if (query_object["limit"].get(compiled_query->limit) == simdjson::error_code::INCORRECT_TYPE) throw simdjson::simdjson_error(simdjson::error_code::INCORRECT_TYPE);

        // Check for seek direction.
        // TODO - change query setting to boolean.
        // TODO - check before checking for direction, performance penalty on invalid elements.
        int seek_direction;
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
                table_column& f_column = column_find->second;

                // Set the bit for the column.
                filtered_columns |= (1 << f_column.index);
            }

            compiled_query->columns_returned = filtered_columns;
        }

        // Prevent smart pointers from deallocating.
        conditions.release();

        return compiled_query.release();
    }
};