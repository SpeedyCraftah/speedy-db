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
            if (type == json_type::object) {
                simdjson::ondemand::object cmp_object = value.get_object();
            }
            
            // Direct comparison.
            else {
                switch (column.type) {
                    case types::string: {
                        StringQueryComparison* cmp = reinterpret_cast<StringQueryComparison*>(&conditions[conditions_count]);
                        std::string_view comparator = value.raw_json();

                        // Slice out quotes from beginning and end.
                        comparator.remove_prefix(1);
                        comparator.remove_suffix(1);

                        // We don't get type checking with raw_json, so manually do it.
                        if (type != json_type::string) throw simdjson::simdjson_error(simdjson::error_code::INCORRECT_TYPE);

                        cmp->op = where_compare_op::STRING_EQUAL;
                        cmp->column_index = column.index;
                        cmp->comparator = comparator;
                        cmp->comparator_hash = XXH64(comparator.data(), comparator.length(), HASH_SEED);

                        break;
                    }

                    // Standard numbers with no specific requirements.
                    default: {
                        size_t buffer;

                        // Correctly cast binary values based on type.
                        switch (value.get_number_type()) {
                            case number_type::floating_point_number: *((float*)&buffer) = (float)value.get_double(); break;
                            case number_type::signed_integer: *((int*)&buffer) = (int)value; break;
                            default: buffer = value.get_uint64(); break;
                        }

                        UnsignedNumericQueryComparison* cmp = reinterpret_cast<UnsignedNumericQueryComparison*>(&conditions[conditions_count]);
                        cmp->op = where_compare_op::NUMERIC_EQUAL;
                        cmp->column_index = column.index;
                        cmp->comparator = buffer;
                    }
                }
            }

            conditions_count++;
            if (conditions_count >= MAX_VARIABLE_OPERATION_COUNT) throw query_compiler::exception(error::TOO_MANY_CMP_OPS);
        }

        // Prevent smart pointers from deallocating.
        conditions.release();

        return compiled_query.release();
    }
};