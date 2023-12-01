#include "query-compiler.h"
#include "table.h"
#include <memory>
#include <string_view>
#include <exception> 

using simdjson::fallback::ondemand::json_type;

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

            auto column_find = table->columns.find(key);
            if (column_find == table->columns.end()) throw query_compiler::exception(error::COLUMN_NOT_FOUND);
            

            conditions_count++;
            if (conditions_count >= MAX_VARIABLE_OPERATION_COUNT) throw query_compiler::exception(error::TOO_MANY_CMP_OPS);
        }

        // Prevent smart pointers from deallocating.
        conditions.release();

        return compiled_query.release();
    }
};