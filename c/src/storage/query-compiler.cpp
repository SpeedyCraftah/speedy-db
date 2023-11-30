#include "query-compiler.h"
#include "table.h"
#include <memory>
#include <string_view>
#include <exception> 

#define MAX_VARIABLE_OPERATION_COUNT 20

namespace query_compiler {
    class exception : public std::exception {
        
    };

    CompiledFindQuery* compile_find_query(ActiveTable* table, simdjson::ondemand::document& query_object) {
        // TODO - use existing buffer?
        std::unique_ptr<CompiledFindQuery> compiled_query(new CompiledFindQuery);
        
        simdjson::ondemand::object conditions_object = query_object["where"];

        // Create buffer which should cover every condition operation.
        compiled_query->conditions = new GenericQueryComparison[MAX_VARIABLE_OPERATION_COUNT];

        // Iterate over the conditional queries.
        for (auto condition : conditions_object) {
            std::string_view key = condition.unescaped_key();

            auto column_find = table->columns.find(key);
            if (column_find == table->columns.end()) 
            

        }
    }
};