#pragma once

#include "stdint.h"
#include <cstdint>
#include <string_view>
#include <sys/types.h>
#include "../deps/simdjson/simdjson.h"
#include "../deps/rapidjson/document.h"
#include "table.h"
#include "compiled-query.h"

namespace query_compiler {
    enum error {
        COLUMN_NOT_FOUND,
        RETURN_COLUMN_NOT_FOUND,
        TOO_MANY_CMP_OPS,
        TOO_MANY_UPDATE_OPS,
        INVALID_CONDITION,
        INVALID_OPTION_SETTING,
        DUPLICATE_COLUMNS,
        UNSPECIFIED_COLUMNS
    };

    extern const rapidjson::GenericStringRef<char> error_text[];

    class exception : public std::exception {
        public:
            exception(query_compiler::error errorCode) : errorCode_(errorCode) {};
            query_compiler::error error() const {
                return errorCode_;
            }

            virtual const char* what() const noexcept override {
                return error_text[errorCode_];
            }

        private:
            query_compiler::error errorCode_;
    };

    // Functions.

    CompiledFindQuery* compile_find_query(ActiveTable* table, simdjson::ondemand::object& query_object);
    CompiledInsertQuery* compile_insert_query(ActiveTable* table, simdjson::ondemand::object& query_object);
    CompiledEraseQuery* compile_erase_query(ActiveTable* table, simdjson::ondemand::object& query_object);
    CompiledUpdateQuery* compile_update_query(ActiveTable* table, simdjson::ondemand::object& query_object);
};