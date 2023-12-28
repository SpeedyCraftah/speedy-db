#pragma once

#include "compiled-query.h"
#include "query-compiler.h"
#include "table-reusable-types.h"
#include "table.h"
#include "../deps/xxh/xxh3.h"
#include <string_view>

namespace query_builder {
    using namespace query_compiler;

    //#define where_count 3
    //#define updates_count 3

    // Prototypes.
    class general_query {
        protected:
            ActiveTable* table;

            // Resolve index of column from name.
            inline uint32_t resolve_column_index(std::string_view column_name) {
                table_column* column = table->columns.find(column_name)->second;
                return column->index;
            }
    };

    template<size_t where_count>
    class general_where_query : public general_query {
        public:
            static StringQueryComparison string_equal_to(std::string_view comparator) {
                StringQueryComparison cmp;
                cmp.op = where_compare_op::STRING_EQUAL;
                cmp.comparator = comparator;
                cmp.comparator_hash = XXH64(comparator.data(), comparator.size(), HASH_SEED);

                return cmp;
            }

            static NumericQueryComparison numeric_equal_to(NumericType comparator) {
                NumericQueryComparison cmp;
                cmp.op = where_compare_op::NUMERIC_EQUAL;
                cmp.comparator = comparator;
                
                return cmp;
            }

            void add_where_condition(std::string_view column_name, StringQueryComparison cmp) {
                cmp.column_index = resolve_column_index(column_name);
                conditions[conditions_i].string = cmp;
                conditions_i++;
            }

            void add_where_condition(std::string_view column_name, NumericQueryComparison cmp) {
                cmp.column_index = resolve_column_index(column_name);
                conditions[conditions_i].numeric = cmp;
                conditions_i++;
            }

            inline void set_seek_direction(bool sd) {
                this->seek_direction = sd;
            }

            inline void set_limit(size_t limit) {
                this->limit = limit;
            }

        protected:
            int conditions_i = 0;
            QueryComparison conditions[where_count];

            bool seek_direction = 1;
            size_t limit = 0;
    };


    // Queries.

    template<size_t where_count>
    class find_query : public general_where_query<where_count> {
        public:
            find_query(ActiveTable* t) { this->table = t; }

            CompiledFindQuery* build() {
                this->query.conditions = this->conditions;
                this->query.conditions_count = this->conditions_i;
                this->query.limit = this->limit;
                this->query.seek_direction = this->seek_direction;

                return &this->query;
            }

        private:
            CompiledFindQuery query;
    };

    template<size_t where_count, size_t updates_count>
    class update_query : public general_where_query<where_count> {
        public:
            update_query(ActiveTable* t) { this->table = t; }

            static StringUpdateSet update_string(std::string_view new_value) {
                StringUpdateSet update;
                update.op = update_changes_op::STRING_SET;
                update.new_value = new_value;
                update.new_value_hash = XXH64(new_value.data(), new_value.size(), HASH_SEED);
                
                return update;
            }

            static NumericUpdateSet update_numeric(NumericType value) {
                NumericUpdateSet update;
                update.op = update_changes_op::NUMERIC_SET;
                update.new_value = value;
                
                return update;
            }

            void add_change(std::string_view column_name, StringUpdateSet update) {
                update.column_index = this->resolve_column_index(column_name);
                updates[updates_i].string = update;
                updates_i++;
            }

            void add_change(std::string_view column_name, NumericUpdateSet update) {
                update.column_index = this->resolve_column_index(column_name);
                updates[updates_i].numeric = update;
                updates_i++;
            }

            CompiledUpdateQuery* build() {
                this->query.conditions = this->conditions;
                this->query.conditions_count = this->conditions_i;
                this->query.changes = this->updates;
                this->query.changes_count = this->updates_i;
                this->query.limit = this->limit;
                this->query.seek_direction = this->seek_direction;

                return &this->query;
            }

        private:
            int updates_i = 0;
            UpdateSet updates[updates_count];

            CompiledUpdateQuery query;
    };

    template<size_t columns_count>
    class insert_query : public general_query {
        public:
            insert_query(ActiveTable* t) { this->table = t; }

            void set_value(std::string_view column_name, std::string_view value) {
                StringInsertColumn& column = values[this->resolve_column_index(column_name)].string;
                column.data = value;
                column.data_hash = XXH64(value.data(), value.size(), HASH_SEED);
            }

            void set_value(std::string_view column_name, NumericType value) {
                NumericInsertColumn& column = values[this->resolve_column_index(column_name)].numeric;
                column.data = value;
            }

            CompiledInsertQuery* build() {
                query.values = values;

                return &this->query;
            }
        
        private:
            InsertColumn values[columns_count];

            CompiledInsertQuery query;
    };

    template<size_t where_count>
    class erase_query : public general_where_query<where_count> {
        public:
            erase_query(ActiveTable* t) { this->table = t; }

            CompiledEraseQuery* build() {
                query.conditions = this->conditions;
                query.conditions_count = this->conditions_i;
                query.limit = this->limit;
                query.seek_direction = this->seek_direction;

                return &query;
            }

        private:
            CompiledEraseQuery query;
    };
};