#pragma once

#include "compiled-query.h"
#include "query-compiler.h"
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
            static QueryComparator string_equal_to(std::string_view comparator) {
                QueryComparator cmp;
                cmp.op = where_compare_op::STRING_EQUAL;
                cmp.negated = false;
                cmp.info.set_as<QueryComparator::String>();
                cmp.info.as<QueryComparator::String>().comparator = comparator;
                cmp.info.as<QueryComparator::String>().comparator_hash = XXH64(comparator.data(), comparator.size(), HASH_SEED);

                return cmp;
            }

            static QueryComparator string_not_equal_to(std::string_view comparator) {
                QueryComparator cmp = string_equal_to(comparator);
                cmp.negated = true;

                return cmp;
            }

            static QueryComparator numeric_equal_to(NumericColumnData comparator) {
                QueryComparator cmp;
                cmp.op = where_compare_op::NUMERIC_EQUAL;
                cmp.negated = false;
                cmp.info.set_as<QueryComparator::Numeric>();
                cmp.info.as<QueryComparator::Numeric>().comparator = comparator;
                
                return cmp;
            }

            static QueryComparator numeric_not_equal_to(NumericColumnData comparator) {
                QueryComparator cmp = numeric_equal_to(comparator);
                cmp.negated = true;

                return cmp;
            }

            void add_where_condition(std::string_view column_name, QueryComparator&& cmp) {
                cmp.column_index = resolve_column_index(column_name);
                conditions[conditions_i] = std::move(cmp);
                conditions_i++;
            }

            inline void set_limit(size_t limit) {
                this->limit = limit;
            }

        protected:
            int conditions_i = 0;
            QueryComparator conditions[where_count];

            size_t limit = 0;
    };


    // Queries.

    template<size_t where_count>
    class find_query : public general_where_query<where_count> {
        public:
            find_query(ActiveTable* t) { this->table = t; }

            inline void set_offset(size_t offset) {
                this->offset = offset;
            }

            CompiledFindQuery* build() {
                this->query.is_static_alloc = true; // Important! Arrays here are released automatically once the class is dropped.
                this->query.conditions = this->conditions;
                this->query.conditions_count = this->conditions_i;
                this->query.limit = this->limit;
                this->query.offset = this->offset;

                return &this->query;
            }

        private:
            CompiledFindQuery query;
            size_t offset = 0;
    };

    template<size_t where_count, size_t updates_count>
    class update_query : public general_where_query<where_count> {
        public:
            update_query(ActiveTable* t) { this->table = t; }

            static UpdateSet update_string(std::string_view new_value) {
                UpdateSet update;
                update.op = update_changes_op::STRING_SET;
                update.info.set_as<UpdateSet::String>();
                update.info.as<UpdateSet::String>().new_value = new_value;
                update.info.as<UpdateSet::String>().new_value_hash = XXH64(new_value.data(), new_value.size(), HASH_SEED);
                
                return update;
            }

            static UpdateSet update_numeric(NumericColumnData value) {
                UpdateSet update;
                update.op = update_changes_op::NUMERIC_SET;
                update.info.set_as<UpdateSet::Numeric>();
                update.info.as<UpdateSet::Numeric>().new_value = value;
                
                return update;
            }

            void add_change(std::string_view column_name, UpdateSet&& update) {
                update.column_index = this->resolve_column_index(column_name);
                updates[updates_i] = std::move(update);
                updates_i++;
            }

            CompiledUpdateQuery* build() {
                this->query.is_static_alloc = true; // Important! Arrays here are released automatically once the class is dropped.
                this->query.conditions = this->conditions;
                this->query.conditions_count = this->conditions_i;
                this->query.changes = this->updates;
                this->query.changes_count = this->updates_i;
                this->query.limit = this->limit;

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
                InsertColumn& column = values[this->resolve_column_index(column_name)];
                column.info.set_as<InsertColumn::String>();
                column.info.as<InsertColumn::String>().data = value;
                column.info.as<InsertColumn::String>().data_hash = XXH64(value.data(), value.size(), HASH_SEED);
            }

            void set_value(std::string_view column_name, NumericColumnData value) {
                InsertColumn& column = values[this->resolve_column_index(column_name)];
                column.info.set_as<InsertColumn::Numeric>();
                column.info.as<InsertColumn::Numeric>().data = value;
            }

            CompiledInsertQuery* build() {
                query.is_static_alloc = true; // Important! Arrays here are released automatically once the class is dropped.
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
                query.is_static_alloc = true; // Important! Arrays here are released automatically once the class is dropped.
                query.conditions = this->conditions;
                query.conditions_count = this->conditions_i;
                query.limit = this->limit;

                return &query;
            }

        private:
            CompiledEraseQuery query;
    };
};