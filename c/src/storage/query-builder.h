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
    class GeneralQuery {
        protected:
            ActiveTable* table;

            // Resolve index of column from name.
            inline TableColumn* resolve_column(std::string_view column_name) {
                return table->columns.find(column_name)->second;
            }
    };

    template<size_t where_count>
    class GeneralWhereQuery : public GeneralQuery {
        public:
            constexpr static QueryComparator string_equal_to(std::string_view comparator) {
                QueryComparator cmp;
                cmp.op = WhereCompareOp::STRING_EQUAL;
                cmp.negated = false;
                cmp.info.set_as<QueryComparator::String>();
                cmp.info.as<QueryComparator::String>().comparator = comparator;
                cmp.info.as<QueryComparator::String>().comparator_hash = XXH64(comparator.data(), comparator.size(), HASH_SEED);

                return cmp;
            }

            constexpr static QueryComparator string_not_equal_to(std::string_view comparator) {
                QueryComparator cmp = string_equal_to(comparator);
                cmp.negated = true;

                return cmp;
            }

            constexpr static QueryComparator numeric_equal_to(NumericColumnData comparator) {
                QueryComparator cmp;
                cmp.op = WhereCompareOp::NUMERIC_EQUAL;
                cmp.negated = false;
                cmp.info.set_as<QueryComparator::Numeric>();
                cmp.info.as<QueryComparator::Numeric>().comparator = comparator;
                
                return cmp;
            }

            constexpr static QueryComparator numeric_not_equal_to(NumericColumnData comparator) {
                QueryComparator cmp = numeric_equal_to(comparator);
                cmp.negated = true;

                return cmp;
            }

            constexpr void add_where_condition(std::string_view column_name, QueryComparator&& cmp) {
                cmp.column = resolve_column(column_name);
                conditions[conditions_i] = std::move(cmp);
                conditions_i++;
            }

            constexpr inline void set_limit(size_t limit) {
                this->limit = limit;
            }

        protected:
            int conditions_i = 0;
            QueryComparator conditions[where_count];

            size_t limit = 0;
    };


    // Queries.

    template<size_t where_count>
    class FindQuery : public GeneralWhereQuery<where_count> {
        public:
            constexpr FindQuery(ActiveTable* t) { this->table = t; }

            constexpr inline void set_offset(size_t offset) {
                this->offset = offset;
            }

            constexpr CompiledFindQuery* build() {
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
    class UpdateQuery : public GeneralWhereQuery<where_count> {
        public:
            constexpr UpdateQuery(ActiveTable* t) { this->table = t; }

            constexpr static UpdateSet update_string(std::string_view new_value) {
                UpdateSet update;
                update.op = UpdateChangesOp::STRING_SET;
                update.info.set_as<UpdateSet::String>();
                update.info.as<UpdateSet::String>().new_value = new_value;
                update.info.as<UpdateSet::String>().new_value_hash = XXH64(new_value.data(), new_value.size(), HASH_SEED);
                
                return update;
            }

            constexpr static UpdateSet update_numeric(NumericColumnData value) {
                UpdateSet update;
                update.op = UpdateChangesOp::NUMERIC_SET;
                update.info.set_as<UpdateSet::Numeric>();
                update.info.as<UpdateSet::Numeric>().new_value = value;
                
                return update;
            }

            constexpr void add_change(std::string_view column_name, UpdateSet&& update) {
                update.column = this->resolve_column(column_name);
                updates[updates_i] = std::move(update);
                updates_i++;
            }

            constexpr CompiledUpdateQuery* build() {
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
    class InsertQuery : public GeneralQuery {
        public:
            constexpr InsertQuery(ActiveTable* t) { this->table = t; }

            constexpr void set_value(std::string_view column_name, std::string_view value) {
                InsertColumn& column = values[this->resolve_column(column_name)->index];
                column.info.set_as<InsertColumn::String>();
                column.info.as<InsertColumn::String>().data = value;
                column.info.as<InsertColumn::String>().data_hash = XXH64(value.data(), value.size(), HASH_SEED);
            }

            constexpr void set_value(std::string_view column_name, NumericColumnData value) {
                InsertColumn& column = values[this->resolve_column(column_name)->index];
                column.info.set_as<InsertColumn::Numeric>();
                column.info.as<InsertColumn::Numeric>().data = value;
            }

            constexpr CompiledInsertQuery* build() {
                query.is_static_alloc = true; // Important! Arrays here are released automatically once the class is dropped.
                query.values = values;

                return &this->query;
            }
        
        private:
            InsertColumn values[columns_count];

            CompiledInsertQuery query;
    };

    template<size_t where_count>
    class EraseQuery : public GeneralWhereQuery<where_count> {
        public:
            constexpr EraseQuery(ActiveTable* t) { this->table = t; }

            constexpr CompiledEraseQuery* build() {
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