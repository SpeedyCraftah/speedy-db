#pragma once

#include <stdint.h>
#include <string_view>

namespace query_compiler {
    enum where_compare_op : uint8_t {
        STRING_EQUAL,
        NUMERIC_EQUAL,
        NUMERIC_GREATER_THAN,
        NUMERIC_GREATER_THAN_EQUAL_TO,
        NUMERIC_LESS_THAN,
        NUMERIC_LESS_THAN_EQUAL_TO,
        STRING_CONTAINS
    };

    enum update_changes_op : uint8_t {
        STRING_SET,
        NUMERIC_SET
    };

    struct GenericQueryComparison {
        where_compare_op op;
        uint32_t column_index;

        char _padding[24];
    };

    struct StringQueryComparison {
        where_compare_op op;
        uint32_t column_index;

        std::string_view comparator;
        size_t comparator_hash;
    };

    // Used for signed/float comparisons as well, excluding LG/LT.
    struct NumericQueryComparison {
        where_compare_op op;
        uint32_t column_index;

        size_t comparator;
        
        char _padding[16];
    };


    struct GenericUpdate {
        update_changes_op op;
        uint32_t column_index;

        char _padding[24];
    };

    struct NumericUpdateSet {
        update_changes_op op;
        uint32_t column_index;

        size_t new_value;

        char _padding[16];
    };

    struct StringUpdateSet {
        update_changes_op op;
        uint32_t column_index;

        std::string_view new_value;
        size_t new_value_hash;
    };


    struct GenericInsertColumn {
        char _padding[24];
    };

    struct NumericInsertColumn {
        size_t data;

        char _padding[16];
    };

    struct StringInsertColumn {
        std::string_view data;
        size_t data_hash;
    };


    // TODO note - compare performance between using bitfield for result limits or an array, or both.
    // TODO - consider memory deallocation

    // TODO - set column amount limit to (sizeof(size_t) * 8)

    struct CompiledFindQuery {
        void destroy() {
            if (conditions_count != 0) delete conditions;
            if (seek_conditions_count != 0) delete seek_conditions;
            delete this;
        }

        GenericQueryComparison* conditions;
        uint32_t conditions_count = 0;

        GenericQueryComparison* seek_conditions;
        uint32_t seek_conditions_count = 0;

        bool seek_direction = true; // false = end-start, true = start-end
        size_t limit = 0;
        size_t columns_returned = SIZE_MAX;
    };

    struct CompiledUpdateQuery {
        void destroy() {
            if (conditions_count != 0) delete conditions;
            if (changes_count != 0) delete changes;
            delete this;
        }

        GenericQueryComparison* conditions;
        uint32_t conditions_count = 0;

        GenericUpdate* changes;
        uint32_t changes_count = 0;

        bool seek_direction = true; // false = end-start, true = start-end
        size_t limit = 0;
    };

    struct CompiledEraseQuery {
        void destroy() {
            if (conditions_count != 0) delete conditions;
            delete this;
        }

        GenericQueryComparison* conditions;
        uint32_t conditions_count = 0;

        bool seek_direction = true; // false = end-start, true = start-end
        size_t limit = 0;
    };

    // Values must be the count of table columns and be in the indexed order.
    struct CompiledInsertQuery {
        void destroy() {
            if (values != nullptr) delete values;
            delete this;
        }

        GenericInsertColumn* values;
    };
}