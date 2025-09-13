#pragma once

#include <stdint.h>
#include <string_view>
#include <unordered_map>
#include <unordered_set>
#include "table-reusable-types.h"
#include "../structures/fast_variant.h"
#include "../structures/no_copy.h"
#include "../structures/short_store.h"

/* WARNING: UNDER NO CIRCUMSTANCES SHOULD THE STRUCTURES HERE BE RE-USED AFTER PROCESSING OF THE CURRENT QUERY */
/* STRINGS HERE REFERENCE THE QUERY-PROVIDED STRING BUFFERS WHICH BECOME INVALID AFTER THE QUERY FINISHES! */

namespace query_compiler {
    enum where_compare_op : uint8_t {
        STRING_EQUAL,
        NUMERIC_EQUAL,
        NUMERIC_GREATER_THAN,
        NUMERIC_GREATER_THAN_EQUAL_TO,
        NUMERIC_LESS_THAN,
        NUMERIC_LESS_THAN_EQUAL_TO,
        STRING_CONTAINS,
        NUMERIC_IN_LIST,
        STRING_IN_LIST
    };

    enum update_changes_op : uint8_t {
        STRING_SET,
        NUMERIC_SET
    };

    
    struct QueryComparator {
        struct String : NoCopy {
            std::string_view comparator;
            size_t comparator_hash;
        };
    
        // Used for signed/float comparisons as well, excluding LG/LT.
        struct Numeric : NoCopy {
            NumericType comparator;
        };

        struct NumericInList : NoCopy {
            std::unordered_set<size_t> list;
        };

        struct StringInList : NoCopy {
            std::unordered_map<size_t, speedystd::short_store<std::string_view>> list;
            uint32_t longest_string_length;
            uint32_t shortest_string_length;
        };

        using ComparatorInfo = speedystd::fast_variant<
            String,
            Numeric,
            NumericInList,
            StringInList
        >;

        where_compare_op op;
        uint32_t column_index;
        bool negated;

        ComparatorInfo info;
    };

    struct UpdateSet {
        struct Numeric : NoCopy {
            update_changes_op op;
            uint32_t column_index;
    
            NumericType new_value;
        };
    
        struct String : NoCopy {
            update_changes_op op;
            uint32_t column_index;
    
            std::string_view new_value;
            size_t new_value_hash;
        };

        using UpdateInfo = speedystd::fast_variant<
            String,
            Numeric
        >;

        update_changes_op op;
        uint32_t column_index;

        UpdateInfo info;
    };
    
    struct InsertColumn {
        struct Numeric : NoCopy {
            NumericType data;
        };
    
        struct String : NoCopy {
            std::string_view data;
            size_t data_hash;
        };

        using InsertInfo = speedystd::fast_variant<
            Numeric,
            String
        >;

        InsertInfo info;
    };


    struct CompiledFindQuery {
        bool is_static_alloc = false; // Whether conditions and other dynamic-sized arrays should be released or not.

        QueryComparator* conditions = nullptr;
        uint32_t conditions_count = 0;
        
        size_t limit = 0;
        size_t offset = 0;
        size_t columns_returned = SIZE_MAX;

        ~CompiledFindQuery() {
            if (!is_static_alloc && conditions != nullptr) delete[] conditions;
        }
    };

    struct CompiledUpdateQuery {
        bool is_static_alloc = false; // Whether conditions and other dynamic-sized arrays should be released or not.

        QueryComparator* conditions = nullptr;
        uint32_t conditions_count = 0;
        
        UpdateSet* changes = nullptr;
        uint32_t changes_count = 0;
        
        size_t limit = 0;
        
        ~CompiledUpdateQuery() {
            if (!is_static_alloc && conditions != nullptr) delete[] conditions;
            if (!is_static_alloc && changes != nullptr) delete[] changes;
        }
    };

    struct CompiledEraseQuery {
        bool is_static_alloc = false; // Whether conditions and other dynamic-sized arrays should be released or not.

        QueryComparator* conditions = nullptr;
        uint32_t conditions_count = 0;
        
        size_t limit = 0;
        
        ~CompiledEraseQuery() {
            if (!is_static_alloc && conditions_count != 0) delete[] conditions;
        }
    };

    // Values must be the count of table columns and be in the indexed order.
    struct CompiledInsertQuery {
        bool is_static_alloc = false; // Whether conditions and other dynamic-sized arrays should be released or not.
        
        InsertColumn* values = nullptr;

        ~CompiledInsertQuery() {
            if (!is_static_alloc && values != nullptr) delete[] values;
        }
    };
}