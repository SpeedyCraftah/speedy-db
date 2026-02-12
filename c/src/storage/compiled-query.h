#pragma once

#include <cstdint>
#include <stdint.h>
#include <string_view>
#include <unordered_map>
#include <unordered_set>
#include "../structures/fast_variant.h"
#include "../structures/no_copy.h"
#include "../structures/short_store.h"
#include "./structures/types.h"
#include "table-basic.h"

/* WARNING: UNDER NO CIRCUMSTANCES SHOULD THE STRUCTURES HERE BE RE-USED AFTER PROCESSING OF THE CURRENT QUERY */
/* STRINGS HERE REFERENCE THE QUERY-PROVIDED STRING BUFFERS WHICH BECOME INVALID AFTER THE QUERY FINISHES! */

namespace query_compiler {
    enum WhereCompareOp : uint8_t {
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

    enum UpdateChangesOp : uint8_t {
        STRING_SET,
        NUMERIC_SET
    };

    enum ResultSortMode : int8_t {
        NONE = 0,
        ASCENDING = 1,
        DESCENDING = -1
    };

    
    struct QueryComparator {
        struct String : NoCopy {
            std::string_view comparator;
            size_t comparator_hash;
        };
    
        // Used for signed/float comparisons as well, excluding LG/LT.
        struct Numeric : NoCopy {
            NumericColumnData comparator;
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

        WhereCompareOp op;
        TableColumn* column;
        bool negated;

        ComparatorInfo info;
    };

    struct UpdateSet {
        struct Numeric : NoCopy {
            UpdateChangesOp op;
            TableColumn* column;
    
            NumericColumnData new_value;
        };
    
        struct String : NoCopy {
            UpdateChangesOp op;
            uint32_t column_index;
    
            std::string_view new_value;
            size_t new_value_hash;
        };

        using UpdateInfo = speedystd::fast_variant<
            String,
            Numeric
        >;

        UpdateChangesOp op;
        TableColumn* column;

        UpdateInfo info;
    };
    
    struct InsertColumn {
        struct Numeric : NoCopy {
            NumericColumnData data;
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
        
        ResultSortMode result_sort = ResultSortMode::NONE;

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