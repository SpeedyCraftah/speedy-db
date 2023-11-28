#pragma once

#include "stdint.h"

namespace query_compiler {
    enum WHERE_COMPARE_OP : uint8_t {
        EQUAL,
        LARGER_THAN,
        LARGER_THAN_EQUAL_TO,
        LESS_THAN,
        LESS_THAN_EQUAL_TO,
        STRING_CONTAINS
    };
};