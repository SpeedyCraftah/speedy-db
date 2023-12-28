#pragma once

#include <cstddef>
#include "stdint.h"

enum types: uint32_t {
    integer,
    float32,
    long64,
    byte,
    string
};

union NumericType {
    size_t unsigned64_raw;
    uint32_t unsigned32_raw;
    long long64;
    uint8_t byte;
    int int32;
    float float32;
};