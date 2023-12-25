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
    size_t unsigned_raw;
    long long64;
    uint8_t byte;
    int int32;
    float float32;
};