#pragma once

#include <cstddef>
#include <string_view>
#include "stdint.h"

enum ColumnType : uint32_t {
    Integer,
    Float32,
    Long64,
    Byte,
    String
};

constexpr std::string_view column_type_to_string(ColumnType type);
constexpr ColumnType string_to_column_type(std::string_view type);
constexpr size_t column_type_alignof(ColumnType type);
constexpr size_t column_type_sizeof(ColumnType type);

union NumericColumnData {
    size_t unsigned64_raw = 0;
    uint32_t unsigned32_raw;
    long long64;
    uint8_t byte;
    int int32;
    float float32;
};