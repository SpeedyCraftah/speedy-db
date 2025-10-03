#include "types.h"
#include "../table.h"
#include <string_view>

// Converts a column type into a readable string.
constexpr std::string_view column_type_to_string(ColumnType type) {
    if (type == ColumnType::Integer) return "integer";
    else if (type == ColumnType::String) return "string";
    else if (type == ColumnType::Byte) return "byte";
    else if (type == ColumnType::Float32) return "float";
    else if (type == ColumnType::Long64) return "long";
}

// Converts a string name into a column type.
constexpr ColumnType string_to_column_type(std::string_view type) {
    if (type == "integer") return ColumnType::Integer;
    else if (type == "string") return ColumnType::String;
    else if (type == "byte") return ColumnType::Byte;
    else if (type == "float") return ColumnType::Float32;
    else if (type == "long") return ColumnType::Long64;
    else return (ColumnType)-1;
}

// Returns the physical alignment required by the type, which may differ from the alignment imposed by the compiler.
constexpr size_t column_type_alignof(ColumnType type) {
    if (type == ColumnType::Integer) return alignof(int);
    else if (type == ColumnType::String) return alignof(size_t);
    else if (type == ColumnType::Byte) return alignof(uint8_t);
    else if (type == ColumnType::Float32) return alignof(float);
    else if (type == ColumnType::Long64) return alignof(long);
}

// Returns the physical size of the type, without any padding.
constexpr size_t column_type_sizeof(ColumnType type) {
    if (type == ColumnType::Integer) return sizeof(int);
    else if (type == ColumnType::String) return sizeof(TableHashedColumn);
    else if (type == ColumnType::Byte) return sizeof(uint8_t);
    else if (type == ColumnType::Float32) return sizeof(float);
    else if (type == ColumnType::Long64) return sizeof(long);
}
