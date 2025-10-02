#include "types.h"
#include <string_view>

std::string_view column_type_to_string(ColumnType type) {
    if (type == ColumnType::Integer) return "integer";
    else if (type == ColumnType::String) return "string";
    else if (type == ColumnType::Byte) return "byte";
    else if (type == ColumnType::Float32) return "float";
    else if (type == ColumnType::Long64) return "long";
    else return std::string_view("");
}

ColumnType string_to_column_type(std::string_view type) {
    if (type == "integer") return ColumnType::Integer;
    else if (type == "string") return ColumnType::String;
    else if (type == "byte") return ColumnType::Byte;
    else if (type == "float") return ColumnType::Float32;
    else if (type == "long") return ColumnType::Long64;
    else return (ColumnType)-1;
}