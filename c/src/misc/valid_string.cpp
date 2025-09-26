#include "valid_string.h"
#include <algorithm>
#include <regex>

bool misc::name_string_legal(std::string_view name) {
    if (name.length() > 32 || name.length() < 2) return false;

    // Iterate over string and check if each character is an alpha, digit, lowercase or dash/underscore character.
    bool valid = std::all_of(name.begin(), name.end(), [](char c) {
        return (std::isalpha(c) && std::islower(c)) || std::isdigit(c) || c == '-' || c == '_';
    });

    return valid;
}

const std::regex column_name_regex = std::regex("^[a-z_]+$");
bool misc::column_name_string_legal(std::string_view name) {
    if (name.length() > 32 || name.length() < 2) return false;
    if (!std::regex_match(name.begin(), name.end(), column_name_regex)) return false;

    return true;
}