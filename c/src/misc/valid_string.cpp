#include "valid_string.h"
#include <algorithm>

bool misc::name_string_legal(std::string& name) {
    // Iterate over string and check if each character is an alpha, digit or dash/underscore character.
    bool valid = std::all_of(name.begin(), name.end(), [](char c) {
        return std::isalpha(c) || std::isdigit(c) || c == '-' || c == '_';
    });

    return valid;
}