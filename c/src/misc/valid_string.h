#pragma once

#include <regex>
#include <string>

namespace misc {
    bool name_string_legal(std::string_view name);
    bool column_name_string_legal(std::string_view name);
}