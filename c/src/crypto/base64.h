#pragma once

#include <stdint.h>
#include <stddef.h>
#include <string>

namespace base64 {
    inline size_t encode_res_length(size_t length) {
        return 4 * ((length + 2) / 3);
    }
    
    inline size_t decode_res_length(char* input, size_t length) {
        size_t out_len = length / 4 * 3;
        if (input[length - 1] == '=') out_len--;
        if (input[length - 2] == '=') out_len--;

        return out_len;
    }

    void encode(char* data, size_t in_length, char* dest);
    int decode(char* input, size_t in_length, char* out);
    std::string quick_encode(char* data, size_t in_length);
};