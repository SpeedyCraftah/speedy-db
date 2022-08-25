#include <cstddef>
#include <stdint.h>
#include "base64.h"

static constexpr char sEncodingTable[] = {
      'A', 'B', 'C', 'D', 'E', 'F', 'G', 'H',
      'I', 'J', 'K', 'L', 'M', 'N', 'O', 'P',
      'Q', 'R', 'S', 'T', 'U', 'V', 'W', 'X',
      'Y', 'Z', 'a', 'b', 'c', 'd', 'e', 'f',
      'g', 'h', 'i', 'j', 'k', 'l', 'm', 'n',
      'o', 'p', 'q', 'r', 's', 't', 'u', 'v',
      'w', 'x', 'y', 'z', '0', '1', '2', '3',
      '4', '5', '6', '7', '8', '9', '+', '/'
};

static constexpr unsigned char kDecodingTable[] = {
      64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64,
      64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64,
      64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 62, 64, 64, 64, 63,
      52, 53, 54, 55, 56, 57, 58, 59, 60, 61, 64, 64, 64, 64, 64, 64,
      64,  0,  1,  2,  3,  4,  5,  6,  7,  8,  9, 10, 11, 12, 13, 14,
      15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 64, 64, 64, 64, 64,
      64, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40,
      41, 42, 43, 44, 45, 46, 47, 48, 49, 50, 51, 64, 64, 64, 64, 64,
      64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64,
      64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64,
      64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64,
      64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64,
      64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64,
      64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64,
      64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64,
      64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64
};

void base64::encode(char* data, size_t in_length, char* dest) {
    size_t i;

    for (i = 0; i < in_length - 2; i += 3) {
      *dest++ = sEncodingTable[(data[i] >> 2) & 0x3F];
      *dest++ = sEncodingTable[((data[i] & 0x3) << 4) | ((int) (data[i + 1] & 0xF0) >> 4)];
      *dest++ = sEncodingTable[((data[i + 1] & 0xF) << 2) | ((int) (data[i + 2] & 0xC0) >> 6)];
      *dest++ = sEncodingTable[data[i + 2] & 0x3F];
    }

    if (i < in_length) {
      *dest++ = sEncodingTable[(data[i] >> 2) & 0x3F];

      if (i == (in_length - 1)) {
        *dest++ = sEncodingTable[((data[i] & 0x3) << 4)];
        *dest++ = '=';
      }

      else {
        *dest++ = sEncodingTable[((data[i] & 0x3) << 4) | ((int) (data[i + 1] & 0xF0) >> 4)];
        *dest++ = sEncodingTable[((data[i + 1] & 0xF) << 2)];
      }

      *dest++ = '=';
    }    
}

int base64::decode(char* input, size_t in_length, char* out) {
    if (in_length % 4 != 0) return -1;    
    
    size_t out_len = in_length / 4 * 3;

    if (input[in_length - 1] == '=') out_len--;
    if (input[in_length - 2] == '=') out_len--;

    for (size_t i = 0, j = 0; i < in_length;) {
      uint32_t a = input[i] == '=' ? 0 & i++ : kDecodingTable[static_cast<int>(input[i++])];
      uint32_t b = input[i] == '=' ? 0 & i++ : kDecodingTable[static_cast<int>(input[i++])];
      uint32_t c = input[i] == '=' ? 0 & i++ : kDecodingTable[static_cast<int>(input[i++])];
      uint32_t d = input[i] == '=' ? 0 & i++ : kDecodingTable[static_cast<int>(input[i++])];

      uint32_t triple = (a << 3 * 6) + (b << 2 * 6) + (c << 1 * 6) + (d << 0 * 6);

      if (j < out_len) {
        out[j++] = (triple >> 2 * 8) & 0xFF;
      }

      if (j < out_len) {
        out[j++] = (triple >> 1 * 8) & 0xFF;
      }

      if (j < out_len) {
        out[j++] = (triple >> 0 * 8) & 0xFF;
      }
    }
    
    return 0;
}

std::string base64::quick_encode(char* data, size_t in_length) {
    size_t encode_length = base64::encode_res_length(in_length);
    std::string dest(encode_length, '\0');
    base64::encode(data, in_length, (char*)dest.c_str());
    return dest;
}