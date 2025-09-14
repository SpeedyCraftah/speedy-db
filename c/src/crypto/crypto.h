#pragma once

#include <openssl/ossl_typ.h>
#include <stdint.h>
#include <openssl/evp.h>
#include <openssl/dh.h>
#include <string>
#include "../permissions/accounts.h"

namespace crypto {
    namespace dh {
        DH* create_session();
        std::string export_prime(DH* dh);
        std::string export_public_key(DH* dh);
        char* compute_secret(DH* dh, const std::string& foreign_key);
    }

    namespace aes256 {
        void encrypt_buffer(EVP_CIPHER_CTX* ctx, const char* key, const char* iv, const char* input, size_t input_size, const char* output);
        size_t decrypt_buffer(EVP_CIPHER_CTX* ctx, const char* key, const char* iv, const char* input, size_t input_size, const char* output);

        inline size_t encode_res_length(size_t length) {
            return 16 + (length / 16) * 16;
        }
    };

    namespace password {
        void hash(std::string_view plaintext_password, AccountPassword* out);
        bool equal(std::string_view plaintext_password, AccountPassword* hashed_password);
    };

    void random_bytes(void* dest, size_t size);
};