#pragma once

#include <openssl/ossl_typ.h>
#include <stdint.h>
#include <openssl/evp.h>
#include <openssl/dh.h>
#include <string>
#include "../permissions/accounts.h"

#define MAX_DH_KEY_DERIVE_SIZE 32

namespace crypto {
    namespace dh {
        EVP_PKEY* create_session();
        std::string export_prime(EVP_PKEY* local_key);
        std::string export_public_key(EVP_PKEY* local_key);
        std::unique_ptr<uint8_t> compute_secret(EVP_PKEY* local_key, const std::string_view raw_foreign_key);
    }

    namespace aes256 {
        void encrypt_buffer(EVP_CIPHER_CTX* ctx, const char* key, const char* iv, const char* input, size_t input_size, char* output);
        size_t decrypt_buffer(EVP_CIPHER_CTX* ctx, const char* key, const char* iv, const char* input, size_t input_size, char* output);

        inline size_t encode_res_length(size_t length) {
            return 16 + (length / 16) * 16;
        }
    };

    namespace password {
        void hash(std::string_view plaintext_password, AccountPassword* out);
        bool equal(std::string_view plaintext_password, AccountPassword* hashed_password);
    };

    void random_bytes(void* dest, int size);
};