#include "crypto.h"
#include <cstdlib>
#include <exception>
#include <openssl/bn.h>
#include <openssl/core_names.h>
#include <openssl/crypto.h>
#include <openssl/dh.h>
#include <openssl/err.h>
#include "base64.h"
#include <stdexcept>
#include <stdlib.h>
#include <openssl/pem.h>
#include <openssl/bio.h>
#include <openssl/evp.h>
#include <openssl/kdf.h>
#include <openssl/rand.h>
#include <time.h>
#include "../permissions/accounts.h"

EVP_PKEY* crypto::dh::create_session() {
    EVP_PKEY_CTX* pctx = EVP_PKEY_CTX_new_from_name(nullptr, "DH", nullptr);
    if (!pctx) {
        ERR_print_errors_fp(stderr);
        throw std::runtime_error("EVP_PKEY_CTX_new_from_name failed");
    }

    EVP_PKEY* params = nullptr;
    if (EVP_PKEY_paramgen_init(pctx) <= 0) {
        ERR_print_errors_fp(stderr);
        EVP_PKEY_CTX_free(pctx);
        throw std::runtime_error("Could not initialize parameters for DH");
    }

    if (EVP_PKEY_CTX_set_dh_nid(pctx, NID_ffdhe2048) <= 0) {
        ERR_print_errors_fp(stderr);
        EVP_PKEY_CTX_free(pctx);
        throw std::runtime_error("Could not set pre-defined DH group");
    }

    if (EVP_PKEY_paramgen(pctx, &params) <= 0) {
        ERR_print_errors_fp(stderr);
        EVP_PKEY_CTX_free(pctx);
        throw std::runtime_error("Could not generate parameters for DH");
    }

    EVP_PKEY_CTX_free(pctx);

    EVP_PKEY_CTX* kctx = EVP_PKEY_CTX_new(params, nullptr);
    if (!kctx) {
        EVP_PKEY_free(params);
        ERR_print_errors_fp(stderr);
        throw std::runtime_error("EVP_PKEY_CTX_new(params) failed");
    }

    if (EVP_PKEY_keygen_init(kctx) <= 0) {
        EVP_PKEY_free(params);
        EVP_PKEY_CTX_free(kctx);
        ERR_print_errors_fp(stderr);
        throw std::runtime_error("EVP_PKEY_keygen_init failed");
    }

    EVP_PKEY* keypair = nullptr;
    if (EVP_PKEY_keygen(kctx, &keypair) <= 0) {
        EVP_PKEY_free(params);
        EVP_PKEY_CTX_free(kctx);
        ERR_print_errors_fp(stderr);
        throw std::runtime_error("EVP_PKEY_keygen failed");
    }

    EVP_PKEY_free(params);
    EVP_PKEY_CTX_free(kctx);

    return keypair;
}

std::unique_ptr<uint8_t> crypto::dh::compute_secret(EVP_PKEY* local_key, const std::string_view raw_foreign_key) {
    size_t decoded_foreign_key_length = base64::decode_res_length((char*)raw_foreign_key.data(), raw_foreign_key.length());
    uint8_t* decoded_foreign_key = (uint8_t*)malloc(decoded_foreign_key_length);
    if (base64::decode((char*)raw_foreign_key.data(), raw_foreign_key.length(), (char*)decoded_foreign_key) == -1) {
        free(decoded_foreign_key);
        throw std::runtime_error("Invalid base64 DH public key from peer");
    }

    EVP_PKEY* foreign_key = EVP_PKEY_new();
    if (!foreign_key) {
        free(decoded_foreign_key);
        ERR_print_errors_fp(stderr);
        throw std::runtime_error("EVP_PKEY_new failed");
    }

    // Copy the parameters from our key onto the foreign key.
    if (!EVP_PKEY_copy_parameters(foreign_key, local_key)) {
        EVP_PKEY_free(foreign_key);
        free(decoded_foreign_key);
        ERR_print_errors_fp(stderr);
        throw std::runtime_error("EVP_PKEY_copy_parameters failed");
    }

    if (!EVP_PKEY_set1_encoded_public_key(foreign_key, (const unsigned char*)decoded_foreign_key, decoded_foreign_key_length)) {
        EVP_PKEY_free(foreign_key);
        free(decoded_foreign_key);
        ERR_print_errors_fp(stderr);
        throw std::runtime_error("EVP_PKEY_set1_encoded_public_key failed");
    }

    free(decoded_foreign_key);

    // Create a session for deriving the secret.
    EVP_PKEY_CTX* dctx = EVP_PKEY_CTX_new(local_key, nullptr);
    if (!dctx) {
        EVP_PKEY_free(foreign_key);
        ERR_print_errors_fp(stderr);
        throw std::runtime_error("EVP_PKEY_CTX_new(local) failed");
    }

    if (EVP_PKEY_derive_init(dctx) <= 0 || EVP_PKEY_derive_set_peer(dctx, foreign_key) <= 0) {
        EVP_PKEY_free(foreign_key);
        EVP_PKEY_CTX_free(dctx);
        ERR_print_errors_fp(stderr);
        throw std::runtime_error("EVP_PKEY_derive_init / set_peer failed");
    }

    size_t secret_length;
    if (EVP_PKEY_derive(dctx, nullptr, &secret_length) <= 0) {
        EVP_PKEY_free(foreign_key);
        EVP_PKEY_CTX_free(dctx);
        ERR_print_errors_fp(stderr);
        throw std::runtime_error("EVP_PKEY_derive (size) failed");
    }

    if (secret_length < MAX_DH_KEY_DERIVE_SIZE) {
        EVP_PKEY_free(foreign_key);
        EVP_PKEY_CTX_free(dctx);
        ERR_print_errors_fp(stderr);
        throw std::runtime_error("Generated DH key is less than the amount required by MAX_DH_KEY_DERIVE_SIZE");
    }

    // Prepare the secret output.
    uint8_t* secret = new uint8_t[secret_length];
    EVP_PKEY_derive(dctx, secret, &secret_length);

    EVP_PKEY_CTX_free(dctx);
    EVP_PKEY_free(foreign_key);

    return std::unique_ptr<uint8_t>(secret);
}

std::string crypto::dh::export_prime(EVP_PKEY* local_key) {
    BIGNUM* p = nullptr;
    if (EVP_PKEY_get_bn_param(local_key, "p", &p) <= 0 || !p) {
        throw std::runtime_error("Failed to get DH prime 'p'");
    }

    int num_bytes = BN_num_bytes(p);
    uint8_t* prime = (uint8_t*)malloc(num_bytes);
    BN_bn2bin(p, prime);
    BN_free(p);

    std::string b64_prime;
    b64_prime.resize(base64::encode_res_length(num_bytes));
    base64::encode((char*)prime, num_bytes, &b64_prime[0]);
    free(prime);

    return b64_prime;
}

std::string crypto::dh::export_public_key(EVP_PKEY* local_key) {
    BIGNUM* pub = nullptr;
    int out = EVP_PKEY_get_bn_param(local_key, OSSL_PKEY_PARAM_PUB_KEY, &pub);
    if (out <= 0 || !pub) {
        throw std::runtime_error("Failed to get DH public key 'y'");
    }

    int num_bytes = BN_num_bytes(pub);
    uint8_t* pub_bytes = (uint8_t*)malloc(num_bytes);
    BN_bn2bin(pub, pub_bytes);
    BN_free(pub);

    std::string b64_public_key;
    b64_public_key.resize(base64::encode_res_length(num_bytes));
    base64::encode((char*)pub_bytes, num_bytes, &b64_public_key[0]);
    free(pub_bytes);

    return b64_public_key;
}

void crypto::random_bytes(void* dest, int size) {
    if (RAND_bytes((unsigned char*)dest, size) != 1) {
        throw std::runtime_error("Failed to generate cryptographically secure random bytes");
    }
}

// Encrypts the buffer and includes an IV at the beginning.
void crypto::aes256::encrypt_buffer(EVP_CIPHER_CTX* ctx, const char* key, const char* input, size_t input_size, char* output) {
    int outlen1;
    int outlen2;

    // Generate a random IV at the beginning.
    crypto::random_bytes(output, AES_IV_SIZE);

    // Carry on with the rest of the encryption.
    EVP_EncryptInit(ctx, EVP_aes_256_cbc(), (unsigned char*)key, (unsigned char*)output);
    EVP_EncryptUpdate(ctx, (unsigned char*)output + AES_IV_SIZE, &outlen1, (unsigned char*)input, input_size);
    EVP_EncryptFinal(ctx, (unsigned char*)output + AES_IV_SIZE + outlen1, &outlen2);
}

// Decrypts the buffer, taking the first AES_IV_SIZE bytes as IV.
size_t crypto::aes256::decrypt_buffer(EVP_CIPHER_CTX* ctx, const char* key, const char* input, size_t input_size, char* output) {
    int outlen1;
    int outlen2;

    EVP_DecryptInit(ctx, EVP_aes_256_cbc(), (unsigned char*)key, (unsigned char*)input);
    EVP_DecryptUpdate(ctx, (unsigned char*)output, &outlen1, (unsigned char*)input + AES_IV_SIZE, input_size - AES_IV_SIZE);
    EVP_DecryptFinal(ctx, (unsigned char*)output + outlen1, &outlen2);

    return outlen1 + outlen2;
}

#define SALT_LENGTH 32
#define HASH_LENGTH 32
#define ITERATIONS 10000

void crypto::password::hash(std::string_view plaintext_password, AccountPassword* out) {
    // Generate the 32-byte salt and load it into the salt pointer.
    RAND_bytes((unsigned char*)out->salt, SALT_LENGTH);

    // Hash the password with the salt and load the resulting hash into out_hash.
    PKCS5_PBKDF2_HMAC(
        plaintext_password.data(), plaintext_password.length(),
        (unsigned char*)out->salt, SALT_LENGTH,
        ITERATIONS, EVP_sha256(), HASH_LENGTH,
        (unsigned char*)out->hash
    );
}

bool crypto::password::equal(std::string_view plaintext_password, AccountPassword* hashed_password) {
    char hash[HASH_LENGTH];

    // Hash the plaintext password with the salt.
    PKCS5_PBKDF2_HMAC(
        plaintext_password.data(), plaintext_password.length(),
        (unsigned char*)hashed_password->salt, SALT_LENGTH,
        ITERATIONS, EVP_sha256(), HASH_LENGTH, (unsigned char*)hash
    );

    // Compare the passwords with a timing-attack resistant version of memcmp for added security.
    int result = CRYPTO_memcmp(hash, hashed_password->hash, HASH_LENGTH);

    // Return true if result == 0 (equal).
    return result == 0;
}