#include "crypto.h"
#include <exception>
#include <openssl/bn.h>
#include <openssl/crypto.h>
#include <openssl/dh.h>
#include "base64.h"
#include <stdlib.h>
#include <openssl/pem.h>
#include <openssl/bio.h>
#include <openssl/evp.h>
#include <openssl/kdf.h>
#include <openssl/rand.h>
#include <time.h>
#include "../permissions/accounts.h"

DH* crypto::dh::create_session() {
    DH* dh = DH_new();
    
    if (DH_generate_parameters_ex(dh, 1024, 2, NULL) == -1) {
        OPENSSL_free(dh);
        throw std::exception();
    }

    if (DH_generate_key(dh) == -1) {
        OPENSSL_free(dh);
        throw std::exception();
    }

    return dh;
}

char* crypto::dh::compute_secret(DH* dh, const std::string& foreign_key) {
    BIGNUM* key_bn = BN_new();
    size_t key_bin_len = base64::decode_res_length((char*)foreign_key.c_str(), foreign_key.length());

    char* key_buffer = (char*)malloc(key_bin_len);
    if (base64::decode((char*)foreign_key.c_str(), foreign_key.length(), key_buffer) == -1) {
        free(key_buffer);
        OPENSSL_free(key_bn);
        throw std::exception();
    }

    BN_bin2bn((unsigned char*)key_buffer, key_bin_len, key_bn);
    free(key_buffer);

    char* secret = (char*)malloc(128);
    if (DH_compute_key((unsigned char*)secret, key_bn, dh) != 128) {
        free(secret);
        OPENSSL_free(key_bn);
        throw std::exception();
    }

    OPENSSL_free(key_bn);
    return secret;
}

std::string crypto::dh::export_prime(DH* dh) {
    const BIGNUM* bn = DH_get0_p(dh);
    size_t bn_size = BN_num_bytes(bn);

    char* buffer = (char*)malloc(bn_size);
    BN_bn2bin(bn, (unsigned char*)buffer);
    
    std::string b64_out(base64::encode_res_length(bn_size), '\0');
    base64::encode(buffer, bn_size, (char*)b64_out.c_str());

    free(buffer);
    return b64_out;
}

std::string crypto::dh::export_public_key(DH* dh) {
    const BIGNUM* bn = DH_get0_pub_key(dh);
    size_t bn_size = BN_num_bytes(bn);

    char* buffer = (char*)malloc(bn_size);
    BN_bn2bin(bn, (unsigned char*)buffer);
    
    std::string b64_out(base64::encode_res_length(bn_size), '\0');
    base64::encode(buffer, bn_size, const_cast<char*>(b64_out.c_str()));

    free(buffer);
    return b64_out;
}

void crypto::random_bytes(void* dest, size_t size) {
    char* ptr = (char*)dest;
    srand(time(NULL));
    for (size_t i = 0; i < size; i++) {
        ptr[i] = rand() % 255;
    }
    
}

void crypto::aes256::encrypt_buffer(EVP_CIPHER_CTX* ctx, const char* key, const char* iv, const char* input, size_t input_size, const char* output) {
    int outlen1;
    int outlen2;

    EVP_EncryptInit(ctx, EVP_aes_256_cbc(), (unsigned char*)key, (unsigned char*)iv);
    EVP_EncryptUpdate(ctx, (unsigned char*)output, &outlen1, (unsigned char*)input, input_size);
    EVP_EncryptFinal(ctx, (unsigned char*)output + outlen1, &outlen2);
}

size_t crypto::aes256::decrypt_buffer(EVP_CIPHER_CTX* ctx, const char* key, const char* iv, const char* input, size_t input_size, const char* output) {
    int outlen1;
    int outlen2;

    // SEGF here
    EVP_DecryptInit(ctx, EVP_aes_256_cbc(), (unsigned char*)key, (unsigned char*)iv);
    
    EVP_DecryptUpdate(ctx, (unsigned char*)output, &outlen1, (unsigned char*)input, input_size);
    EVP_DecryptFinal(ctx, (unsigned char*)output + outlen1, &outlen2);

    return outlen1 + outlen2;
}

#define SALT_LENGTH 32
#define HASH_LENGTH 32
#define ITERATIONS 10000

void crypto::password::hash(char* plaintext_password, AccountPassword* out) {
    // Generate the 32-byte salt and load it into the salt pointer.
    RAND_bytes((unsigned char*)out->salt, SALT_LENGTH);

    // Hash the password with the salt and load the resulting hash into out_hash.
    PKCS5_PBKDF2_HMAC(plaintext_password, strlen(plaintext_password), (unsigned char*)out->salt, SALT_LENGTH, ITERATIONS, EVP_sha256(), HASH_LENGTH, (unsigned char*)out->hash);
}

bool crypto::password::equal(char* plaintext_password, AccountPassword* hashed_password) {
    char hash[32];

    // Hash the plaintext password with the salt.
    PKCS5_PBKDF2_HMAC(plaintext_password, strlen(plaintext_password), (unsigned char*)hashed_password->salt, SALT_LENGTH, ITERATIONS, EVP_sha256(), HASH_LENGTH, (unsigned char*)hash);

    // Compare the passwords with a timing-attack resistant version of memcmp for added security.
    int result = CRYPTO_memcmp(hash, hashed_password->hash, HASH_LENGTH);

    // Return true if result == 0 (equal).
    return result == 0;
}