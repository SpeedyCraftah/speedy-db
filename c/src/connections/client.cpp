#include "client.h"
#include "../main.h"
#include <cstdint>
#include <cstdlib>
#include <stdio.h>
#include <sys/socket.h>
#include <stdlib.h>
#include <netinet/in.h>
#include "../logging/logger.h"
#include <unistd.h>
#include "handler.h"
#include "../storage/query.h"
#include <chrono>
#include <thread>
#include <exception>
#include <openssl/bn.h>
#include <openssl/crypto.h>
#include <openssl/dh.h>
#include "../crypto/crypto.h"
#include "../crypto/base64.h"
#include <sys/socket.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include "../deps/rapidjson/writer.h"
#include "../deps/rapidjson/stringbuffer.h"
#include "../deps/simdjson/simdjson.h"
#include "../storage/query-compiler.h"

#define MAX_PACKET_SIZE 104857600

// Define the error text list.
const rapidjson::GenericStringRef<char> query_error_text[] = {
    "The provided JSON could not be parsed by the engine.",
    "The total size of the sent data exceeds the maximum packet size. This limit can be increased in the server settings.",
    "The buffer overflow protection has been triggered. This could be due to your query not containing a valid or correctly calculated header/terminator.",
    "An unhandled internal error has occurred while executing this query.",
    "The provided query does not contain all of the mandatory parameters for the requested operation or are not the correct types. Refer to the documentation on solving this.",
    "The configuration handshake has either not been sent, is invalid or contains unsupported types or attributes.",
    "The client is using an outdated version of SpeedDB. This version is too significant to be compatible.",
    "The client is using a never version of SpeedDB than the server. This version is too significant to be compatible.",
    "Your query is either missing a mandatory parameter or uses an inappropriate type for a parameter.",
    "The target table could not be found.",
    "The operation query either does not contain the operation ID or is not a number.",
    "The operation type provided does not exist or is not supported by the database version.",
    "The operation query either does not contain the data entry or is not an object.",
    "The operation query either does not contain a unique nonce or is not a number.",
    "The table name you have specified is already used by another table.",
    "There was insufficient memory available to perform the operation you requested.",
    "The handshake has failed due to incorrect database account credentials provided.",
    "The simulataneous connection limit has been exhausted. Please either disconnect clients, ensure clients disconnect properly or increase the connection limit with max-connections.",
    "The server requests that all clients establish an encrypted connection. Reconnect and supply a public key or adjust the server settings.",
    "The account username you provided for creation has already been taken. Please pick another account username.",
    "The table/column name you have provided is an internally reserved name and cannot be used.",
    "The number value you have provided is an internally reserved value and cannot be used.",
    "The account username you provided does not belong to any account.",
    "This account does not have access to the privileges required to perform this operation.",
    "Your query has too many WHERE conditions and cannot be processed due to efficiency reasons.",
    "Your query defines too many columns, reduce the number of columns and try again.",
    "The total size of the sent data does not satisfy the minimum length required based on the connection settings. This could be because the IV wasn't included in the length with encryption enabled."
};

// A function which sends data to the socket across a TCP stream which supports
// packets being bundled together during transmission by including a header and terminator.

// Sends an empty packet with size 0 which should be treated as a keep-alive test by the client.
uint32_t ka_data = 0;
ssize_t send_ka(client_socket_data* socket_data) {
    return send(socket_data->socket_id, (const char*)&ka_data, sizeof(uint32_t), 0);
}

// May be improved to reduce send calls.
// TODO - group packets and send avoiding malloc and memcpy
void send_res(client_socket_data* socket_data, const char* data, uint32_t raw_length) {
    uint8_t* buffer;
    uint32_t length;

    // If connection is encrypted.
    if (socket_data->encryption.enabled) {
        // Calculate the encryption length.
        size_t enc_length = crypto::aes256::encode_res_length(raw_length);

        length = enc_length;

        // Create buffer.
        buffer = (uint8_t*)malloc(sizeof(uint32_t) + enc_length + 1);

        // Copy over length header.
        *(uint32_t*)buffer = enc_length + 1;

        // Encrypt the data.
        crypto::aes256::encrypt_buffer(
            socket_data->encryption.aes_ctx,
            socket_data->encryption.aes_secret,
            data, raw_length, (char*)(buffer + sizeof(uint32_t))
        );
    } else {
        length = raw_length;

        // Create buffer.
        buffer = (uint8_t*)malloc(sizeof(uint32_t) + length + 1);

        // Copy over length header.
        *(uint32_t*)buffer = length + 1;
        
        // Copy over buffer.
        memcpy(buffer + sizeof(uint32_t), data, length);
    }

    // Add terminator at the end.
    buffer[length + sizeof(uint32_t)] = 0;

    // Send buffer.
    send(socket_data->socket_id, buffer, sizeof(uint32_t) + length + 1, 0);

    // Free buffer.
    free(buffer);
}

// TODO - speed this up.
inline void send_json(client_socket_data* socket_data, rapidjson::Document& data) {
    rapidjson::StringBuffer sb;
    rapidjson::Writer<rapidjson::StringBuffer> writer(sb);
    data.Accept(writer);
    send_res(socket_data, sb.GetString(), sb.GetSize());
}

// Version of above for handshake stage.
inline void send_json_handshake(client_socket_data* socket_data, rapidjson::Document& data) {
    rapidjson::StringBuffer sb;
    rapidjson::Writer<rapidjson::StringBuffer> writer(sb);
    data.Accept(writer);
    send(socket_data->socket_id, sb.GetString(), sb.GetSize(), 0);
}

// Handle the message.
// true = disconnect the socket.
// false = continue.
bool process_message(const char* buffer, uint32_t data_size, client_socket_data* socket_data) {
    bool error_text = socket_data->config.error_text;

    simdjson::ondemand::document data = socket_data->parser.iterate(buffer, data_size, data_size + simdjson::SIMDJSON_PADDING);

    // Attempt to extract query nonce.
    size_t query_nonce;
    if (data[rj_query_keys::nonce].get(query_nonce) != 0) {
        rapidjson::Document object;
        object.SetObject();
        object.AddMember(rj_query_keys::error, 1, object.GetAllocator());
        
        rapidjson::Document data_object(&object.GetAllocator());
        data_object.SetObject();
        data_object.AddMember(rj_query_keys::error_code, QueryError::nonce_invalid, data_object.GetAllocator());
        if (error_text) data_object.AddMember(rj_query_keys::error_text, query_error_text[QueryError::nonce_invalid], data_object.GetAllocator());
        object.AddMember(rj_query_keys::data, data_object, object.GetAllocator());

        send_json(socket_data, object);

        return false;
    }

    // Catch multiple types of errors automatically without redundant checks the library does regardless.
    // Slight performance penalty on exceptions which are ok.
    try {
        process_query(socket_data, query_nonce, data);
    } catch (const simdjson::simdjson_error& e) {
        switch (e.error()) {
            case simdjson::error_code::INCORRECT_TYPE:
                send_query_error(socket_data, query_nonce, QueryError::params_invalid);
                break;
            case simdjson::error_code::MEMALLOC:
                send_query_error(socket_data, query_nonce, QueryError::insufficient_memory);
                break;
            case simdjson::error_code::NO_SUCH_FIELD:
                send_query_error(socket_data, query_nonce, QueryError::params_invalid);
            default:
                send_query_error(socket_data, query_nonce, QueryError::json_invalid);
                break;
        }
    } catch (const query_compiler::exception& e) {
        send_query_error(socket_data, query_nonce, e.error());
    } catch (const std::exception& e) {
        send_query_error(socket_data, query_nonce, QueryError::internal);
    }
    
    return false;
}

#define HANDSHAKE_BUFFER_SIZE 1000 + 1 + simdjson::SIMDJSON_PADDING

void* client_connection_handle(void* arg) {
    client_socket_data* socket_data = (client_socket_data*)arg;

    // Make socket_id local as it is often accessed and data in the stack is likely to be cached by the CPU.
    int socket_id = socket_data->socket_id;
    bool error_text = socket_data->config.error_text;

    // Allocate space for buffer.
    // TODO - reuse buffer, perhaps by malloc.
    char incoming_buffer[HANDSHAKE_BUFFER_SIZE];

    uint8_t* buffer = nullptr;
    uint8_t* buffer_ptr = nullptr;
    EVP_PKEY* dh = nullptr;
    uint32_t remaining_size;

    int incoming_bytes;

    // Wait for the configuration handshake.
    incoming_bytes = recv(socket_id, &incoming_buffer, 1000, 0);

    if (incoming_bytes == -1) {
        logerr("Socket with handle %d has been terminated due to an error during handshake", socket_id);
        
        rapidjson::Document object;
        object.SetObject();
        object.AddMember(rj_query_keys::error, 1, object.GetAllocator());
        
        rapidjson::Document data_object(&object.GetAllocator());
        data_object.SetObject();
        data_object.AddMember(rj_query_keys::error_code, QueryError::handshake_config_json_invalid, object.GetAllocator());
        if (error_text) data_object.AddMember(rj_query_keys::error_text, query_error_text[QueryError::handshake_config_json_invalid], object.GetAllocator());
        object.AddMember(rj_query_keys::data, data_object, object.GetAllocator());

        send_json_handshake(socket_data, object);

        goto break_socket;
    }

    // If data is empty, treat as voluntary close request.
    if (incoming_bytes == 0) {
        log("Received terminate signal from socket handle %d during handshake - closing connection", socket_data->socket_id);
        goto break_socket;
    }

    // Add terminator (if none).
    incoming_buffer[incoming_bytes] = 0;

    try {
        auto data = socket_data->parser.iterate(incoming_buffer, incoming_bytes, HANDSHAKE_BUFFER_SIZE);
        
        // Check the versions.
        simdjson::ondemand::object version_object = data["version"];
        int64_t version_major = version_object["major"];
        int64_t version_minor = version_object["minor"];

        if (version_major > DB_MAJOR_VERSION) {
            logerr("Socket with handle %d has been terminated due to having an unsupported version.", socket_id);

            rapidjson::Document object;
            object.SetObject();
            object.AddMember(rj_query_keys::error, 1, object.GetAllocator());
            
            rapidjson::Document data_object(&object.GetAllocator());
            data_object.SetObject();
            data_object.AddMember(rj_query_keys::error_code, QueryError::outdated_server_version, object.GetAllocator());
            if (error_text) data_object.AddMember(rj_query_keys::error_text, query_error_text[QueryError::outdated_server_version], object.GetAllocator());
            object.AddMember(rj_query_keys::data, data_object, object.GetAllocator());

            send_json_handshake(socket_data, object);

            goto break_socket;
        } else if (version_major < DB_MAJOR_VERSION) {
            logerr("Socket with handle %d has been terminated due to having an unsupported version.", socket_id);

            rapidjson::Document object;
            object.SetObject();
            object.AddMember(rj_query_keys::error, 1, object.GetAllocator());
            
            rapidjson::Document data_object(&object.GetAllocator());
            data_object.SetObject();
            data_object.AddMember(rj_query_keys::error_code, QueryError::outdated_client_version, object.GetAllocator());
            if (error_text) data_object.AddMember(rj_query_keys::error_text, query_error_text[QueryError::outdated_client_version], object.GetAllocator());
            object.AddMember(rj_query_keys::data, data_object, object.GetAllocator());

            send_json_handshake(socket_data, object);
            
            goto break_socket;
        }
        
        rapidjson::Document handshake_object;
        handshake_object.SetObject();

        simdjson::ondemand::object cipher_object;
        if (data["cipher"].get(cipher_object) == 0) {
            std::string_view type = cipher_object["algorithm"];

            if (type == "diffie-hellman-aes256-cbc") {
                // Create a key exchange session.
                socket_data->encryption.enabled = true;
                dh = crypto::dh::create_session();

                rapidjson::Document cipher_object(&handshake_object.GetAllocator());
                cipher_object.SetObject();
                cipher_object.AddMember("public_key", crypto::dh::export_public_key(dh), handshake_object.GetAllocator());
                cipher_object.AddMember("prime", crypto::dh::export_prime(dh), handshake_object.GetAllocator());
                cipher_object.AddMember("generator", 2, handshake_object.GetAllocator());

                handshake_object.AddMember("cipher", cipher_object, handshake_object.GetAllocator());
            } else throw std::exception();
            
        } else if (server_config::force_encrypted_traffic) {
            logerr("Socket with handle %d has been terminated due to not being encrypted despite server requiring it", socket_id);

            rapidjson::Document object;
            object.SetObject();
            object.AddMember(rj_query_keys::error, 1, object.GetAllocator());

            rapidjson::Document data_object(&object.GetAllocator());
            data_object.SetObject();
            data_object.AddMember(rj_query_keys::error_code, QueryError::traffic_encryption_mandatory, object.GetAllocator());
            if (error_text) data_object.AddMember(rj_query_keys::error_text, query_error_text[QueryError::traffic_encryption_mandatory], object.GetAllocator());
            object.AddMember(rj_query_keys::data, data_object, object.GetAllocator());

            send_json_handshake(socket_data, handshake_object);
            
            goto break_socket;
        }
        
        simdjson::ondemand::object options_object;
        if (data["options"].get(options_object) == 0) {
            bool error_text_setting;
            if (options_object["error_text"].get(error_text_setting) == 0) {
                socket_data->config.error_text = error_text_setting;
                error_text = error_text_setting;
            }
        }

        socket_data->version.major = version_major;
        socket_data->version.minor = version_minor;
    
        // Send back handshake success.
        rapidjson::Document version_server_object(&handshake_object.GetAllocator());
        version_server_object.SetObject();
        version_server_object.AddMember("major", DB_MAJOR_VERSION, handshake_object.GetAllocator());
        version_server_object.AddMember("minor", DB_MINOR_VERSION, handshake_object.GetAllocator());
        handshake_object.AddMember("version", version_server_object, handshake_object.GetAllocator());

        send_json_handshake(socket_data, handshake_object);

        // If cipher is enabled, wait for a follow up message.
        if (socket_data->encryption.enabled) {
            incoming_bytes = recv(socket_id, &incoming_buffer, 1000, 0);
            if (incoming_bytes == -1) throw std::exception();

            // If data is empty, treat as voluntary close request.
            if (incoming_bytes == 0) {
                log("Received terminate signal from socket handle %d during handshake - closing connection", socket_data->socket_id);
                goto break_socket;
            }

            // Add terminator (if none).
            incoming_buffer[incoming_bytes] = 0;

            // Parse the data.
            data = socket_data->parser.iterate(incoming_buffer, incoming_bytes, HANDSHAKE_BUFFER_SIZE);

            std::string_view public_key = data["public_key"];

            // Compute the secret.
            std::unique_ptr<uint8_t> raw_secret = crypto::dh::compute_secret(dh, public_key);

            // Hash the secret with SHA256 to truncate the DH output for use with AES256.
            crypto::hash::sha256((const char*)raw_secret.get(), MAX_DH_KEY_SIZE, socket_data->encryption.aes_secret);

            // Create new cipher.
            socket_data->encryption.aes_ctx = EVP_CIPHER_CTX_new();

            EVP_PKEY_free(dh);
            dh = nullptr;

            rapidjson::Document res_object;
            res_object.SetObject();

            send_json_handshake(socket_data, res_object);
        }

        // Authentication/extended handshake stage.
        // This stage is different since the payload is structured as typical post-auth messages, and is also encrypted (if applicable).

        incoming_bytes = recv(socket_id, &incoming_buffer, 1000, 0);
        if (incoming_bytes == -1) throw std::exception();

        // If data is empty, treat as voluntary close request.
        if (incoming_bytes == 0) {
            log("Received terminate signal from socket handle %d during handshake - closing connection", socket_data->socket_id);
            goto break_socket;
        }

        char* auth_handshake_buffer;
        size_t auth_handshake_size;
        uint32_t specified_length = *(uint32_t*)incoming_buffer;

        // Check for correct specified length and for expected terminator at end of message.
        if (specified_length + 4 != (uint32_t)incoming_bytes) throw std::exception();
        if (incoming_buffer[incoming_bytes - 1] != 0) throw std::exception();

        // Either decrypt buffer first or directly set the message pointer.
        if (socket_data->encryption.enabled) {
            if (specified_length < AES_IV_SIZE) throw std::exception();

            auth_handshake_buffer = (char*)malloc(specified_length + simdjson::SIMDJSON_PADDING);
            size_t decrypted_size = crypto::aes256::decrypt_buffer(
                socket_data->encryption.aes_ctx,
                socket_data->encryption.aes_secret,
                incoming_buffer + 4,
                specified_length - 1,
                auth_handshake_buffer
            );

            auth_handshake_buffer[decrypted_size] = 0;
            auth_handshake_size = decrypted_size;
        } else {
            auth_handshake_size = specified_length - 1;
            auth_handshake_buffer = incoming_buffer + 4;
        }

        try {
            // Parse the data.
            data = socket_data->parser.iterate(auth_handshake_buffer, auth_handshake_size, HANDSHAKE_BUFFER_SIZE - 4);

            simdjson::ondemand::object auth_object = data["auth"];

            // Extended handshake is decrypted and starts here.

            std::string_view username = auth_object["username"];
            std::string_view password = auth_object["password"];

            // Find the user account.
            auto account_lookup = database_accounts.find(username);
            if (account_lookup == database_accounts.end()) {
                logerr("Socket with handle %d has been terminated due to providing an invalid username.", socket_id);
                
                rapidjson::Document object;
                object.SetObject();
                object.AddMember(rj_query_keys::error, 1, object.GetAllocator());
                
                rapidjson::Document data_object(&object.GetAllocator());
                data_object.SetObject();
                data_object.AddMember(rj_query_keys::error_code, QueryError::invalid_account_credentials, object.GetAllocator());
                if (error_text) data_object.AddMember(rj_query_keys::error_text, query_error_text[QueryError::invalid_account_credentials], object.GetAllocator());
                object.AddMember(rj_query_keys::data, data_object, object.GetAllocator());

                // Send a regular message here as it is the extended handshake.
                send_json(socket_data, object);
                
                if (socket_data->encryption.enabled) free(auth_handshake_buffer);
                goto break_socket;
            }

            // Get the account.
            DatabaseAccount* account = account_lookup->second;

            // Check if the provided password matches the account password hash.
            if (!crypto::password::equal(password, &account->password)) {
                logerr("Socket with handle %d has been terminated due to providing an invalid password.", socket_id);
                
                rapidjson::Document object;
                object.SetObject();
                object.AddMember(rj_query_keys::error, 1, object.GetAllocator());
                
                rapidjson::Document data_object(&object.GetAllocator());
                data_object.SetObject();
                data_object.AddMember(rj_query_keys::error_code, QueryError::invalid_account_credentials, object.GetAllocator());
                if (error_text) data_object.AddMember(rj_query_keys::error_text, query_error_text[QueryError::invalid_account_credentials], object.GetAllocator());
                object.AddMember(rj_query_keys::data, data_object, object.GetAllocator());

                send_json(socket_data, object);
                
                if (socket_data->encryption.enabled) free(auth_handshake_buffer);
                goto break_socket;
            }

            // Authentication has passed at this point.
            // Add the account to the socket.
            socket_data->account = account;

            // Send a successful handshake response.
            rapidjson::Document res_object;
            res_object.SetObject();
            send_json(socket_data, res_object);

            log("Socket with handle %d and username '%s' performed a successful handshake with client version %d.%d", socket_id, account->username, socket_data->version.major, socket_data->version.minor);
        } catch (...) {
            if (socket_data->encryption.enabled) free(auth_handshake_buffer);
            throw;
        }

        if (socket_data->encryption.enabled) free(auth_handshake_buffer);
    } catch(std::exception& e) {
        logerr("Socket with handle %d has been terminated due to an invalid handshake", socket_id);
        
        if (dh != nullptr) EVP_PKEY_free(dh);
        
        rapidjson::Document object;
        object.SetObject();
        object.AddMember(rj_query_keys::error, 1, object.GetAllocator());

        // Send handshake failure.
        rapidjson::Document data_object(&object.GetAllocator());
        data_object.SetObject();
        data_object.AddMember(rj_query_keys::error_code, QueryError::handshake_config_json_invalid, object.GetAllocator());
        if (error_text) data_object.AddMember(rj_query_keys::error_text, query_error_text[QueryError::handshake_config_json_invalid], object.GetAllocator());
        object.AddMember(rj_query_keys::data, data_object, object.GetAllocator());

        send_json_handshake(socket_data, object);

        goto break_socket;
    }

    while (1) {
        // Grab the size header from the packet.
        incoming_bytes = recv(socket_id, &remaining_size, sizeof(uint32_t), 0);

        // Perform checks (if errored/disconnected).
        if (incoming_bytes == -1) {
            logerr("Socket with handle %d has been terminated due to an error during transmission", socket_id);
            goto break_socket;
        } else if (incoming_bytes == 0) {
            log("Received terminate signal from socket handle %d - closing connection", socket_data->socket_id);
            goto break_socket;
        }

        // Update last packet time.
        socket_data->last_packet_time = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch()).count();

        // If remaining size is 0, ignore as it is a keep-alive test response.
        if (remaining_size == 0) continue;

        // If encryption is enabled, check if we have enough space for at least an IV.
        if (socket_data->encryption.enabled && remaining_size < AES_IV_SIZE) {
            logerr("Socket with handle %d has been terminated due to the encrypted packet not containing at least the IV amount of bytes", socket_id);

            rapidjson::Document object;
            object.SetObject();
            object.AddMember(rj_query_keys::error, 1, object.GetAllocator());
            
            rapidjson::Document data_object(&object.GetAllocator());
            data_object.SetObject();
            data_object.AddMember(rj_query_keys::error_code, QueryError::packet_size_exceeded, object.GetAllocator());
            if (error_text) data_object.AddMember(rj_query_keys::data, query_error_text[QueryError::packet_size_exceeded], object.GetAllocator());
            object.AddMember(rj_query_keys::data, data_object, object.GetAllocator());

            send_json(socket_data, object);

            goto break_socket;
        }

        // Check if the packet is too large.
        if (remaining_size > MAX_PACKET_SIZE) {
            logerr("Socket with handle %d has been terminated due to packet exceeding max size", socket_id);

            rapidjson::Document object;
            object.SetObject();
            object.AddMember(rj_query_keys::error, 1, object.GetAllocator());
            
            rapidjson::Document data_object(&object.GetAllocator());
            data_object.SetObject();
            data_object.AddMember(rj_query_keys::error_code, QueryError::packet_size_exceeded, object.GetAllocator());
            if (error_text) data_object.AddMember(rj_query_keys::data, query_error_text[QueryError::packet_size_exceeded], object.GetAllocator());
            object.AddMember(rj_query_keys::data, data_object, object.GetAllocator());

            send_json(socket_data, object);

            goto break_socket;
        }

        // Allocate space for the packet (+ simdjson padding).
        buffer = (uint8_t*)malloc(remaining_size + simdjson::SIMDJSON_PADDING);
        size_t size = remaining_size;
        uint32_t actual_data_size = remaining_size - 1;

        // Check if the buffer has been allocated.
        if (buffer == nullptr) {
            rapidjson::Document object;
            object.SetObject();
            object.AddMember(rj_query_keys::error, 1, object.GetAllocator());
            
            rapidjson::Document data_object(&object.GetAllocator());
            data_object.SetObject();
            data_object.AddMember(rj_query_keys::error_code, QueryError::insufficient_memory, object.GetAllocator());
            if (error_text) data_object.AddMember(rj_query_keys::error_text, query_error_text[QueryError::insufficient_memory], object.GetAllocator());
            object.AddMember(rj_query_keys::data, data_object, object.GetAllocator());
            
            send_json(socket_data, object);

            goto break_socket;
        }

        buffer_ptr = buffer;

        // Keep receiving the data until the end.
        while (remaining_size != 0) {
            // Receive remaining bytes.
            incoming_bytes = recv(socket_id, buffer_ptr, remaining_size, 0);

            // Perform checks (if errored/disconnected).
            if (incoming_bytes == -1) {
                logerr("Socket with handle %d has been terminated due to an error during transmission", socket_id);
                goto break_socket;
            } else if (incoming_bytes == 0) {
                log("Received terminate signal from socket handle %d - closing connection", socket_data->socket_id);
                goto break_socket;
            }

            // Subtract remaining bytes from incoming bytes and move the buffer pointer.
            remaining_size -= incoming_bytes;
            buffer_ptr += incoming_bytes;
        }

        // Whole packet has been received at this point.

        // Check for terminator at the end.
        if (*(buffer_ptr - 1) != 0) {
            logerr("Buffer overrun protection triggered from socket handle %d", socket_id);
            rapidjson::Document object;
            object.SetObject();
            object.AddMember(rj_query_keys::error, 1, object.GetAllocator());
            
            rapidjson::Document data_object(&object.GetAllocator());
            data_object.SetObject();
            data_object.AddMember(rj_query_keys::error_code, QueryError::overflow_protection_triggered, object.GetAllocator());
            if (error_text) data_object.AddMember(rj_query_keys::error_text, query_error_text[QueryError::overflow_protection_triggered], object.GetAllocator());
            object.AddMember(rj_query_keys::data, data_object, object.GetAllocator());

            send_json(socket_data, object);

            goto break_socket;
        }

        char* output_buffer;

        if (socket_data->encryption.enabled) {
            output_buffer = (char*)malloc(size);

            // Decrypt the buffer.
            size_t decrypt_size = crypto::aes256::decrypt_buffer(
                socket_data->encryption.aes_ctx,
                socket_data->encryption.aes_secret,
                (char*)buffer,
                size - 1,
                output_buffer
            );

            actual_data_size = decrypt_size;
            output_buffer[decrypt_size] = 0;
            free(buffer);
        } else output_buffer = (char*)buffer;

        // Data can now be processed. Pass it to the relevant handlers.
        bool msg_handle = process_message(output_buffer, actual_data_size, socket_data);
        if (msg_handle == true) goto break_socket;

        // The buffer is no longer needed.
        free(output_buffer);
        buffer = nullptr;
    }

    break_socket:

    // Deallocate buffer (if any).
    if (buffer != nullptr) free(buffer);

    // Terminate the socket.
    terminate_socket(socket_data->socket_id);

    return 0;
}