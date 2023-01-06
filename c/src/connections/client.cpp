#include "client.h"
#include "../main.h"
#include <stdio.h>
#include <sys/socket.h>
#include <stdlib.h>
#include <netinet/in.h>
#include "../logging/logger.h"
#include <unistd.h>
#include "handler.h"
#include "../deps/json.hpp"
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

#define MAX_PACKET_SIZE 104857600

// Define the error text list.
const char* errors::text[] = {
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
    "The table you are attempting to instantiate already exists.",
    "The table you are attempting to open has already been loaded.",
    "The table you are attempting to query has not been loaded. You must load a table before you can query it.",
    "There was insufficient memory available to perform the operation you requested.",
    "The handshake has failed due to incorrect or invalid database account credentials.",
    "The simulataneous connection limit has been exhausted. Please either disconnect client, ensure clients disconnect properly or increase the connection limit with max-connections.",
    "The server requests that all clients establish an encrypted connection. Reconnect and supply a public key or adjust the server settings.",
    "The account username you provided for creation has already been taken. Please pick another account username.",
    "The name you have provided is an internally reserved name and cannot be used.",
    "The number value you have provided is an internally reserved value and cannot be used."
};

// A function which sends data to the socket across a TCP stream which supports
// packets being bundled together during transmission by including a header and terminator.

// Sends an empty packet with size 0 which should be treated as a keep-alive test by the client.
uint32_t ka_data = 0;
int send_ka(client_socket_data* socket_data) {
    return send(socket_data->socket_id, (const char*)&ka_data, sizeof(uint32_t), 0);
}

// May be improved to reduce send calls.
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
            socket_data->encryption.aes_server_iv,
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

inline void send_json(client_socket_data* socket_data, const nlohmann::json &data) {
    const std::string raw_d = data.dump(-1, ' ', true);
    send_res(socket_data, raw_d.c_str(), raw_d.length());
}

// Handle the message.
// true = disconnect the socket.
// false = continue.
bool process_message(const char* buffer, client_socket_data* socket_data) {
    int socket_id = socket_data->socket_id;
    bool short_attr = socket_data->config.short_attr;
    bool error_text = socket_data->config.error_text;

    try {
        auto data = nlohmann::json::parse(buffer);

        if (!data.contains(short_attr ? "n" : "nonce") || !data[short_attr ? "n" : "nonce"].is_number_unsigned()) {
            send_json(socket_data, { 
                { short_attr ? "e" : "error", true },
                { short_attr ? "d" : "data", {
                    { short_attr ? "c" : "code", errors::nonce_invalid },
                    { short_attr ? "t" : "text", error_text ? errors::text[errors::nonce_invalid] : "" }
                }} 
            });

            return false;
        }

        process_query(socket_data, data);
    } catch(int i) {//catch(const std::exception& e) {
        send_json(socket_data, { 
            { short_attr ? "e" : "error", true },
            { short_attr ? "d" : "data", {
                { short_attr ? "c" : "code", errors::json_invalid },
                { short_attr ? "t" : "text", error_text ? errors::text[errors::json_invalid] : "" }
            }} 
        });
    }

    return false;
}

void* client_connection_handle(void* arg) {
    client_socket_data* socket_data = (client_socket_data*)arg;

    // Make socket_id local as it is often accessed and data in the stack is likely to be cached by the CPU.
    int socket_id = socket_data->socket_id;
    bool short_attr = socket_data->config.short_attr;
    bool error_text = socket_data->config.error_text;

    // Allocate space for buffer.
    char incoming_buffer[1000 + 1];

    int incoming_bytes;

    // Wait for the configuration handshake.
    incoming_bytes = recv(socket_id, &incoming_buffer, 1000, 0);

    if (incoming_bytes == -1) {
        logerr("Socket with handle %d has been terminated due to an error during handshake", socket_id);
        send_json(socket_data, {
            { "error", true },
            { "data", {
                { "code", errors::handshake_config_json_invalid },
                { "text", errors::text[errors::handshake_config_json_invalid] }
            }}
        });

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
        auto data = nlohmann::json::parse(incoming_buffer);

        // Mandatory parameters.
        if (!data.contains("version")) throw std::exception();
        if (!data["version"].contains("major") || !data["version"]["major"].is_number_unsigned()) throw std::exception();
        if (!data["version"].contains("minor") || !data["version"]["minor"].is_number_unsigned()) throw std::exception();

        // Check the versions.
        if (data["version"]["major"] > server_config::version::major) {
            logerr("Socket with handle %d has been terminated due to having an unsupported version.", socket_id);
            
            std::string handshake_failure = nlohmann::json({
                { "error", true },
                { "data", {
                    { "code", errors::outdated_server_version },
                    { "text", errors::text[errors::outdated_server_version] }
                }}
            }).dump();

            send(socket_id, handshake_failure.c_str(), handshake_failure.length(), 0);
            
            // Block new connections for 2 seconds.
            std::this_thread::sleep_for(std::chrono::milliseconds(2000));

            goto break_socket;
        } else if (data["version"]["major"] < server_config::version::major) {
            logerr("Socket with handle %d has been terminated due to having an unsupported version.", socket_id);

            std::string handshake_failure = nlohmann::json({
                { "error", true },
                { "data", {
                    { "code", errors::outdated_client_version },
                    { "text", errors::text[errors::outdated_client_version] }
                }}
            }).dump();

            send(socket_id, handshake_failure.c_str(), handshake_failure.length(), 0);
            
            goto break_socket;
        }

        // Authentication.
        // An object as future permission changes may be made.
        // login system here

        nlohmann::json handshake_object = nlohmann::json::object_t();

        DH* dh = NULL;

        if (data.contains("cipher")) {
            if (!data["cipher"].is_object()) throw std::exception();
            nlohmann::json encryption = data["cipher"];

            if (!encryption.contains("algorithm") || !encryption["algorithm"].is_string()) throw std::exception();
            std::string type = encryption["algorithm"];

            if (type == "diffie-hellman-aes256-cbc") {
                // Create a key exchange session.
                socket_data->encryption.enabled = true;
                dh = crypto::dh::create_session();

                // Generate an initial AES IV value for both sides.
                crypto::random_bytes(socket_data->encryption.aes_server_iv, 16);
                memcpy(socket_data->encryption.aes_client_iv, socket_data->encryption.aes_server_iv, 16);

                handshake_object["cipher"] = nlohmann::json::object_t();
                handshake_object["cipher"]["public_key"] = crypto::dh::export_public_key(dh);
                handshake_object["cipher"]["prime"] = crypto::dh::export_prime(dh);
                handshake_object["cipher"]["generator"] = 2;
                handshake_object["cipher"]["initial_iv"] = base64::quick_encode(socket_data->encryption.aes_server_iv, 16); 
            } else throw std::exception();
            
        } else if (server_config::force_encrypted_traffic) {
            logerr("Socket with handle %d has been terminated due to failing authentication.", socket_id);

            std::string handshake_failure = nlohmann::json({
                { "error", true },
                { "data", {
                    { "code", errors::incorrect_password },
                    { "text", errors::text[errors::incorrect_password] }
                }}
            }).dump();

            send(socket_id, handshake_failure.c_str(), handshake_failure.length(), 0);
            
            goto break_socket;
        }
        
        if (data.contains("options")) {
            if (!data["options"].is_object()) throw std::exception();

            auto options = data["options"];
            
            if (options.contains("short_attributes")) {
                if (!options["short_attributes"].is_boolean()) throw std::exception();
                socket_data->config.short_attr = options["short_attributes"];
                short_attr = options["short_attributes"];
            }

            if (options.contains("error_text")) {
                if (!options["error_text"].is_boolean()) throw std::exception();
                socket_data->config.error_text = options["error_text"];
                error_text = options["error_text"];
            }
        }

        socket_data->version.major = data["version"]["major"];
        socket_data->version.minor = data["version"]["minor"];

        log("Socket with handle %d performed a successful handshake with client version %d.%d", socket_id, socket_data->version.major, socket_data->version.minor);
    
        // Send back handshake success.
        handshake_object["version"] = {
            { "major", server_config::version::major },
            { "minor", server_config::version::minor }
        };

        std::string handshake_success = handshake_object.dump();

        send(socket_id, handshake_success.c_str(), handshake_success.length(), 0);

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
            data = nlohmann::json::parse(incoming_buffer);
            if (!data.contains("public_key")) throw std::exception();

            std::string public_key = data["public_key"];

            // Compute the secret.
            char* raw_secret = crypto::dh::compute_secret(dh, public_key);

            // Copy only 32 bytes since that's what AES256 supports.
            memcpy(socket_data->encryption.aes_secret, raw_secret, 32);

            // Create new cipher.
            socket_data->encryption.aes_ctx = EVP_CIPHER_CTX_new();

            // Free the raw secret.
            free(raw_secret);
            OPENSSL_free(dh);

            char* dest = (char*)malloc(base64::encode_res_length(32) + 1);
            dest[base64::encode_res_length(32)] = 0;

            base64::encode(socket_data->encryption.aes_secret, 32, dest);
            log("Secret calculated as: %s", dest);

            nlohmann::json::object_t data = {};
            nlohmann::json dataObj = data;

            std::string rawData = dataObj.dump();
            send(socket_id, rawData.c_str(), rawData.length(), 0);
        }
    } catch(std::exception& e) {
        logerr("Socket with handle %d has been terminated due to an invalid handshake", socket_id);
        
        // Send handshake failure.
        std::string handshake_failure = nlohmann::json({
            { "error", true },
            { "data", {
                { "code", errors::handshake_config_json_invalid },
                { "text", errors::text[errors::handshake_config_json_invalid] }
            }}
        }).dump();

        send(socket_id, handshake_failure.c_str(), handshake_failure.length(), 0);

        goto break_socket;
    }

    uint8_t* buffer;
    uint8_t* buffer_ptr;
    uint32_t remaining_size;

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

        // Check if the packet is too large.
        if (remaining_size > MAX_PACKET_SIZE) {
            logerr("Socket with handle %d has been terminated due to packet exceeding 100M", socket_id);

            send_json(socket_data, {
                { short_attr ? "e" : "error", true },
                { short_attr ? "d" : "data", {
                    { short_attr ? "c" : "code", errors::packet_size_exceeded },
                    { short_attr ? "t" : "text", error_text ? errors::text[errors::packet_size_exceeded] : "" }
                }}
            });

            goto break_socket;
        }

        // Allocate space for the packet.
        buffer = (uint8_t*)malloc(remaining_size);
        size_t size = remaining_size;

        // Check if the buffer has been allocated.
        if (buffer == 0) {
            send_json(socket_data, {
                { short_attr ? "e" : "error", true },
                { short_attr ? "d" : "data", {
                    { short_attr ? "c" : "code", errors::insufficient_memory },
                    { short_attr ? "t" : "text", error_text ? errors::text[errors::insufficient_memory] : "" }
                }}
            });

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
            send_json(socket_data, {
                { short_attr ? "e" : "error", true },
                { short_attr ? "d" : "data", {
                    { short_attr ? "c" : "code", errors::overflow_protection_triggered },
                    { short_attr ? "t" : "text", error_text ? errors::text[errors::overflow_protection_triggered] : "" }
                }}
            });

            goto break_socket;
        }

        char* output_buffer;

        if (socket_data->encryption.enabled) {
            output_buffer = (char*)malloc(size);

            // Decrypt the buffer.
            size_t decrypt_size = crypto::aes256::decrypt_buffer(
                socket_data->encryption.aes_ctx,
                socket_data->encryption.aes_secret, 
                socket_data->encryption.aes_client_iv, 
                (char*)buffer, size - 1, output_buffer
            );

            output_buffer[decrypt_size] = 0;
            free(buffer);
        } else output_buffer = (char*)buffer;

        // Data can now be processed. Pass it to the relevant handlers.
        bool msg_handle = process_message(output_buffer, socket_data);
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