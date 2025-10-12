/*
A simple buffer which reimplements some of the features of an std::vector, but with performance and flexibility in mind.
How does this work? Is this just another pointless reimplementation of an already existing feature (std::vector)?
    Yes, sort of. Many of the structures here are because the standard library doesn't support performance-prioritising use cases, and as such often don't include a single feature/trait that I need that is a deal breaker.
    std::vector doesn't support, safely and in a defined manner, the ability to define members/raw buffers and leave them in an uninitialized state.
    Yes, we could in theory define a class which implements an empty constructor with a single byte which would make the vector think it's initializing the members when it actually isn't, but it doesn't feel right bypassing vector's intended functionality.
    This is a problem because in our use case, we just want to use the vector as a simple expandable buffer, we don't care about the fancy RAII auto member initialization stuff.
    This leads to our second problem, vector in C++ copies the old contents of its internal buffer into its new one when expanding, this is very helpful when using vector for its intended purpose, but not for us :(.
    When we want to expand the buffer (such as when about to copy over an object larger than the capacity), we don't care about the old buffer's contents anymore and don't want to waste time copying over meaningless bytes on each resize.
    Another issue is we have almost no flexibility when using C++'s vector, here we can tune it to work exactly how we want.
Why not just use new[] or malloc for our bytes?
    Why not just use raw pointers everywhere instead of an std::unique_ptr? Same logic goes here, we want a self-owning instance which manages and reuses the buffer, without leaving the caller to deal with memory management.
    The goal is to also provide a simple re-use functionality to reduce malloc calls, we start off with an initial capacity, grow the buffer to that size, if anything smaller is stored, that's great no malloc needed.
    If anything larger comes in, no problem, we can just discard the old buffer and create a new one in its place with the capacity of the new string
*/

#pragma once

#include "simple_string.h"
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <exception>
#include <string_view>

namespace speedystd {
    class simple_buffer {
        public:
            simple_buffer() {}
            simple_buffer(uint32_t initial_capacity) : capacity(initial_capacity) {
                buffer = malloc(initial_capacity);
            }

            // Prepares the instance to receive a buffer of {size} bytes.
            // Returns a copy of the buffer, which can also be acquired via .data().
            void* expect(uint32_t size) {
                this->_size = size;

                // If we have enough capacity, just use that.
                if (this->capacity >= size) return this->buffer;

                // Free the old buffer and allocate a new one.
                free(this->buffer);
                this->buffer = malloc(size);
                
                // Return the new buffer.
                return this->buffer;
            }

            inline void* data() {
                #if !defined(__OPTIMIZE__)
                    if (this->buffer == nullptr) {
                        puts("Debug build check: calling code tried to get buffer from uninitialized simple_buffer");
                        std::terminate();
                    }
                #endif

                return buffer;
            }

            inline uint32_t size() {
                return _size;
            }

            // Common accessors.

            inline std::string_view as_string_view() {
                #if !defined(__OPTIMIZE__)
                    if (this->buffer == nullptr) {
                        puts("Debug build check: calling code tried to get string_view buffer reference from uninitialized simple_buffer");
                        std::terminate();
                    }
                #endif

                return std::string_view((char*)this->buffer, this->_size);
            }

            // Creates a copy of the buffer and loads it into the simple string.
            inline speedystd::simple_string to_simple_string() {
                return speedystd::simple_string(as_string_view());
            }
            
            // Boring C++ RAII stuff.

            inline ~simple_buffer() {
                if (buffer != nullptr) free(buffer);
            }

            simple_buffer(simple_buffer&& other) noexcept {
                this->_size = other._size;
                this->capacity = other.capacity;
                this->buffer = other.buffer;

                other.buffer = nullptr;
                other.capacity = 0;
                other._size = 0;
            }
            
            simple_buffer& operator=(simple_buffer&& other) noexcept {
                // Check for any self-assignment in debug mode.
                #if !defined(__OPTIMIZE__)
                    if (this == &other) {
                        puts("Debug build check: calling code tried self-assign std::move(simple_buffer)");
                        std::terminate();
                    }
                #endif

                // First, release any heap data we have (if any).
                this->~simple_buffer();

                // Proceed with simply moving over the other instances resources to ours.

                this->_size = other._size;
                this->capacity = other.capacity;
                this->buffer = other.buffer;

                other.buffer = nullptr;
                other.capacity = 0;
                other._size = 0;
                
                return *this;
            }

            // We don't really want accidental copies here.
            // First of all, it is ambigious whether the caller wants the entire capacity copied, or just the size.
            // Copies will instead be explicit for this reason.
            simple_buffer& operator=(const simple_buffer& other) = delete;
            simple_buffer(const simple_buffer& other) = delete;

        private:
            void* buffer = nullptr;

            uint32_t capacity = 0;
            uint32_t _size = 0;
    };
}