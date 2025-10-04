/*
A structure designed to provide the facilities of a string_view as well as an owned buffer.
The catch is, it allows you to instantiate the string with raw allocated memory unlike the standards std::string.
This is helpful when the string will always be filled with data, and it is wasteful to first zero the buffer.
This version also includes a slightly bigger small-string-optimization buffer than usual (20 bytes! 12 on 32-bit) as we are able to save 4 bytes due to the size being a uint32_t, as well as no terminator.
It is also smaller than the standard library string (24 bytes, only matches the 32 bytes in debug mode with the additional debug flag!), despite also having a bigger SSO buffer.
We could boil it down even further down to just 16 bytes by removing 8 bytes from the SSO buffer, but we risk more heap allocations for small strings
    at a negligable performance gain over copying 8 less bytes, and preventing even a single extra malloc will more than pay that cost of copying an extra 8 bytes back
    (even despite this, still more efficient than std::string).

ONLY CAVEAT! There is no pointer to store if the heap or local buffer is used, hence on each dereference into a string_view
    it does an LE comparison on the size, this compiles down to a conditional move on the assembly level which does not risk branch misprediction
    so it is fine to do even in tight loops, but in the best case scenario the string is cached/only dereferenced once.
*/

#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <exception>
#include <string_view>

namespace speedystd {
    class alignas(sizeof(char*)) simple_string {
        public:
            inline simple_string() {};

            // Initializes the internal buffer or allocates space for the string on the heap, then returns a pointer to it.
            char* init(uint32_t size) {
                #if !defined(__OPTIMIZE__)
                    if (has_already_init) {
                        puts("Debug build check: calling code tried to initialize simple_string twice!");
                        std::terminate();
                    }

                    has_already_init = true;
                #endif

                this->size = size;

                if (can_use_local_buffer(size)) {
                    return ptr_or_sso.local_buffer;
                } else {
                    ptr_or_sso.heap_buffer = (char*)malloc(size);
                    return ptr_or_sso.heap_buffer;
                }
            }

            // Allows interpreting the object as a string_view.
            inline std::string_view as_string_view() const {
                #if !defined(__OPTIMIZE__)
                    if (!has_already_init) {
                        puts("Debug build check: calling code tried to derive a string_view from an uninitialized simple_string");
                        std::terminate();
                    }
                #endif 

                return std::string_view(can_use_local_buffer(size) ? ptr_or_sso.local_buffer : ptr_or_sso.heap_buffer, size);
            }

            inline explicit operator std::string_view() const { return as_string_view(); }

            // Allows using common string_view operators on the class.

            friend bool operator==(const simple_string& lhs, const simple_string& rhs) noexcept {
                return lhs.as_string_view() == rhs.as_string_view();
            }

            friend bool operator!=(const simple_string& lhs, const simple_string& rhs) noexcept {
                return !(lhs == rhs);
            }

            friend bool operator==(const simple_string& lhs, std::string_view rhs) noexcept {
               return lhs.as_string_view() == rhs;
            }

            friend bool operator==(std::string_view rhs, const simple_string& lhs) noexcept {
               return lhs.as_string_view() == rhs;
            }

            friend bool operator!=(const simple_string& lhs, std::string_view rhs) noexcept {
                return !(lhs == rhs);
            }

            friend bool operator!=(std::string_view rhs, const simple_string& lhs) noexcept {
                return !(lhs == rhs);
            }

            inline ~simple_string() {
                if (!can_use_local_buffer(size)) {
                    free(ptr_or_sso.heap_buffer);
                }
            }

            simple_string(simple_string&& other) noexcept {
                #if !defined(__OPTIMIZE__)
                    this->has_already_init = other.has_already_init;
                    other.has_already_init = false;
                #endif

                this->size = other.size;
                other.size = 0; // prevents a double free if other isn't init'd again and goes out of scope.

                this->ptr_or_sso.heap_buffer = other.ptr_or_sso.heap_buffer;
            }
            
            simple_string& operator=(simple_string&& other) noexcept {
                // Check for any self-assignment in debug mode.
                #if !defined(__OPTIMIZE__)
                    if (this == &other) {
                        puts("Debug build check: calling code tried self-assign std::move(simple_string)");
                        std::terminate();
                    }

                    this->has_already_init = other.has_already_init;
                    other.has_already_init = false;
                #endif

                // First, release any heap data we have (if any).
                this->~simple_string();

                // Proceed with simply moving over the other instances resources to ours.
                this->ptr_or_sso = other.ptr_or_sso;
                this->size = other.size;
                other.size = 0; // prevents a double free if other isn't init'd again and goes out of scope.
                
                return *this;
            }

            simple_string& operator=(const simple_string& other) {
                #if !defined(__OPTIMIZE__)
                    if (this == &other) {
                        puts("Debug build check: calling code tried self-assign std::copy(simple_string)");
                        std::terminate();
                    }

                    this->has_already_init = other.has_already_init;
                #endif

                this->size = other.size;
                
                // Copy just copy the local buffer, or the heap memory if required.
                if (can_use_local_buffer(size)) {
                    this->ptr_or_sso = other.ptr_or_sso;
                } else {
                    this->ptr_or_sso.heap_buffer = (char*)malloc(size);
                    memcpy(this->ptr_or_sso.heap_buffer, other.ptr_or_sso.heap_buffer, size);
                }

                return *this;
            }

            inline simple_string(const simple_string& other) {
                this->operator=(other);
            }

            inline static constexpr bool can_use_local_buffer(uint32_t size) {
                return size <= sizeof(ptr_or_sso);
            }

        private:
            // Compiler here over-cautiously adds in padding, but we're already aligned because of the size after it.
            // Hence why packed is needed.
            union {
                char* heap_buffer;
                char local_buffer[(sizeof(heap_buffer) * 2) + sizeof(uint32_t)];
            } __attribute__((packed)) ptr_or_sso;

            uint32_t size = 0;

            // A safety flag and function only present if optimizations are disabled.
            #if !defined(__OPTIMIZE__)
                bool has_already_init = false;
            #endif
    };
}