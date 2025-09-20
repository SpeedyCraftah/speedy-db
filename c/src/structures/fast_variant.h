#pragma once

#include <algorithm>
#include <cstddef>
#include <cstdio>
#include <exception>
#include <stdexcept>
#include <iostream>
#include <type_traits>
#include <tuple>

/*
A custom written version of std::variant fit specifically to the requirements of SpeedyDB (one-time type instantiation, no safety check when reading, RAII, etc - essentially a union on steroids!).
std::variant unfortunately does not provide a way to bypass the type safety check which happens on each access of the variant, and so this was a necessity.
Unions do not handle memory management such as constructors and destructors automatically and require lots of boilerplate and duplication, and so it was ruled out.
This version contains debug checks for undefined behaviour and performs a safety check on each access, which are eliminated once the code is compiled with optimizations.
*/

namespace speedystd {
  template <typename... Types>
  class fast_variant {
    public:
      fast_variant() {
        #if !defined(__OPTIMIZE__)
            debug_view = std::tuple<Types*...>{reinterpret_cast<Types*>(buffer)...};
        #endif
      }
  
      template <typename T>
      inline T& set_as() {
        #if !defined(__OPTIMIZE__)
          if (selected_type != 0) {
            puts("Debug build check: calling code tried to set the variant type twice!");
            std::terminate();
          }
        #endif
  
        new (buffer) T();
        selected_type = get_selector_for_type<T>();

        return as<T>();
      }
  
      template <typename T>
      inline T& as() {
        #if !defined(__OPTIMIZE__)
          if (selected_type != get_selector_for_type<T>()) {
            puts("Debug build check: calling code tried to get variant not in use!");
            std::terminate();
          }
        #endif
  
        return *reinterpret_cast<T*>(buffer);
      }
  
      ~fast_variant() {
        if (selected_type != 0) destroy_object_impl<Types...>();
      }

      fast_variant(fast_variant&& other) noexcept {
        this->selected_type = other.selected_type;
        if (selected_type != 0) move_object_impl<Types...>(std::move(other));

        other.selected_type = 0;
      }
      
      fast_variant& operator=(fast_variant&& other) noexcept {
        this->selected_type = other.selected_type;
        if (selected_type != 0) move_object_impl<Types...>(std::move(other));

        other.selected_type = 0;
        
        return *this;
      }

      fast_variant& operator=(const fast_variant&) = delete;
      fast_variant(const fast_variant& other) = delete;

    private:
      static constexpr size_t _size = std::max({sizeof(Types)...});
      static constexpr size_t _align = std::max({alignof(Types)...});

      uint selected_type = 0;
      alignas(_align) char buffer[_size];

      // A view specifically for debuggers which allows visibility into the buffer as a particular type.
      #if !defined(__OPTIMIZE__)
        std::tuple<Types*...> debug_view;
      #endif

      template <typename T>
      constexpr inline uint get_selector_for_type() {
        return get_selector_for_type_impl<T, Types...>();
      }
  
      template <typename T, typename First, typename... Rest>
      constexpr inline uint get_selector_for_type_impl() {
        if constexpr (std::is_same_v<T, First>) {
          return sizeof...(Types) - sizeof...(Rest);
        } 
        
        else if constexpr (sizeof...(Rest) > 0) {
          return get_selector_for_type_impl<T, Rest...>();
        }
        
        else {
          static_assert(!std::is_same_v<T, T>, "Unsupported type");
        }
      }
  
      template <typename First, typename... Rest>
      void destroy_object_impl() {
        if (selected_type == sizeof...(Types) - sizeof...(Rest)) {
          reinterpret_cast<First*>(buffer)->~First();
        }
        
        else if constexpr (sizeof...(Rest) > 0) {
          destroy_object_impl<Rest...>();
        }
      }

      template <typename First, typename... Rest>
      void move_object_impl(fast_variant&& other) {
        if (other.selected_type == sizeof...(Types) - sizeof...(Rest)) {
          new (this->buffer) First(std::move(*reinterpret_cast<First*>(&other.buffer)));
        }
        
        else if constexpr (sizeof...(Rest) > 0) {
          move_object_impl<Rest...>(std::move(other));
        }
      }
  };
}