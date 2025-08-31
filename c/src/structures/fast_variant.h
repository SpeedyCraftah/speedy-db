#pragma once

#include <algorithm>
#include <cstddef>
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

template <typename... Types>
class fast_variant {
  public:
    fast_variant() {}

    template <typename T>
    inline void set_as() {
      #if !defined(__OPTIMIZE__)
        if (debug_instantiated) {
          throw std::runtime_error("Debug build check: calling code tried to set the variant type twice!");
        }

        debug_instantiated = true;
      #endif

      new (buffer) T();
      selected_type = get_selector_for_type<T>();
    }

    template <typename T>
    inline T& get() {
      #if !defined(__OPTIMIZE__)
        if (selected_type != get_selector_for_type<T>()) {
          throw std::runtime_error("Debug build check: calling code tried to get variant not in use!");
        }
      #endif

      return *reinterpret_cast<T*>(buffer);
    }

    ~fast_variant() {
      if (selected_type == 0) return;
      destroy_object_impl<Types...>();
    }

  private:
    static constexpr size_t size = std::max({sizeof(Types)...});
    uint selected_type = 0;
    char buffer[size];

    #if !defined(__OPTIMIZE__)
      bool debug_instantiated = false;
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
      
      else {
        throw std::runtime_error("Possibly corrupt selected_type");
      }
    }
};