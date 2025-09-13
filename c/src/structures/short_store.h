/*
A structure optimized for single-element access, with occasional but rare multiple elements.
Optimization stems from a branch to see if there is only one entry, or if it should iterate over multiple in the heap.
Insertions are extremely slow past a certain point, and so should only be used in cases where 99% of instances will only contain one element.
*/

#pragma once

#include <cstdint>
#include <memory>
#include <stdexcept>
#include <vector>

namespace speedystd {
    template <typename T>
    class short_store {
    public:
        short_store(T initial) {
            local_store = initial;
            _is_single = true;
        }

        inline bool is_single() const {
            return _is_single;
        }

        inline const T& get_single() const {
            return local_store;
        }

        inline auto begin() const {
            #if !defined(__OPTIMIZE__)
                if (_is_single) {
                    throw std::runtime_error("Debug build check: calling code tried to run iterator in single mode!");
                }
            #endif
            
            return storage->cbegin();
        }
        
        inline auto end() const { return storage->cend(); }

        void add(T value) {
            if (_is_single) {
                storage = std::unique_ptr<std::vector<T>>(new std::vector<T>());
                storage->reserve(2);
                storage->push_back(local_store);
                storage->push_back(value);
                
                _is_single = false;
            } else {
                storage->push_back(value);
            }
        }

    private:
        bool _is_single;
        T local_store;
        std::unique_ptr<std::vector<T>> storage;
    };
}