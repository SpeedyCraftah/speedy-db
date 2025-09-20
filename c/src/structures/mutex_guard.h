#pragma once

#include <mutex>
#include <utility>
#include <shared_mutex>

/*
A mutex implementation inspired by the Rust standard library, which allows locking objects behind a mutex, meaning you HAVE to acquire a mutex before you can use the data.
This avoids cases where you accidentally forget to acquire a mutex for a resource and do potentially dangerous reads & writes.
This is a zero-cost abstraction as the compiler will optimize this structure out when compiled with any optimization setting.
*/

namespace speedystd {
    template <typename T>
    class guard_mutex {
        public:
            class guard {
                public:
                    explicit guard(guard_mutex& m) : parent(m) {}
                    ~guard() { parent.mutex.unlock(); }

                    T* operator->() { return &parent.value; }
                    T& operator*() { return parent.value; }

                private:
                    guard_mutex& parent;
            };

            guard_mutex(T init_value) {
                value = std::move(init_value);
            }

            guard lock() {
                mutex.lock();
                return guard(*this);
            }

            T& get_unsafe() {
                return value;
            }

            #if !defined(__OPTIMIZE__)
                ~guard_mutex() {
                    if (!mutex.try_lock()) {
                        puts("Dangerous use of guard_mutex - guard mutex was locked while trying to destruct parent!");
                        std::terminate();
                    } else mutex.unlock();
                }
            #endif

        private:
            T value;
            std::mutex mutex;
    };

    template <typename T>
    class guard_rw_mutex {
        public:
            class read_guard {
                public:
                    explicit read_guard(guard_rw_mutex& m) : parent(m) {}
                    ~read_guard() { parent.mutex.unlock_shared(); }

                    T* operator->() { return &parent.value; }
                    T& operator*() { return parent.value; }

                private:
                    guard_rw_mutex& parent;
            };

            class write_guard {
                public:
                    explicit write_guard(guard_rw_mutex& m) : parent(m) {}
                    ~write_guard() { parent.mutex.unlock(); }

                    T* operator->() { return &parent.value; }
                    T& operator*() { return parent.value; }

                private:
                    guard_rw_mutex& parent;
            };

            guard_rw_mutex(T init_value) {
                value = std::move(init_value);
            }

            read_guard read() {
                mutex.lock_shared();
                return read_guard(*this);
            }

            write_guard write() {
                mutex.lock();
                return write_guard(*this);
            }

            T& get_unsafe() {
                return value;
            }

            #if !defined(__OPTIMIZE__)
                ~guard_rw_mutex() {
                    if (!mutex.try_lock()) {
                        puts("Dangerous use of guard_mutex - guard mutex was locked while trying to destruct parent!");
                        std::terminate();
                    } else mutex.unlock();
                }
            #endif

        private:
            T value;
            std::shared_mutex mutex;
    };
}