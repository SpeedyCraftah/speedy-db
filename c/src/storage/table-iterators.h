#pragma once

#include "compiled-query.h"
#include "structures/record.h"
#include "table.h"

#define BULK_HEADER_READ_COUNT 2000

namespace table_iterator {
    // A simple type that does absolutely nothing for the .end() method, for compatibility with the default iterator because technically we don't have an "end".
    class ph_end_iterator {};

    // Iterator for scanning the tables.
    // Performance when compiled with Ofast is comparable to a normal loop.
    class data_iterator {
        public:
            inline data_iterator(ActiveTable& table) : table(table) {
                #ifndef __OPTIMIZE__
                    if (table.is_iterator_running) {
                        logerr("[RUNTIME DEBUG] table '%s' iterator begin() called while another iterator is already running", table.header.name);
                        exit(1);
                    }

                    table.is_iterator_running = true;
                #endif
            }

            #ifndef __OPTIMIZE__
            ~data_iterator() { table.is_iterator_running = false; }
            #endif

            class iterator {
                public:
                    ActiveTable& table;

                    bool complete = false;
                    size_t buffer_index = BULK_HEADER_READ_COUNT;
                    size_t buffer_records_available = BULK_HEADER_READ_COUNT;
            
                    inline iterator(ActiveTable& tbl) : table(tbl) {}
            
                    // Load the next record.
                    iterator operator++() {
                        buffer_index++;
            
                        if (buffer_index >= buffer_records_available) {
                            buffer_index = 0;
                            buffer_records_available = request_bulk_records();
                        }
            
                        complete = (buffer_index >= buffer_records_available);
            
                        return *this;
                    }
            
                    // Manual version of operator++ for when records have to be modified in bulk in a tight frame.
                    // Returns number of records read.
                    inline size_t request_bulk_records() {
                        return fread_unlocked(table.header_buffer, table.header.record_size, BULK_HEADER_READ_COUNT, table.data_handle);
                    }
                    
                    // Useful method for iterators using data_iterator under the hood that need access to the raw record data to avoid risking wrapper overhead.
                    constexpr inline RecordData* get_raw_record() { return table.header_buffer + (buffer_index * table.header.record_size); }
            
                    constexpr inline Record operator*() { return Record(this->table, get_raw_record()); };
                    inline bool operator!=(const ph_end_iterator& _unused) { return !this->complete; }
            };

            inline iterator begin() {
                fseek(table.data_handle, 0, SEEK_SET);
                iterator i = iterator(table);
                i.operator++();
                
                return i;
            }

            inline ph_end_iterator end() {
                return ph_end_iterator();
            }

        private:
            ActiveTable& table;
    };

    inline data_iterator iterate_all(ActiveTable& table) {
        return data_iterator(table);
    }


    // A wrapper around data_iterator, but allows for iterating over records which match the conditions only.
    class specific_data_iterator {
        public:
            inline specific_data_iterator(ActiveTable& table, query_compiler::CompiledFindQuery* query) : table(table), query(query) {}

            class iterator {
                public:
                    query_compiler::CompiledFindQuery* query;
                    ActiveTable& table;
                    data_iterator::iterator d_iterator;
                    RecordData* current_record;
            
                    inline iterator(ActiveTable& tbl, query_compiler::CompiledFindQuery* q) : query(q), table(tbl), d_iterator(iterate_all(table).begin()), current_record(d_iterator.get_raw_record()) {}
            
                    iterator operator++() {
                        while (!this->d_iterator.complete) {
                            ++this->d_iterator;
                            RecordData* record = this->d_iterator.get_raw_record();
            
                            if (this->table.verify_record_conditions_match(record, query->conditions, query->conditions_count)) {
                                current_record = record;
                                break;
                            }
                        }
            
                        return *this;
                    }
            
                    inline Record operator*() {
                        return Record(table, this->current_record);
                    }
            
                    inline bool operator!=(const ph_end_iterator& _unused) { return !this->d_iterator.complete; }
            };

            inline iterator begin() {
                iterator i = iterator(table, query);
                if (!i.d_iterator.complete && !table.verify_record_conditions_match(i.current_record, query->conditions, query->conditions_count)) i.operator++();
                
                return i;
            }

            inline ph_end_iterator end() {
                return ph_end_iterator();
            }

        private:
            ActiveTable& table;
            query_compiler::CompiledFindQuery* query;
    };

    inline specific_data_iterator iterate_specific(ActiveTable& table, query_compiler::CompiledFindQuery* query) {
        return specific_data_iterator(table, query);
    }
    

    // Same as above but instead of iterating over individual records, only does by bulk.
    // Useful for erase/update operations.
    class bulk_data_iterator {
        public:
            struct progress {
                size_t byte_offset;
                size_t available;
            };

            inline bulk_data_iterator(ActiveTable& table) : table(table) {
                #ifndef __OPTIMIZE__
                    if (table.is_iterator_running) {
                        logerr("[RUNTIME DEBUG] table '%s' iterator begin() called while another iterator is already running", table.header.name);
                        exit(1);
                    }

                    table.is_iterator_running = true;
                #endif
            }

            #ifndef __OPTIMIZE__
            ~bulk_data_iterator() { table.is_iterator_running = false; }
            #endif

            class iterator {
                public:
                    ActiveTable& table;
                    bool complete = false;
                    size_t buffer_records_available = BULK_HEADER_READ_COUNT;
                    size_t records_byte_offset = 0;
            
                    inline iterator(ActiveTable& tbl) : table(tbl) {}

                    iterator operator++() {
                        records_byte_offset += buffer_records_available * table.header.record_size;
                        
                        if (buffer_records_available != BULK_HEADER_READ_COUNT) {
                            complete = true;
                            return *this;
                        }
                        
                        buffer_records_available = fread_unlocked(table.header_buffer, table.header.record_size, BULK_HEADER_READ_COUNT, table.data_handle);
                        return *this;
                    }
            
                    inline progress operator*() {
                        return progress { bulk_byte_offset(), buffer_records_available };
                    }
            
                    inline size_t bulk_byte_offset() { return records_byte_offset; }
                    inline bool operator!=(const ph_end_iterator& _unused) { return !this->complete; }
            };

            inline iterator begin() {
                fseek(table.data_handle, 0, SEEK_SET);

                iterator i = iterator(table);
                i.operator++();
                i.records_byte_offset = 0;

                return i;
            }

            inline ph_end_iterator end() {
                return ph_end_iterator();
            }

        private:
            ActiveTable& table;
    };

    inline bulk_data_iterator iterate_bulk(ActiveTable& table) {
        return bulk_data_iterator(table);
    }
};