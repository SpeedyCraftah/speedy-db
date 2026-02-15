#pragma once

#include <cstddef>
#include <cstdint>
#include "../table-basic.h"
#include "../../structures/simple_string.h"
#include "../table.h"
#include "types.h"

class NumericColumn : NoCopy {
    public:
        constexpr inline NumericColumn(TableColumn* column, NumericColumnData* data) : column(column), data(data) {};

        // Writes a value in the corresponding records column.
        // This will modify the underlying record at the columns position.
        inline void write(NumericColumnData data) {
            switch (this->column->type) {
                case ColumnType::Byte: 
                    this->data->byte = data.byte;
                    break;

                case ColumnType::Long64:
                    this->data->long64 = data.long64;
                    break;
                
                // Guaranteed to be 4 bytes in length.
                case ColumnType::Float32:
                case ColumnType::Integer:
                    this->data->unsigned32_raw = data.unsigned32_raw;
                    break;
                
                case ColumnType::String:
                    __builtin_unreachable();
            }
        }

        inline bool cmp_lt(const query_compiler::QueryComparator::Numeric& cmp) {
            switch (this->column->type) {
                case ColumnType::Byte:
                    return this->data->byte < cmp.comparator.byte;

                case ColumnType::Long64:
                    return this->data->long64 < cmp.comparator.long64;

                case ColumnType::Float32:
                    return this->data->float32 < cmp.comparator.float32;

                case ColumnType::Integer:
                    return this->data->int32 < cmp.comparator.int32;
                
                case ColumnType::String:
                    __builtin_unreachable();
            }

            __builtin_unreachable();
        }

        inline bool cmp_lt(const NumericColumn& cmp) {
            #if !defined(__OPTIMIZE__)
                _debug_check_type(cmp);
            #endif

            switch (this->column->type) {
                case ColumnType::Byte:
                    return this->data->byte < cmp.data->byte;

                case ColumnType::Long64:
                    return this->data->long64 < cmp.data->long64;

                case ColumnType::Float32:
                    return this->data->float32 < cmp.data->float32;

                case ColumnType::Integer:
                    return this->data->int32 < cmp.data->int32;
                
                case ColumnType::String:
                    __builtin_unreachable();
            }

            __builtin_unreachable();
        }

        inline bool cmp_gt(const query_compiler::QueryComparator::Numeric& cmp) {
            switch (this->column->type) {
                case ColumnType::Byte:
                    return this->data->byte > cmp.comparator.byte;

                case ColumnType::Long64:
                    return this->data->long64 > cmp.comparator.long64;

                case ColumnType::Float32:
                    return this->data->float32 > cmp.comparator.float32;

                case ColumnType::Integer:
                    return this->data->int32 > cmp.comparator.int32;
                
                case ColumnType::String:
                    __builtin_unreachable();
            }

            __builtin_unreachable();
        }
        
        inline bool cmp_gt(const NumericColumn& cmp) {
            #if !defined(__OPTIMIZE__)
                _debug_check_type(cmp);
            #endif

            switch (this->column->type) {
                case ColumnType::Byte:
                    return this->data->byte > cmp.data->byte;

                case ColumnType::Long64:
                    return this->data->long64 > cmp.data->long64;

                case ColumnType::Float32:
                    return this->data->float32 > cmp.data->float32;

                case ColumnType::Integer:
                    return this->data->int32 > cmp.data->int32;
                
                case ColumnType::String:
                    __builtin_unreachable();
            }

            __builtin_unreachable();
        }

        inline bool cmp_lte(const query_compiler::QueryComparator::Numeric& cmp) {
            switch (this->column->type) {
                case ColumnType::Byte:
                    return this->data->byte <= cmp.comparator.byte;

                case ColumnType::Long64:
                    return this->data->long64 <= cmp.comparator.long64;

                case ColumnType::Float32:
                    return this->data->float32 <= cmp.comparator.float32;

                case ColumnType::Integer:
                    return this->data->int32 <= cmp.comparator.int32;
                
                case ColumnType::String:
                    __builtin_unreachable();
            }

            __builtin_unreachable();
        }

        inline bool cmp_lte(const NumericColumn& cmp) {
            #if !defined(__OPTIMIZE__)
                _debug_check_type(cmp);
            #endif

            switch (this->column->type) {
                case ColumnType::Byte:
                    return this->data->byte <= cmp.data->byte;

                case ColumnType::Long64:
                    return this->data->long64 <= cmp.data->long64;

                case ColumnType::Float32:
                    return this->data->float32 <= cmp.data->float32;

                case ColumnType::Integer:
                    return this->data->int32 <= cmp.data->int32;
                
                case ColumnType::String:
                    __builtin_unreachable();
            }

            __builtin_unreachable();
        }

        inline bool cmp_gte(const query_compiler::QueryComparator::Numeric& cmp) {
            switch (this->column->type) {
                case ColumnType::Byte:
                    return this->data->byte >= cmp.comparator.byte;

                case ColumnType::Long64:
                    return this->data->long64 >= cmp.comparator.long64;

                case ColumnType::Float32:
                    return this->data->float32 >= cmp.comparator.float32;

                case ColumnType::Integer:
                    return this->data->int32 >= cmp.comparator.int32;
                
                case ColumnType::String:
                    __builtin_unreachable();
            }

            __builtin_unreachable();
        }

        inline bool cmp_gte(const NumericColumn& cmp) {
            #if !defined(__OPTIMIZE__)
                _debug_check_type(cmp);
            #endif

            switch (this->column->type) {
                case ColumnType::Byte:
                    return this->data->byte >= cmp.data->byte;

                case ColumnType::Long64:
                    return this->data->long64 >= cmp.data->long64;

                case ColumnType::Float32:
                    return this->data->float32 >= cmp.data->float32;

                case ColumnType::Integer:
                    return this->data->int32 >= cmp.data->int32;
                
                case ColumnType::String:
                    __builtin_unreachable();
            }

            __builtin_unreachable();
        }

        inline bool cmp_eq(const query_compiler::QueryComparator::Numeric& cmp) {
            switch (this->column->type) {
                case ColumnType::Byte:
                    return this->data->byte == cmp.comparator.byte;

                case ColumnType::Long64:
                    return this->data->long64 == cmp.comparator.long64;

                // Guaranteed to be 4 bytes in length.
                case ColumnType::Float32:
                case ColumnType::Integer:
                    return this->data->unsigned32_raw == cmp.comparator.unsigned32_raw;
                
                case ColumnType::String:
                    __builtin_unreachable();
            }

            __builtin_unreachable();
        }

        inline bool cmp_eq(const NumericColumn& cmp) {
            #if !defined(__OPTIMIZE__)
                if (column_type_sizeof(this->column->type) != column_type_sizeof(cmp.column->type)) {
                    puts("Debug build check: calling code tried compare numerics of mismatching sizes!");
                    std::terminate();
                }
            #endif

            switch (this->column->type) {
                case ColumnType::Byte:
                    return this->data->byte == cmp.data->byte;

                case ColumnType::Long64:
                    return this->data->long64 == cmp.data->long64;

                // Guaranteed to be 4 bytes in length.
                case ColumnType::Float32:
                case ColumnType::Integer:
                    return this->data->unsigned32_raw == cmp.data->unsigned32_raw;
                
                case ColumnType::String:
                    __builtin_unreachable();
            }

            __builtin_unreachable();
        }

        // Safely converts the referenced numeric column data into an owned copy.
        NumericColumnData to_owned_data() {
            NumericColumnData data;
            switch (this->column->type) {
                case ColumnType::Byte:
                    data.byte = this->data->byte;
                    break;

                case ColumnType::Long64:
                    data.long64 = this->data->long64;
                    break;

                // Guaranteed to be 4 bytes in length.
                case ColumnType::Float32:
                case ColumnType::Integer:
                    data.unsigned32_raw = this->data->unsigned32_raw;
                    break;
                
                case ColumnType::String:
                    __builtin_unreachable();
            }

            return data;
        }

    private:
        TableColumn* column;
        NumericColumnData* data;

        // A safety check only present in debug builds to ensure that incompatible types are not compared.
        #if !defined(__OPTIMIZE__)
            void _debug_check_type(const NumericColumn& cmp) {
                if (this->column->type != cmp.column->type) {
                    puts("Debug build check: calling code tried to compare incompatible numerics!");
                    std::terminate();
                }
            }
        #endif
};

// A structure which abstracts a raw record into a modern interface.
class Record : RecordData, NoCopy {
    public:
        constexpr inline Record(ActiveTable& table, RecordData* data) : table(table), data(data) {};

        // Returns a pointer to the impl_flags internal column data.
        inline RecordFlags* get_flags() noexcept {
            return (RecordFlags*)(this->data + this->table.impl_flags_column->buffer_offset);
        }

        // Returns a numeric pointer to the column data.
        inline NumericColumnData* get_numeric_raw(TableColumn* column) noexcept {
            #if !defined(__OPTIMIZE__)
                if (!column_type_is_numeric(column->type)) {
                    puts("Debug build check: calling code tried to get a numeric column that is actually non-numeric!");
                    std::terminate();
                }
            #endif

            return (NumericColumnData*)(this->data + column->buffer_offset);
        }

        // Returns a safe and helpful numeric wrapper to the column data.
        inline NumericColumn get_numeric(TableColumn* column) noexcept {
            #if !defined(__OPTIMIZE__)
                if (!column_type_is_numeric(column->type)) {
                    puts("Debug build check: calling code tried to get a numeric column that is actually non-numeric!");
                    std::terminate();
                }
            #endif

            return NumericColumn(column, get_numeric_raw(column));
        }

        // Returns a hashed entry metadata pointer to the column data.
        inline HashedColumnData* get_hashed(TableColumn* column) noexcept {
            #if !defined(__OPTIMIZE__)
                if (column->type != ColumnType::String) {
                    puts("Debug build check: calling code tried to get a hashed column that is actually non-hashed!");
                    std::terminate();
                }
            #endif

            return (HashedColumnData*)(this->data + column->buffer_offset);
        }

        // Loads the dynamic data behind the hashed entry column into an owned, lightweight string.
        speedystd::simple_string load_dynamic(TableColumn* column) noexcept {
            #if !defined(__OPTIMIZE__)
                if (column->type != ColumnType::String) {
                    puts("Debug build check: calling code tried to get a hashed column that is actually non-hashed!");
                    std::terminate();
                }
            #endif

            HashedColumnData* entry = get_hashed(column);

            // Create the empty string buffer and load the dynamic directly into it.
            // TODO: move this to a preallocated buffer simple_buffer.
            speedystd::simple_string str_buffer;
            load_dynamic_into(entry, str_buffer.init(entry->size));

            return str_buffer;
        }

        // Loads the dynamic data behind the hashed entry column directly into the specified destination.
        void load_dynamic_into(HashedColumnData* hashed_column_meta, void* dest) noexcept {
            size_t pread_result = pread(table.dynamic_handle, dest, hashed_column_meta->size, hashed_column_meta->record_location + sizeof(DynamicRecord));
            if (pread_result != hashed_column_meta->size) {
                /* Will be improved after disk read overhaul */
                logerr("Error or incorrect number of bytes returned from pread for dynamic string");
                exit(1);
            }
        }

        // Creates a copy of this record and returns a pointer to it.
        std::unique_ptr<RecordData[]> to_owned_data() {
            std::unique_ptr<RecordData[]> owned_data(new RecordData[this->table.header.record_size]);
            memcpy(owned_data.get(), this->data, this->table.header.record_size);

            return owned_data;
        }

        constexpr inline explicit operator RecordData*() { return this->data; };

    private:
        ActiveTable& table;
        RecordData* data;
};