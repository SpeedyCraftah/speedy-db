#pragma once

#include <cstddef>
#include <cstdint>
#include "../table-basic.h"
#include "../../structures/simple_string.h"
#include "../table.h"

// A structure which abstracts a raw record into a modern interface.
class Record : RecordData {
    public:
        constexpr inline Record(ActiveTable& table, RecordData* data) : table(table), data(data) {};

        // Returns a pointer to the impl_flags internal column data.
        inline RecordFlags* get_flags() noexcept {
            return (RecordFlags*)(this->data + this->table.impl_flags_column->buffer_offset);
        }

        // Returns a numeric pointer to the column data.
        inline NumericColumnData* get_numeric(TableColumn* column) noexcept {
            return (NumericColumnData*)(this->data + column->buffer_offset);
        }

        // Returns a hashed entry metadata pointer to the column data.
        inline HashedColumnData* get_hashed(TableColumn* column) noexcept {
            return (HashedColumnData*)(this->data + column->buffer_offset);
        }

        // Loads the dynamic data behind the hashed entry column into an owned, lightweight string.
        speedystd::simple_string load_dynamic(TableColumn* column) noexcept {
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

        constexpr inline explicit operator RecordData*() { return this->data; };

    private:
        ActiveTable& table;
        RecordData* data;
};