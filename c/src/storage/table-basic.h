#pragma once

#include <cstddef>
#include <cstdint>

enum ColumnType : uint32_t {
    Integer,
    Float32,
    Long64,
    Byte,
    String
};

// The record preamble holding useful flags for each record.
#define INTERNAL_COLUMN_IMPL_FLAGS_NAME "impl_flags"
typedef uint8_t RecordImplFlags;
enum RecordFlags : RecordImplFlags {
    Active = 1 // Determines whether this block holds an active record.
};

// A dummy type to clearly identify memory that refers to a record.
struct RecordData {} __attribute__((packed));

// A variation of record data which outlines the definition for a hashed column which references dynamic data.
struct HashedColumnData : RecordData {
    size_t hash;
    size_t record_location; // TODO: with block-based storage, this can be reduced to a uint32_t and be naturally aligned.
    uint32_t size;
} __attribute__((packed));

struct TableColumn {
    char name[33] = {0};
    bool is_implementation = false; // Whether the column is hidden from the user and strictly for implementation use.
    uint8_t name_length;
    ColumnType type;
    uint32_t index;
    uint32_t buffer_offset;
};

struct TableHeader {
    uint32_t magic_number;
    char name[33] = {0};
    uint32_t num_columns;
    uint32_t record_size;
};

struct DynamicRecord {
    size_t record_location;
    uint32_t physical_size;
    char data[];
} __attribute__((packed));

union NumericColumnData {
    size_t unsigned64_raw = 0;
    uint32_t unsigned32_raw;
    long long64;
    uint8_t byte;
    int int32;
    float float32;
};