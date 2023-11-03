#include "table.h"
#include <cstddef>
#include <unordered_map>

// TODO - add mutex

ActiveTable::ActiveTable(const char* table_name, bool is_internal = false) : is_internal(is_internal) {
    // Create the data paths.
    std::string path = std::string("./data/").append(table_name);
    std::string meta_path = path + "/meta.bin";
    std::string data_path = path + "/data.bin";
    std::string dynamic_path = path + "/dynamic.bin";
    std::string permissions_path = path + "/permissions.bin";

    // Open the files in r+w mode.
    FILE* header_handle = fopen(meta_path.c_str(), "r+b");
    fseek(header_handle, 0, SEEK_SET);

    FILE* permissions_handle = fopen(permissions_path.c_str(), "r+b");
    fseek(permissions_handle, 0, SEEK_SET);

    this->data_handle = fopen(data_path.c_str(), "r+b");
    fseek(data_handle, 0, SEEK_SET);

    this->dynamic_handle = fopen(dynamic_path.c_str(), "r+b");
    fseek(dynamic_handle, 0, SEEK_SET);

    // Read the header.
    fread_unlocked(&this->header, 1, sizeof(table_header), header_handle);

    this->record_size += sizeof(record_header);

    // Load the columns to the map.
    for (int i = 0; i < this->header.num_columns; i++) {
        table_column& column = this->header.columns[i];

        // Keep index order intact.
        column.index = i;

        // Add sizes.
        this->record_data_size += column.size == 0 ? sizeof(hashed_entry) : column.size;
        this->hashed_column_count += column.size == 0 ? 1 : 0;

        this->columns[this->header.columns[i].name] = this->header.columns[i];
    }

    this->record_size += this->record_data_size;

    // Load the account permissions if table is not internal as internal tables do not have custom permissions.
    if (this->is_internal) {
        // Create permission map instance.
        this->permissions = new std::unordered_map<size_t, TablePermissions>();

        // todo
    }

    // Close handles which are not needed.
    fclose(header_handle);
    fclose(permissions_handle);
}

ActiveTable::~ActiveTable() {
    // Close handles.
    fclose(this->data_handle);
    fclose(this->dynamic_handle);

    // Free permissions if needed.
    if (this->permissions != nullptr) delete this->permissions;
}