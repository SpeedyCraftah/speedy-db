#include "files.h"

#include <sys/stat.h>

bool folder_exists(const char* path) {
    struct stat sb;
    return stat(path, &sb) == 0 && S_ISDIR(sb.st_mode);
}

bool file_exists(const char* path) {
    struct stat sb;
    return stat(path, &sb) == 0 && S_ISREG(sb.st_mode);
}