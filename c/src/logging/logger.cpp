#include "logger.h"
#include <stdio.h>
#include <stdarg.h>

void logerr(const char* str, ...) {
    va_list args;
    va_start(args, str);

    printf("\033[0;37m[\033[0;31mERR\033[0;37m] \033[0m");
    vprintf(str, args);
    printf("\n");

    va_end(args);
}

void log(const char* str, ...) {
    va_list args;
    va_start(args, str);

    printf("\033[0;37m[\033[0;36mLOG\033[0;37m] \033[0m");
    vprintf(str, args);
    printf("\n");

    va_end(args);
}

void logwarn(const char* str, ...) {
    va_list args;
    va_start(args, str);

    printf("\033[0;37m[\033[0;33mWARN\033[0;37m] \033[0m");
    vprintf(str, args);
    printf("\n");

    va_end(args);
}