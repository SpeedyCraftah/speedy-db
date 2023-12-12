# Fetch all cpp files from the current directory and sub directories and put in a variable.
files = $(wildcard ./src/*.cpp) $(wildcard ./src/**/*.cpp) $(wildcard ./src/*.c) $(wildcard ./src/**/*.c) $(wildcard ./src/**/**/*.c) $(wildcard ./src/**/**/*.cpp)
flags = -pthread -DRAPIDJSON_HAS_STDSTRING=1

run:
	g++ -std=c++20 -g -O0 -DSIMDJSON_DEVELOPMENT_CHECKS=1 -o ./bin/bin $(flags) $(files) -lcrypto
	./bin/bin enable-root-account

production:
	g++ -std=c++20 -Wall -Ofast -ffat-lto-objects -flto -o ./bin/bin $(flags) $(files) -lcrypto