# Fetch all cpp files from the current directory and sub directories and put in a variable.
files = $(wildcard ./src/*.cpp) $(wildcard ./src/**/*.cpp) $(wildcard ./src/*.c) $(wildcard ./src/**/*.c) $(wildcard ./src/**/**/*.c)
flags = -pthread

run:
	g++ -g -O0 -DSIMDJSON_DEVELOPMENT_CHECKS -o ./bin/bin $(flags) $(files) -lcrypto
	./bin/bin password=test

production:
	g++ -Wall -Ofast -ffat-lto-objects -flto -o ./bin/bin $(flags) $(files) -lcrypto