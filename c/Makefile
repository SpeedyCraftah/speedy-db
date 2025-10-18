# Directories
SRC_DIR = ./src
BUILD_DIR = ./build
BIN_DIR = ./bin

# Files
SRC_FILES = $(wildcard $(SRC_DIR)/*.cpp) $(wildcard $(SRC_DIR)/**/*.cpp) $(wildcard $(SRC_DIR)/*.c) $(wildcard $(SRC_DIR)/**/*.c) $(wildcard $(SRC_DIR)/**/**/*.c) $(wildcard $(SRC_DIR)/**/**/*.cpp)

# We don't want to link XXHash dispatching which only works on X86 on any other architecture, otherwise build will fail.
IS_X86 := $(filter x86_64 i386 i486 i586 i686,$(shell uname -m))
ifeq ($(IS_X86),)
    SRC_FILES := $(filter-out $(SRC_DIR)/deps/xxh/xxh_x86dispatch.c $(SRC_DIR)/deps/xxh/xxh_x86dispatch.h, $(SRC_FILES))
endif

OBJ_FILES = $(patsubst $(SRC_DIR)/%.cpp,$(BUILD_DIR)/%.o,$(SRC_FILES))
FLAGS = -std=c++20 -pthread -DRAPIDJSON_HAS_STDSTRING=1

# Targets
.PHONY: all clean clean-bin debug production kill

all: debug

debug: CXXFLAGS = $(FLAGS) -g -O0 -DSIMDJSON_DEVELOPMENT_CHECKS=1 -Wall 
debug: $(BIN_DIR)/debug

# For the build tester
debug-norun: CXXFLAGS = $(FLAGS) -g -O0 -DSIMDJSON_DEVELOPMENT_CHECKS=1 -Wall
debug-norun: $(BIN_DIR)/debug-norun

production: CXXFLAGS = $(FLAGS) -Wall -Ofast -march=native -ffat-lto-objects -flto -fuse-linker-plugin
production: $(BIN_DIR)/production

# Linking
$(BIN_DIR)/debug: $(OBJ_FILES)
	@mkdir -p $(BIN_DIR)
	$(CXX) $(CXXFLAGS) -o $@ $(OBJ_FILES) -lcrypto
	$(BIN_DIR)/debug enable-root-account

$(BIN_DIR)/debug-norun: $(OBJ_FILES)
	@mkdir -p $(BIN_DIR)
	$(CXX) $(CXXFLAGS) -o $@ $(OBJ_FILES) -lcrypto

$(BIN_DIR)/production: $(OBJ_FILES)
	@mkdir -p $(BIN_DIR)
	$(CXX) $(CXXFLAGS) -o $@ $(OBJ_FILES) -lcrypto

# Compiling
$(BUILD_DIR)/%.o: $(SRC_DIR)/%.cpp
	@mkdir -p $(@D)
	$(CXX) $(CXXFLAGS) -c $< -o $@

# Cleaning
clean:
	rm -rf $(BUILD_DIR) $(BIN_DIR)/debug $(BIN_DIR)/production $(BIN_DIR)/debug-norun

clean-bin:
	rm -rf $(BIN_DIR)/debug $(BIN_DIR)/production $(BIN_DIR)/debug-norun

# Misc
kill:
	sudo kill $(sudo lsof -t -i :4546)