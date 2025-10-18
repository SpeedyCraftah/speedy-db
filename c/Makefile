# Make flags.
RUN ?= 0

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

OBJ_FILES := $(patsubst $(SRC_DIR)/%.cpp,$(BUILD_DIR)/%.o,$(patsubst $(SRC_DIR)/%.c,$(BUILD_DIR)/%.o,$(SRC_FILES)))
FLAGS = -pthread -DRAPIDJSON_HAS_STDSTRING=1

# Targets
.PHONY: all clean clean-bin debug production kill

all: debug

debug: CXXFLAGS = $(FLAGS) -std=c++20 -g -O0 -DSIMDJSON_DEVELOPMENT_CHECKS=1
debug: CCFLAGS = $(FLAGS) -g -O0
debug: $(BIN_DIR)/debug

production: CXXFLAGS = $(FLAGS) -std=c++20 -Wall -Ofast -march=native -ffat-lto-objects -flto -fuse-linker-plugin
production: CCFLAGS = $(FLAGS) -Wall -Ofast -march=native -ffat-lto-objects -flto -fuse-linker-plugin
production: $(BIN_DIR)/production

# Linking
$(BIN_DIR)/debug: $(OBJ_FILES)
	@mkdir -p $(BIN_DIR)
	$(CXX) $(CXXFLAGS) -o $@ $(OBJ_FILES) -lcrypto
ifeq ($(RUN),1)
	$(BIN_DIR)/debug enable-root-account
endif

$(BIN_DIR)/production: $(OBJ_FILES)
	@mkdir -p $(BIN_DIR)
	$(CXX) $(CXXFLAGS) -o $@ $(OBJ_FILES) -lcrypto
ifeq ($(RUN),1)
	$(BIN_DIR)/production
endif

# Compiling
$(BUILD_DIR)/%.o: $(SRC_DIR)/%.cpp
	@mkdir -p $(@D)
	$(CXX) $(CXXFLAGS) -c $< -o $@

$(BUILD_DIR)/%.o: $(SRC_DIR)/%.c
	@mkdir -p $(@D)
	$(CXX) $(CCFLAGS) -c $< -o $@

# Cleaning
clean:
	rm -rf $(BUILD_DIR) $(BIN_DIR)/debug $(BIN_DIR)/production

clean-bin:
	rm -rf $(BIN_DIR)/debug $(BIN_DIR)/production

# Misc
kill:
	sudo kill $(sudo lsof -t -i :4546)