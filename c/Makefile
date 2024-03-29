# Directories
SRC_DIR = ./src
BUILD_DIR = ./build
BIN_DIR = ./bin

# Files
SRC_FILES = $(wildcard $(SRC_DIR)/*.cpp) $(wildcard $(SRC_DIR)/**/*.cpp) $(wildcard $(SRC_DIR)/*.c) $(wildcard $(SRC_DIR)/**/*.c) $(wildcard $(SRC_DIR)/**/**/*.c) $(wildcard $(SRC_DIR)/**/**/*.cpp)
OBJ_FILES = $(patsubst $(SRC_DIR)/%.cpp,$(BUILD_DIR)/%.o,$(SRC_FILES))
FLAGS = -std=c++20 -pthread -DRAPIDJSON_HAS_STDSTRING=1

# Targets
.PHONY: all clean clean-bin debug production kill

all: debug

debug: CXXFLAGS = $(FLAGS) -g -O0 -DSIMDJSON_DEVELOPMENT_CHECKS=1
debug: $(BIN_DIR)/debug

production: CXXFLAGS = $(FLAGS) -Wall -Ofast -ffat-lto-objects -flto
production: $(BIN_DIR)/production

# Linking
$(BIN_DIR)/debug: $(OBJ_FILES)
	@mkdir -p $(BIN_DIR)
	$(CXX) $(CXXFLAGS) -o $@ $(OBJ_FILES) -lcrypto
	$(BIN_DIR)/debug enable-root-account

$(BIN_DIR)/production: $(OBJ_FILES)
	@mkdir -p $(BIN_DIR)
	$(CXX) $(CXXFLAGS) -o $@ $(OBJ_FILES) -lcrypto

# Compiling
$(BUILD_DIR)/%.o: $(SRC_DIR)/%.cpp
	@mkdir -p $(@D)
	$(CXX) $(CXXFLAGS) -c $< -o $@

# Cleaning
clean:
	rm -rf $(BUILD_DIR) $(BIN_DIR)/debug $(BIN_DIR)/production

clean-bin:
	rm -rf $(BIN_DIR)/debug $(BIN_DIR)/production

# Misc
kill:
	sudo kill $(sudo lsof -t -i :4546)