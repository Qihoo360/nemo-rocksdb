SRC_DIR = ./src
OUTPUT = output

ROCKSDB_PATH = ./rocksdb

dummy_d := $(shell (make -C ./rocksdb static_lib))
include $(ROCKSDB_PATH)/make_config.mk
CXXFLAGS = $(PLATFORM_CXXFLAGS) $(PLATFORM_SHARED_CFLAGS) -Wall -W -Wno-unused-parameter -g -O2 -D__STDC_FORMAT_MACROS 

INCLUDE_PATH = -I./include \
			   -I./rocksdb/ \
			   -I./rocksdb/include

LIBRARY = libnemodb.a

.PHONY: all clean


BASE_OBJS := $(wildcard $(SRC_DIR)/*.cc)
BASE_OBJS += $(wildcard $(SRC_DIR)/*.c)
BASE_OBJS += $(wildcard $(SRC_DIR)/*.cpp)
OBJS = $(patsubst %.cc,%.o,$(BASE_OBJS))

all: $(LIBRARY)
	@echo "Success, go, go, go..."


$(LIBRARY): $(OBJS)
	rm -rf $(OUTPUT)
	mkdir -p $(OUTPUT)/include
	mkdir -p $(OUTPUT)/lib
	rm -rf $@
	ar -rcs $@ $(OBJS)
	cp $(ROCKSDB_PATH)/librocksdb.a $(OUTPUT)/lib
	mv ./libnemodb.a $(OUTPUT)/lib/
	cp -r ./include/* $(OUTPUT)/include

$(OBJS): %.o : %.cc
	$(CXX) $(CXXFLAGS) -c $< -o $@ $(INCLUDE_PATH)

clean: 
	rm -rf $(SRC_DIR)/*.o
	rm -rf $(OUTPUT)

distclean:
	make -C $(ROCKSDB_PATH) clean
	rm -rf $(SRC_DIR)/*.o
	rm -rf $(OUTPUT)

