SRC_DIR = $(CURDIR)/src
OUTPUT = $(CURDIR)/output

ROCKSDB_PATH = $(CURDIR)/rocksdb

dummy := $(shell (cd $(ROCKSDB_PATH); export ROCKSDB_ROOT="$(CURDIR)/rocksdb"; "$(CURDIR)/rocksdb/build_tools/build_detect_platform" "$(CURDIR)/make_config.mk"))
include $(CURDIR)/make_config.mk
CXXFLAGS = $(PLATFORM_CXXFLAGS) $(PLATFORM_SHARED_CFLAGS) -Wall -W -Wno-unused-parameter -g -O2 -D__STDC_FORMAT_MACROS 

INCLUDE_PATH = -I./ \
							 -I./rocksdb/ \
							 -I./rocksdb/include

LIBRARY = libnemodb.a
ROCKSDB = $(ROCKSDB_PATH)/librocksdb.a

.PHONY: all clean


BASE_OBJS := $(wildcard $(SRC_DIR)/*.cc)
BASE_OBJS += $(wildcard $(SRC_DIR)/*.c)
BASE_OBJS += $(wildcard $(SRC_DIR)/*.cpp)
OBJS = $(patsubst %.cc,%.o,$(BASE_OBJS))

all: $(ROCKSDB) $(LIBRARY)
	mv $(LIBRARY) $(OUTPUT)/lib
	@echo "Success, go, go, go..."

$(ROCKSDB):
	make -C $(ROCKSDB_PATH) static_lib

$(LIBRARY): $(OBJS)
	rm -rf $(OUTPUT)
	mkdir -p $(OUTPUT)/include
	mkdir -p $(OUTPUT)/lib
	rm -rf $@
	ar -rcs $@ $(OBJS)
	cp $(ROCKSDB_PATH)/librocksdb.a $(OUTPUT)/lib
	cp -r ./include/* $(OUTPUT)/include

$(OBJS): %.o : %.cc
	$(CXX) $(CXXFLAGS) -c $< -o $@ $(INCLUDE_PATH)

clean: 
	rm -rf $(SRC_DIR)/*.o
	rm -rf $(OUTPUT)

distclean:
	make -C $(ROCKSDB_PATH) clean
	rm -rf $(CURDIR)/make_config.mk
	rm -rf $(SRC_DIR)/*.o
	rm -rf $(OUTPUT)

