include ../make_config.mk

.PHONY: clean

all: examples benchmark

ifndef ROCKSDB_PATH
$(info rocksdb path is not given, use default: ../rocksdb)
$(info acquiring rocksdb from github, wait...)
dummy := $(shell cd ..; git submodule init; git submodule update)
$(info acquire rocksdb from github is done)

ROCKSDB_PATH = ../rocksdb
endif

LIBROCKSDB =$(ROCKSDB_PATH)/librocksdb.a
LIBNEMODB =../lib/libnemodb.a

$(LIBROCKSDB):
	make -C $(ROCKSDB_PATH) static_lib

examples: examples.cc $(LIBNEMODB) $(LIBROCKSDB)
	$(CXX) $(CXXFLAGS) $^ -o$@ -I../ -I$(ROCKSDB_PATH) -I$(ROCKSDB_PATH)/include -O2 -std=c++11 $(PLATFORM_LDFLAGS) $(PLATFORM_CXXFLAGS)

benchmark: benchmark.cc $(LIBNEMODB) $(LIBROCKSDB)
	$(CXX) $(CXXFLAGS) $^ -o$@  -I../ -I$(ROCKSDB_PATH) -I$(ROCKSDB_PATH)/include -O2 -std=c++11 $(PLATFORM_LDFLAGS) $(PLATFORM_CXXFLAGS)

clean:
	find . -name "*.[oda]" -exec rm -f {} \;
	rm -rf ./examples ./benchmark
