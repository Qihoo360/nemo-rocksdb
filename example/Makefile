include ../make_config.mk

.PHONY: clean

all: examples benchmark

ifndef ROCKSDB_PATH
  $(warning Warning: rocksdb path required, using defaut path)
  ROCKSDB_PATH = ../rocksdb
endif

LIBROCKSDB =$(ROCKSDB_PATH)/librocksdb.a
LIBNEMODB =../lib/libnemodb.a

examples: examples.cc $(LIBNEMODB) $(LIBROCKSDB)
	$(CXX) $(CXXFLAGS) $^ -o$@ -I../ -I$(ROCKSDB_PATH) -I$(ROCKSDB_PATH)/include -O2 -std=c++11 $(PLATFORM_LDFLAGS) $(PLATFORM_CXXFLAGS)

benchmark: benchmark.cc $(LIBNEMODB) $(LIBROCKSDB)
	$(CXX) $(CXXFLAGS) $^ -o$@  -I../ -I$(ROCKSDB_PATH) -I$(ROCKSDB_PATH)/include -O2 -std=c++11 $(PLATFORM_LDFLAGS) $(PLATFORM_CXXFLAGS)

clean:
	find . -name "*.[oda]" -exec rm -f {} \;
	rm -rf ./example ./benchmark
