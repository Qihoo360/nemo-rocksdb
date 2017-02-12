include ../rocksdb/make_config.mk
CXXFLAGS = $(PLATFORM_CXXFLAGS) $(PLATFORM_SHARED_CFLAGS) -Wall -W -Wno-unused-parameter -g -O2 -D__STDC_FORMAT_MACROS 

SRC_DIR = .
OUTPUT = .

LIB_PATH = -L../output/lib
LIBS = -lpthread \
			 -lnemodb \
			 -lrocksdb \
			 -lsnappy \
			 -lz \
			 -lbz2

INCLUDE_PATH = -I../output/include/ \
							 -I../rocksdb/ \
							 -I../rocksdb/include/

.PHONY: all clean

all: example benchmark
	@echo "Success, go, go, go..."

example: example.cc
	$(CXX) $(CXXFLAGS) -o $@ $^ $(INCLUDE_PATH) $(LIB_PATH) $(LIBS)

benchmark: benchmark.cc
	$(CXX) $(CXXFLAGS) -o $@ $^ $(INCLUDE_PATH) $(LIB_PATH) $(LIBS)

clean: 
	rm -rf ./*.o
	rm -rf ./example ./benchmark
