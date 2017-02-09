CXX = g++

CXXFLAGS = -Wall -W -Wno-unused-parameter -g -O2 -D__STDC_FORMAT_MACROS -fPIC -std=c++11 -DROCKSDB_PLATFORM_POSIX -DOS_LINUX -fno-builtin-memcmp -DSNAPPY -DGFLAGS=google -DZLIB -DBZIP2

SRC_DIR = .
OUTPUT = .

LIB_PATH = -L../output/lib
LIBS = -lpthread \
			 -lnemodb \
			 -lrocksdb \
			 -lsnappy \
			 -lz \
			 -lbz2 \
			 -lrt

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
