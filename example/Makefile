CXX = g++

CXXFLAGS = -Wall -W -Wno-unused-parameter -g -O2 -D__STDC_FORMAT_MACROS -fPIC -std=c++11 -DROCKSDB_PLATFORM_POSIX -DOS_LINUX -fno-builtin-memcmp -DSNAPPY -DGFLAGS=google -DZLIB -DBZIP2

OBJECT = example
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


BASE_OBJS := $(wildcard $(SRC_DIR)/*.cc)
BASE_OBJS += $(wildcard $(SRC_DIR)/*.c)
BASE_OBJS += $(wildcard $(SRC_DIR)/*.cpp)
OBJS = $(patsubst %.cc,%.o,$(BASE_OBJS))

all: $(OBJECT)
	@echo "Success, go, go, go..."

$(OBJECT): $(OBJS)
	$(CXX) $(CXXFLAGS) -o $@ $^ $(INCLUDE_PATH) $(LIB_PATH) $(LIBS)

$(OBJS): %.o : %.cc
	$(CXX) $(CXXFLAGS) -c $< -o $@ $(INCLUDE_PATH) 

$(TOBJS): %.o : %.cc
	$(CXX) $(CXXFLAGS) -c $< -o $@ $(INCLUDE_PATH) 

clean: 
	rm -rf ./*.o
	rm -rf $(OBJECT)
