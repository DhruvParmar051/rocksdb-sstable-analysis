# CMake glue for the optional pybind11 extension (pass 2).
#
# Build:
#   cd python && mkdir -p build && cd build
#   cmake -DCMAKE_BUILD_TYPE=Release ..
#   cmake --build . -j
#   cp _native*.so ../sstable_experiments/
#
# Requires the rocksdb static library at ../rocksdb/librocksdb.a (run
# `make static_lib -j` in rocksdb/ first).

cmake_minimum_required(VERSION 3.18)
project(sstable_experiments_native LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_POSITION_INDEPENDENT_CODE ON)

find_package(Python3 REQUIRED COMPONENTS Interpreter Development)
find_package(pybind11 CONFIG REQUIRED)

set(ROCKSDB_ROOT "${CMAKE_CURRENT_SOURCE_DIR}/../rocksdb"
    CACHE PATH "Path to vendored RocksDB tree")

pybind11_add_module(_native sstable_experiments/bindings.cc)

target_include_directories(_native PRIVATE
    "${ROCKSDB_ROOT}/include"
    "${ROCKSDB_ROOT}")

target_link_libraries(_native PRIVATE
    "${ROCKSDB_ROOT}/librocksdb.a"
    pthread dl z bz2 snappy lz4 zstd)
