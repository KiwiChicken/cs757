#
# Dependencies
#
find_package(Threads)
find_package (glog 0.7.0 REQUIRED)
#
# Sources
#
set(SOURCES_SIMPLE_RR_SERVER
    src/simpleRRServer.cpp
)

set(SOURCES_REQUEST_LB_SERVER
    src/requestLBServer.cpp
)

set(SOURCES_CACHE_LB_SERVER
    src/cacheLBServer.cpp
)

source_group(TREE ${CMAKE_CURRENT_SOURCE_DIR} FILES ${SOURCES_SIMPLE_RR_SERVER} ${SOURCES_REQUEST_LB_SERVER} ${SOURCES_CACHE_LB_SERVER})

#
# Target
#
add_executable(SimpleRRServer ${SOURCES_SIMPLE_RR_SERVER})
target_include_directories(SimpleRRServer PRIVATE ${PROJECT_SOURCE_DIR}/third_party/rocksdb/include/)
target_link_directories(SimpleRRServer PRIVATE ${PROJECT_SOURCE_DIR}/third_party/rocksdb)
target_link_libraries(SimpleRRServer PRIVATE myproto PRIVATE rocksdb)

add_executable(RequestLBServer ${SOURCES_REQUEST_LB_SERVER})
target_include_directories(RequestLBServer PRIVATE ${PROJECT_SOURCE_DIR}/third_party/rocksdb/include/)
target_link_directories(RequestLBServer PRIVATE ${PROJECT_SOURCE_DIR}/third_party/rocksdb)
target_link_libraries(RequestLBServer PRIVATE myproto PRIVATE rocksdb)

add_executable(CacheLBServer ${SOURCES_CACHE_LB_SERVER})
target_include_directories(CacheLBServer PRIVATE ${PROJECT_SOURCE_DIR}/third_party/rocksdb/include/)
target_link_directories(CacheLBServer PRIVATE ${PROJECT_SOURCE_DIR}/third_party/rocksdb)
target_link_libraries(CacheLBServer PRIVATE myproto PRIVATE rocksdb)