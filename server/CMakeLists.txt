#
# Dependencies
#
find_package(Threads)

#
# Sources
#
set(SOURCES
    src/simpleRRServer.cpp
)

source_group(TREE ${CMAKE_CURRENT_SOURCE_DIR} FILES ${SOURCES})


#
# Target
#
add_executable(server ${SOURCES})
target_include_directories(server PRIVATE ${PROJECT_SOURCE_DIR}/third_party/rocksdb/include/)
target_link_directories(server PRIVATE ${PROJECT_SOURCE_DIR}/third_party/rocksdb)
target_link_libraries(server PRIVATE myproto PRIVATE rocksdb)
