add_subdirectory(cppfs)
add_subdirectory(lmdb)
add_subdirectory(snappy)

set_property(TARGET cppfs lmdb snappy PROPERTY FOLDER "external")

add_library(ThirdParty INTERFACE)
target_link_libraries(ThirdParty INTERFACE cppfs lmdb snappy)

if(WIN32)
    target_link_libraries(ThirdParty INTERFACE ws2_32 advapi32 crypt32 gdi32 user32)
endif()
