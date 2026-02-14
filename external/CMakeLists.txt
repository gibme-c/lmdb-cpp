add_subdirectory(cppfs)
add_subdirectory(lmdb)
add_subdirectory(snappy)

set_property(TARGET cppfs lmdb snappy PROPERTY FOLDER "external")

# Suppress warnings from external dependencies
foreach(_target cppfs lmdb snappy)
    # Treat external headers as SYSTEM so consumers don't see their warnings
    get_target_property(_dirs ${_target} INTERFACE_INCLUDE_DIRECTORIES)
    if(_dirs)
        set_target_properties(${_target} PROPERTIES INTERFACE_SYSTEM_INCLUDE_DIRECTORIES "${_dirs}")
    endif()

    # Silence warnings in external source files
    if(MSVC)
        target_compile_options(${_target} PRIVATE /W0)
    else()
        target_compile_options(${_target} PRIVATE -w)
    endif()
endforeach()

add_library(lmdbcpp-thirdparty INTERFACE)
target_link_libraries(lmdbcpp-thirdparty INTERFACE cppfs lmdb snappy)

if(WIN32)
    target_link_libraries(lmdbcpp-thirdparty INTERFACE ws2_32 advapi32 crypt32 gdi32 user32)
endif()
