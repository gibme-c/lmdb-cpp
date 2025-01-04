cmake_minimum_required(VERSION 2.8.12)

if(DEFINED CMAKE_BUILD_TYPE)
    set(CMAKE_BUILD_TYPE ${CMAKE_BUILD_TYPE} CACHE STRING "Choose the type of build, options are: Debug, Release, RelWithDebInfo")
else()
    set(CMAKE_BUILD_TYPE Release CACHE STRING "Choose the type of build, options are: Debug, Release, RelWithDebInfo")
endif()

set(CMAKE_CONFIGURATION_TYPES Debug RelWithDebInfo Release CACHE STRING INTERNAL)
set(ARCH native CACHE STRING "CPU to build for: -march value or native")

message(STATUS "Building for target architecture: ${ARCH}")

set(LIB_MAJOR_VERSION "2")
set(LIB_MINOR_VERSION "0")
set(LIB_PATCH_VERSION "0")
set(LIB_VERSION_STRING "${LIB_MAJOR_VERSION}.${LIB_MINOR_VERSION}.${LIB_PATCH_VERSION}")

if(CMAKE_VERSION VERSION_LESS 3.0)
    project(lmdbcpp CXX)
    enable_language(CXX)
else()
    cmake_policy(SET CMP0048 NEW)
    project(lmdbcpp VERSION "${LIB_VERSION_STRING}" LANGUAGES CXX)
endif()

if(NOT MSVC)
    find_program(CCACHE_PROGRAM ccache)
    if(CCACHE_PROGRAM)
        message(STATUS "Found ccache package... Activating...")
        set_property(GLOBAL PROPERTY RULE_LAUNCH_COMPILE "${CCACHE_PROGRAM}")
    endif()
endif()

## This section describes our general CMake setup options
set_property(GLOBAL PROPERTY USE_FOLDERS ON)
set(CMAKE_SKIP_INSTALL_RULES OFF FORCE)
set(CMAKE_SKIP_PACKAGE_ALL_DEPENDENCY ON FORCE)
set(CMAKE_SUPPRESS_REGENERATION ON)
set(CMAKE_POLICY_WARNING_CMP0048 OFF)
set(CMAKE_MODULE_PATH ${CMAKE_MODULE_PATH} ${CMAKE_CURRENT_SOURCE_DIR}/cmake)
set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR})

# Enable c++17
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

## We only build static binaries -- this is left here for our dependencies
set(STATIC ON CACHE BOOL FORCE "Link libraries statically? Forced to ON")
add_definitions(-DSTATICLIB)
add_definitions(-DRAPIDJSON_HAS_STDSTRING)

set(NO_AES OFF CACHE BOOL "Turn off Hardware AES instructions?")
set(NO_OPTIMIZED_MULTIPLY_ON_ARM OFF CACHE BOOL "Turn off Optimized Multiplication on ARM?")

if (NO_AES)
    add_definitions(-DNO_AES)
    message(STATUS "HW AES: DISABLED")
else ()
    message(STATUS "HW AES: ENABLED")
endif ()

if (NO_OPTIMIZED_MULTIPLY_ON_ARM)
    add_definitions(-DNO_OPTIMIZED_MULTIPLY_ON_ARM)
    message(STATUS "OPTIMIZED_ARM_MULTIPLICATION: DISABLED")
else ()
    message(STATUS "OPTIMIZED_ARM_MULTIPLICATION: ENABLED")
endif ()

option(STATIC_LIBC "Build with static libc" OFF)
if(DEFINED ENV{STATIC_LIBC})
    set(STATIC_LIBC $ENV{STATIC_LIBC})
endif()
if(STATIC_LIBC)
    message(STATUS "Forcing build with statically linked libc, binaries may not be portable")
endif()

option(BUILD_UNIT_TESTS "Build included tests" OFF)
if(DEFINED ENV{BUILD_UNIT_TESTS})
    set(BUILD_UNIT_TESTS $ENV{BUILD_UNIT_TESTS})
endif()
if(BUILD_UNIT_TESTS)
    message(STATUS "Tests added to targets list")
endif()

# We need to set the label and import it into CMake if it exists
set(LABEL "")
if (DEFINED ENV{LABEL})
    set(LABEL $ENV{LABEL})
    message(STATUS "Found LABEL: ${LABEL}")
endif ()

if(MSVC)
    set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} /D_CRT_SECURE_NO_WARNINGS")
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std:c++17 /wd4267 /wd4804 /wd4996 /D_DLL /D_CRT_SECURE_NO_WARNINGS")

    set(CMAKE_C_FLAGS_RELEASE "${CMAKE_C_FLAGS_RELEASE} /O2")
    set(CMAKE_CXX_FLAGS_RELEASE "${CMAKE_CXX_FLAGS_RELEASE} /O2")
else()
    if("${ARCH}" STREQUAL "default")
        set(ARCH_FLAG "")
    else()
        set(ARCH_FLAG "-march=${ARCH}")
    endif()

    if (${CMAKE_SYSTEM_PROCESSOR} STREQUAL "aarch64" OR "${LABEL}" STREQUAL "aarch64")
        set(MAES_FLAG "")
        set(CRYPTOPP_AARCH64 ON CACHE BOOL FORCE "Tell CryptoPP that we are building for aarch64")
    elseif (${CMAKE_SYSTEM_PROCESSOR} MATCHES "x86_64" AND NOT "${LABEL}" STREQUAL "aarch64")
        set(MAES_FLAG "-maes")
    else ()
        set(MAES_FLAG "")
    endif ()

    set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -std=c11 -Wuninitialized ${MAES_FLAG}")
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++17 -Wuninitialized ${MAES_FLAG}")

    set(CMAKE_C_FLAGS_DEBUG "${CMAKE_C_FLAGS_DEBUG} -g3 -Og")
    set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -g3 -Og")

    set(CMAKE_C_FLAGS_RELEASE "${CMAKE_C_FLAGS_RELEASE} -DNDEBUG -O3")
    set(CMAKE_CXX_FLAGS_RELEASE "${CMAKE_CXX_FLAGS_RELEASE} -DNDEBUG -O3")

    if(APPLE)
        if(CMAKE_C_COMPILER_ID STREQUAL "GNU")
            # Need to build against libc++ instead of libstc++ on apple
            set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -nostdinc++ -I/usr/local/opt/llvm/include/c++/v1 -nodefaultlibs -lc++ -lc++abi -lm -lc -lgcc -lgcc_eh")

            # Need these flags so gcc OSX works on the cryptopp ASM - https://groups.google.com/forum/#!topic/cryptopp-users/po8yO-vYaac
            set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -DCRYPTOPP_CLANG_INTEGRATED_ASSEMBLER=1")

        elseif(CMAKE_C_COMPILER_ID STREQUAL "Clang")
            # Need to link against the llvm libc++ library, default is too old for std::filesystem
            set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -stdlib=libc++ -I/usr/local/opt/llvm/include/c++/v1")
        endif()
    endif()

    if(MINGW OR STATIC_LIBC)
        set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -static -static-libstdc++")
        set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -static")
    endif()

    if(NOT APPLE)
        set(CMAKE_EXE_LINKER_FLAGS  "${CMAKE_EXE_LINKER_FLAGS} -static-libgcc -static-libstdc++")
    endif()
endif()

add_subdirectory(${CMAKE_CURRENT_SOURCE_DIR}/external)

set(INCLUDE_DIRECTORIES
    ${CMAKE_CURRENT_SOURCE_DIR}/include
)

include_directories(${INCLUDE_DIRECTORIES})

set(SOURCES
    src/lmdb_errors.cpp
    src/lmdb_cpp.cpp
)

add_library(lmdbcpp-static STATIC ${SOURCES})
target_link_libraries(lmdbcpp-static ThirdParty)
target_include_directories(lmdbcpp-static PUBLIC ${INCLUDE_DIRECTORIES})

if(WIN32)
    target_link_libraries(lmdbcpp-static ws2_32 advapi32 crypt32 gdi32 user32)
endif()

set(TEST_SOURCES
    tests/db_test.cpp
)

add_executable(dbtest ${TEST_SOURCES})
target_link_libraries(dbtest lmdbcpp-static)
set_property(TARGET dbtest PROPERTY OUTPUT_NAME "db_test")

