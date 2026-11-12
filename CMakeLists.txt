cmake_minimum_required(VERSION 3.24)
project(download_wrapper LANGUAGES CXX C)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

# 符号可见性
set(CMAKE_CXX_VISIBILITY_PRESET hidden)
set(CMAKE_VISIBILITY_INLINES_HIDDEN ON)

# 依赖通过 vcpkg toolchain 提供（构建时传入 CMAKE_TOOLCHAIN_FILE）
find_package(CURL REQUIRED)
find_package(Threads REQUIRED)
find_package(unofficial-sqlite3 CONFIG REQUIRED)
find_package(Boost REQUIRED COMPONENTS url json system random)
find_package(OpenSSL CONFIG REQUIRED)  # libtorrent 需要（encryption 功能）

# libtorrent-rasterbar: 使用预编译二进制
# 版本由 scripts/versions.py 统一管理，build_wrapper.py 负责获取
# DW_LIBTORRENT_DIR 指向预编译目录（包含 include/ 和 lib/）
if(DEFINED ENV{DW_LIBTORRENT_DIR} AND NOT DEFINED DW_LIBTORRENT_DIR)
    set(DW_LIBTORRENT_DIR "$ENV{DW_LIBTORRENT_DIR}")
endif()

if(NOT DEFINED DW_LIBTORRENT_DIR)
    message(FATAL_ERROR "DW_LIBTORRENT_DIR 未指定\n"
        "请通过 build_wrapper.py 构建，或手动设置 DW_LIBTORRENT_DIR")
endif()

if(EXISTS "${DW_LIBTORRENT_DIR}/include/libtorrent/version.hpp"
   AND EXISTS "${DW_LIBTORRENT_DIR}/lib")
    message(STATUS "libtorrent: 使用预编译库 (${DW_LIBTORRENT_DIR})")
    add_library(torrent-rasterbar STATIC IMPORTED)
    if(WIN32)
        set_target_properties(torrent-rasterbar PROPERTIES
            IMPORTED_LOCATION "${DW_LIBTORRENT_DIR}/lib/torrent-rasterbar.lib")
    else()
        set_target_properties(torrent-rasterbar PROPERTIES
            IMPORTED_LOCATION "${DW_LIBTORRENT_DIR}/lib/libtorrent-rasterbar.a")
    endif()
    target_include_directories(torrent-rasterbar INTERFACE "${DW_LIBTORRENT_DIR}/include")
    # libtorrent 构建时启用了 OpenSSL（encryption），消费者需同步定义
    target_compile_definitions(torrent-rasterbar INTERFACE TORRENT_USE_OPENSSL=1)
else()
    message(FATAL_ERROR "DW_LIBTORRENT_DIR 指向无效目录: ${DW_LIBTORRENT_DIR}\n"
        "需要包含 include/libtorrent/version.hpp 和 lib/libtorrent-rasterbar.a")
endif()

if(WIN32)
    # 阻止 windows.h 宏污染 + 裁剪头文件 + 消除 Boost/ASIO 版本推断警告
    add_compile_definitions(NOMINMAX WIN32_LEAN_AND_MEAN _WIN32_WINNT=0x0601)
endif()

if(APPLE)
    if(NOT CMAKE_OSX_DEPLOYMENT_TARGET)
        # std::format chrono 需 macOS 14+；iOS 17+ 对齐
        if(CMAKE_SYSTEM_NAME STREQUAL "iOS")
            set(CMAKE_OSX_DEPLOYMENT_TARGET "17.0" CACHE STRING "")
        else()
            set(CMAKE_OSX_DEPLOYMENT_TARGET "14.0" CACHE STRING "")
        endif()
    endif()
endif()

# 源文件（自动发现，新增文件无需手动添加）
file(GLOB_RECURSE DW_SOURCES CONFIGURE_DEPENDS src/*.cpp)

# 库类型开关：桌面端/Android 构建动态库；iOS 构建静态库（.a）随 Runner 链接
option(DW_BUILD_SHARED "构建为动态库（OFF 时构建静态库，供 iOS 链接）" ON)
if(DW_BUILD_SHARED)
    add_library(download_wrapper SHARED ${DW_SOURCES})
else()
    add_library(download_wrapper STATIC ${DW_SOURCES})
endif()

# 平台 & 架构检测 → 产物命名 download-{platform}-{arch}
# 注意：project() 后 CMAKE_OSX_SYSROOT 已解析为完整路径（含大写 iPhoneSimulator），
# MATCHES 大小写敏感，需归一小写后再匹配，否则 iOS 模拟器会被误判为 ios 平台
string(TOLOWER "${CMAKE_OSX_SYSROOT}" DW_SYSROOT_LOWER)
if(CMAKE_SYSTEM_NAME STREQUAL "iOS" AND DW_SYSROOT_LOWER MATCHES "iphonesimulator")
    set(DW_PLATFORM "ios-simulator")
elseif(CMAKE_SYSTEM_NAME STREQUAL "Darwin")
    set(DW_PLATFORM "macos")
elseif(CMAKE_SYSTEM_NAME STREQUAL "Android")
    set(DW_PLATFORM "android")
else()
    string(TOLOWER "${CMAKE_SYSTEM_NAME}" DW_PLATFORM)
endif()

if(CMAKE_SYSTEM_NAME STREQUAL "Android")
    if(CMAKE_ANDROID_ARCH_ABI STREQUAL "arm64-v8a")
        set(DW_ARCH "arm64")
    elseif(CMAKE_ANDROID_ARCH_ABI STREQUAL "x86_64")
        set(DW_ARCH "x64")
    else()
        message(FATAL_ERROR "Unsupported Android ABI: ${CMAKE_ANDROID_ARCH_ABI}")
    endif()
elseif(WIN32 AND CMAKE_VS_PLATFORM_NAME)
    if(CMAKE_VS_PLATFORM_NAME MATCHES "ARM64")
        set(DW_ARCH "arm64")
    elseif(CMAKE_VS_PLATFORM_NAME MATCHES "x64")
        set(DW_ARCH "x64")
    elseif(CMAKE_VS_PLATFORM_NAME MATCHES "Win32")
        set(DW_ARCH "x86")
    else()
        set(DW_ARCH "${CMAKE_VS_PLATFORM_NAME}")
    endif()
elseif(CMAKE_OSX_ARCHITECTURES)
    list(LENGTH CMAKE_OSX_ARCHITECTURES DW_ARCH_COUNT)
    if(DW_ARCH_COUNT GREATER 1)
        set(DW_ARCH "universal")
    else()
        list(GET CMAKE_OSX_ARCHITECTURES 0 DW_ARCH)
    endif()
else()
    set(DW_ARCH "${CMAKE_SYSTEM_PROCESSOR}")
endif()
if(DW_ARCH STREQUAL "x86_64")
    set(DW_ARCH "x64")
elseif(DW_ARCH STREQUAL "aarch64")
    set(DW_ARCH "arm64")
endif()

set_target_properties(download_wrapper PROPERTIES
    OUTPUT_NAME "download-${DW_PLATFORM}-${DW_ARCH}"
    PREFIX ""  # 全平台统一无 lib 前缀
)

target_include_directories(download_wrapper
    PUBLIC
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
        $<INSTALL_INTERFACE:include>
    PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}/src
)
# 使用与预编译库配套的头文件，避免 vcpkg 中的另一版本优先被找到。
target_include_directories(download_wrapper SYSTEM BEFORE PRIVATE "${DW_LIBTORRENT_DIR}/include")

target_link_libraries(download_wrapper PRIVATE
    CURL::libcurl
    Threads::Threads
    torrent-rasterbar
    unofficial::sqlite3::sqlite3
    Boost::url
    Boost::json
    OpenSSL::SSL
    OpenSSL::Crypto
)

target_compile_definitions(download_wrapper PRIVATE DOWNLOAD_WRAPPER_BUILD)

target_compile_options(download_wrapper PRIVATE
    $<$<CXX_COMPILER_ID:GNU,Clang,AppleClang>:-Wall -Wextra -Wpedantic>
    $<$<CXX_COMPILER_ID:MSVC>:/W4>
)

# 安装规则
install(TARGETS download_wrapper EXPORT download_wrapper-targets
    RUNTIME DESTINATION bin
    LIBRARY DESTINATION lib
    ARCHIVE DESTINATION lib
)
install(DIRECTORY include/ DESTINATION include)

# 导出配置（供下游项目 find_package）
install(EXPORT download_wrapper-targets
    FILE download_wrapper-config.cmake
    NAMESPACE download_wrapper::
    DESTINATION lib/cmake/download_wrapper
)
