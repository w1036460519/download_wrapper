cmake_minimum_required(VERSION 4.3)
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
find_package(LibtorrentRasterbar CONFIG REQUIRED)
find_package(Boost REQUIRED COMPONENTS url json)

if(WIN32)
    # 阻止 windows.h 宏污染 + 裁剪头文件 + 消除 Boost/ASIO 版本推断警告
    add_compile_definitions(NOMINMAX WIN32_LEAN_AND_MEAN _WIN32_WINNT=0x0601)
endif()

if(APPLE)
    if(NOT CMAKE_OSX_DEPLOYMENT_TARGET)
        # std::jthread/stop_token 需 iOS 17+；std::format chrono 需 macOS 14+
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
if(CMAKE_SYSTEM_NAME STREQUAL "iOS" AND CMAKE_OSX_SYSROOT MATCHES "iphonesimulator")
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

target_link_libraries(download_wrapper PRIVATE
    CURL::libcurl
    Threads::Threads
    LibtorrentRasterbar::torrent-rasterbar
    unofficial::sqlite3::sqlite3
    Boost::url
    Boost::json
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
