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
find_package(LibtorrentRasterbar CONFIG REQUIRED)
find_package(Boost REQUIRED COMPONENTS url json)

# P2P 打洞库（libwebrtc）：多端共享基础。通过 GitHub Release 预编译二进制提供，
# 不走 vcpkg。开关关闭时跳过 P2P 模块编译，仅影响 dw_p2p_* 系列 C API
# 的链接可用性，下载主流程不受影响。
option(DW_ENABLE_P2P "启用 P2P 多端共享模块（libwebrtc）" ON)
if(DW_ENABLE_P2P)
    # libwebrtc 二进制解压目录由 CI 或手动指定，需含 include/ + lib/
    if(DEFINED ENV{DW_LIBWEBRTC_DIR} AND NOT DEFINED DW_LIBWEBRTC_DIR)
        set(DW_LIBWEBRTC_DIR "$ENV{DW_LIBWEBRTC_DIR}")
    endif()
    if(DEFINED DW_LIBWEBRTC_DIR)
        if(EXISTS "${DW_LIBWEBRTC_DIR}/include/libwebrtc.h"
           AND EXISTS "${DW_LIBWEBRTC_DIR}/lib")
            set(DW_P2P_AVAILABLE ON)
            message(STATUS "P2P 模块: libwebrtc 已找到 (${DW_LIBWEBRTC_DIR})，启用 dw_p2p_* 接口")
        else()
            set(DW_P2P_AVAILABLE OFF)
            message(WARNING "P2P 模块: DW_LIBWEBRTC_DIR 指向无效目录: ${DW_LIBWEBRTC_DIR}")
        endif()
    else()
        set(DW_P2P_AVAILABLE OFF)
        message(STATUS "P2P: DW_LIBWEBRTC_DIR 未指定，跳过 src/p2p/ 编译。"
                       "CI 会自动下载 libwebrtc 二进制；本地构建需手动指定。")
    endif()
endif()

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

# P2P 未启用时剔除 src/p2p/ 下的源文件，避免链接期找不到 libwebrtc 符号
if(NOT DW_P2P_AVAILABLE)
    list(FILTER DW_SOURCES EXCLUDE REGEX "/p2p/")
endif()

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

target_link_libraries(download_wrapper PRIVATE
    CURL::libcurl
    Threads::Threads
    LibtorrentRasterbar::torrent-rasterbar
    unofficial::sqlite3::sqlite3
    Boost::url
    Boost::json
)

# P2P 模块链接：libwebrtc 通过 DW_LIBWEBRTC_DIR 指定预编译二进制
if(DW_P2P_AVAILABLE)
    target_include_directories(download_wrapper PRIVATE
        "${DW_LIBWEBRTC_DIR}/include")
    if(APPLE)
        # xcframework 切片：优先找 framework 内的 WebRTC 二进制
        find_library(LIBWEBRTC_LIB WebRTC
            PATHS "${DW_LIBWEBRTC_DIR}/lib"
            NO_DEFAULT_PATH)
        if(NOT LIBWEBRTC_LIB)
            # 备选：framework bundle 结构
            find_library(LIBWEBRTC_LIB WebRTC
                PATHS "${DW_LIBWEBRTC_DIR}/WebRTC.framework"
                NO_DEFAULT_PATH)
        endif()
    elseif(WIN32)
        set(LIBWEBRTC_LIB "${DW_LIBWEBRTC_DIR}/lib/webrtc.lib")
        if(NOT EXISTS "${LIBWEBRTC_LIB}")
            # 备选名称
            file(GLOB LIBWEBRTC_LIB "${DW_LIBWEBRTC_DIR}/lib/*.lib")
        endif()
    else()
        find_library(LIBWEBRTC_LIB webrtc
            PATHS "${DW_LIBWEBRTC_DIR}/lib"
            NO_DEFAULT_PATH)
        if(NOT LIBWEBRTC_LIB)
            find_library(LIBWEBRTC_LIB libwebrtc
                PATHS "${DW_LIBWEBRTC_DIR}/lib"
                NO_DEFAULT_PATH)
        endif()
    endif()
    if(LIBWEBRTC_LIB)
        target_link_libraries(download_wrapper PRIVATE ${LIBWEBRTC_LIB})
        message(STATUS "P2P: 链接 libwebrtc → ${LIBWEBRTC_LIB}")
    else()
        message(WARNING "P2P: 未找到 libwebrtc 库文件，P2P 接口将不可用")
        set(DW_P2P_AVAILABLE OFF)
    endif()
    if(DW_P2P_AVAILABLE)
        target_compile_definitions(download_wrapper PRIVATE DW_P2P_ENABLED=1)
        # webrtc-sdk C++ 封装实现需随项目编译（预编译二进制仅含 Google WebRTC 核心）。
        # src/ 目录下包含 ~33 个 .cc 文件 + base/portable.cc，它们实现
        # libwebrtc.h / rtc_peerconnection.h 等接口到 Google WebRTC 的桥接。
        if(EXISTS "${DW_LIBWEBRTC_DIR}/src")
            file(GLOB LIBWEBRTC_SOURCES "${DW_LIBWEBRTC_DIR}/src/*.cc")
            if(EXISTS "${DW_LIBWEBRTC_DIR}/src/base/portable.cc")
                list(APPEND LIBWEBRTC_SOURCES "${DW_LIBWEBRTC_DIR}/src/base/portable.cc")
            endif()
            if(LIBWEBRTC_SOURCES)
                target_sources(download_wrapper PRIVATE ${LIBWEBRTC_SOURCES})
                # 封装实现以 "include/xxx.h" 路径引用头文件
                set_source_files_properties(${LIBWEBRTC_SOURCES}
                    PROPERTIES INCLUDE_DIRECTORIES "${DW_LIBWEBRTC_DIR}")
                message(STATUS "P2P: 编译 libwebrtc 封装层 (${CMAKE_CURRENT_LIST_DIR}/src/)")
            endif()
        endif()
    endif()
endif()

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
