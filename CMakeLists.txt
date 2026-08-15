cmake_minimum_required(VERSION 3.20)
project(download_wrapper LANGUAGES CXX C)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

# 平台导出宏
set(CMAKE_CXX_VISIBILITY_PRESET hidden)
set(CMAKE_VISIBILITY_INLINES_HIDDEN ON)

# 依赖统一通过 vcpkg toolchain 提供：
#   构建时传入 -DCMAKE_TOOLCHAIN_FILE=$VCPKG_ROOT/scripts/buildsystems/vcpkg.cmake
# CLion 等 IDE 需在 CMake 选项中配置该 toolchain 文件（或设置 VCPKG_ROOT 环境变量）。

# 查找依赖
find_package(CURL REQUIRED)
find_package(Threads REQUIRED)
find_package(unofficial-sqlite3 CONFIG REQUIRED)

# libtorrent 提供 CMake config 包，优先使用 find_package
# Boost.Beast / Boost.Asio 为 playback_proxy 直接依赖（header-only），
# Boost.URL 为 URL 解析库（需链接），经 vcpkg 安装后通过 libtorrent 依赖链可用。
# Boost.date_time 为 time_util 跨平台时间格式化依赖。
find_package(LibtorrentRasterbar CONFIG REQUIRED)
find_package(Boost REQUIRED COMPONENTS url json date_time)

if(WIN32)
    # NOMINMAX：阻止 windows.h 定义 min/max 宏（经 curl 头传入），否则会撕碎
    # libtorrent aux_/aligned_union.hpp 中的 max 模板导致 MSVC 语法错误；
    # WIN32_LEAN_AND_MEAN：裁剪 windows.h，减少宏污染与编译时间；
    # _WIN32_WINNT=0x0601：目标 Windows 7+，消除 Boost/ASIO 的版本推断警告。
    add_compile_definitions(NOMINMAX WIN32_LEAN_AND_MEAN _WIN32_WINNT=0x0601)
endif()

if(APPLE)
    if(NOT CMAKE_OSX_DEPLOYMENT_TARGET)
        if(CMAKE_SYSTEM_NAME STREQUAL "iOS")
            set(CMAKE_OSX_DEPLOYMENT_TARGET "13" CACHE STRING "最低 iOS 版本")
        else()
            set(CMAKE_OSX_DEPLOYMENT_TARGET "12" CACHE STRING "最低 macOS 版本")
        endif()
    endif()
    # 目标架构由外部传入：macOS CI 分架构编译后用 lipo 合并为 universal binary；
    # iOS 传入 arm64。本地不指定时默认构建当前主机架构。
endif()

# 源文件
set(DW_SOURCES
    src/download_wrapper.cpp
    src/core/task_manager.cpp
    src/core/task_store.cpp
    src/http/http_engine.cpp
    src/http/http_engine_impl.cpp
    src/torrent/torrent_engine.cpp
    src/playback/playback_proxy.cpp
    src/utils/string_util.cpp
    src/utils/time_util.cpp
    src/utils/unique_name.cpp
)

# 库类型开关：桌面端 / Android 构建动态库（.dylib/.so/.dll）；
# iOS 不允许加载未签名独立动态库，改构建静态库（.a）随 Runner 链接。
option(DW_BUILD_SHARED "构建为动态库（OFF 时构建静态库，供 iOS 链接）" ON)
if(DW_BUILD_SHARED)
    add_library(download_wrapper SHARED ${DW_SOURCES})
else()
    add_library(download_wrapper STATIC ${DW_SOURCES})
endif()

target_include_directories(download_wrapper
    PUBLIC
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
        $<INSTALL_INTERFACE:include>
    PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}/src
)

target_link_libraries(download_wrapper
    PRIVATE
        CURL::libcurl
        Threads::Threads
        LibtorrentRasterbar::torrent-rasterbar
        unofficial::sqlite3::sqlite3
        Boost::url
        Boost::json
        Boost::date_time
)

target_compile_definitions(download_wrapper PRIVATE DOWNLOAD_WRAPPER_BUILD)

# 安装规则
install(TARGETS download_wrapper
    EXPORT download_wrapper-targets
    RUNTIME DESTINATION bin
    LIBRARY DESTINATION lib
    ARCHIVE DESTINATION lib
)

install(DIRECTORY include/ DESTINATION include)
