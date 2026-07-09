cmake_minimum_required(VERSION 3.20)
project(download_wrapper LANGUAGES CXX C)

set(CMAKE_CXX_STANDARD 20)
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

# libtorrent 提供 CMake config 包，优先使用 find_package
find_package(LibtorrentRasterbar CONFIG REQUIRED)

if(APPLE)
    if(NOT CMAKE_OSX_DEPLOYMENT_TARGET)
        set(CMAKE_OSX_DEPLOYMENT_TARGET "12" CACHE STRING "Minimum macOS version")
    endif()
    # 目标架构由外部传入（CI 分架构编译后用 lipo 合并为 universal binary）；
    # 本地不指定时默认构建当前主机架构。
endif()

# 源文件
set(DW_SOURCES
    src/download_wrapper.cpp
    src/http/http_engine.cpp
    src/http/http_engine_impl.cpp
    src/torrent/torrent_engine.cpp
)

add_library(download_wrapper SHARED ${DW_SOURCES})

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
