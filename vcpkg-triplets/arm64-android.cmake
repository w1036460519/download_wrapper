# 覆盖 vcpkg 内置 arm64-android triplet：显式锁定 Android API 级别为 21。
#
# 背景：依赖端口（curl/openssl/boost/libtorrent）若按 NDK 默认 API 编译
# （NDK 29 下默认 >= 28），会引用高版本 libc 符号：
#   aligned_alloc（API 28）、getifaddrs/freeifaddrs/__fread_chk/__fwrite_chk（API 24）、
#   stdout/stderr 真符号（API 23）、__poll_chk/__memchr_chk（API 23）、__sendto_chk（API 26）。
# 而主项目按 android-21 链接（对齐 Flutter minSdk），API 21 的 sysroot 缺这些符号，
# 触发 ld.lld "undefined symbol" 链接失败。
#
# VCPKG_CMAKE_SYSTEM_VERSION 会被翻译为所有 port 的
# --target=aarch64-linux-android21（对 CMake / b2 / perl-configure 各类构建系统均生效），
# 从而让依赖与主项目、与 Flutter minSdk 统一到 API 21，只引用 API 21 可用的符号。
set(VCPKG_TARGET_ARCHITECTURE arm64)
set(VCPKG_CRT_LINKAGE dynamic)
set(VCPKG_LIBRARY_LINKAGE static)
set(VCPKG_CMAKE_SYSTEM_NAME Android)
set(VCPKG_CMAKE_SYSTEM_VERSION 21)
