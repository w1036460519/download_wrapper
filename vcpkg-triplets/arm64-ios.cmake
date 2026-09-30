# iOS 真机 arm64 triplet
set(VCPKG_TARGET_ARCHITECTURE arm64)
set(VCPKG_CRT_LINKAGE dynamic)
set(VCPKG_LIBRARY_LINKAGE static)

set(VCPKG_CMAKE_SYSTEM_NAME iOS)
set(VCPKG_OSX_SYSROOT iphoneos)
set(VCPKG_OSX_ARCHITECTURES arm64)
set(VCPKG_OSX_DEPLOYMENT_TARGET "17.0")

# 不强制 -std：Apple clang（≥15）下 -std=c99 会关闭 POSIX 特性宏，隐藏
# libSystem 头文件中的函数声明，触发 openssl 等端口隐式声明硬错误。
# std 由各端口自管，与 vcpkg 内置 iOS triplet 一致（iOS 代码默认 PIC，无需额外 flag）。

# 仅构建 Release：分发产物不消费 Debug 库，跳过多余的 Debug 构建
set(VCPKG_BUILD_TYPE release)
