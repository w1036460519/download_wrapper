# Android x86_64 triplet
set(VCPKG_TARGET_ARCHITECTURE x64)
set(VCPKG_CRT_LINKAGE dynamic)
set(VCPKG_LIBRARY_LINKAGE dynamic)

set(VCPKG_CMAKE_SYSTEM_NAME Android)
set(VCPKG_CMAKE_SYSTEM_VERSION 28)

# 共享库需 -fPIC（Android 构建 .so 必须）
# 不强制 -std：NDK clang 18 下 -std=c99 会关闭 GNU/POSIX 特性宏，隐藏 bionic
# 头文件中的 POSIX 函数声明，触发 openssl 等端口隐式声明硬错误。std 由各端口
# 自管（vcpkg 社区 android triplet 同样不强制 -std）；此处额外保留 -fPIC。
set(VCPKG_C_FLAGS "-fPIC")
set(VCPKG_CXX_FLAGS "-fPIC")

# 仅构建 Release：分发产物不消费 Debug 库，跳过多余的 Debug 构建
set(VCPKG_BUILD_TYPE release)
