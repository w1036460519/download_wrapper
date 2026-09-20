# aarch64-linux 交叉编译工具链（vcpkg chainload）
#
# 在 x64 Linux runner 上编译 arm64-linux 目标时使用。
# 通过 VCPKG_CHAINLOAD_TOOLCHAIN_FILE 传入 vcpkg，替代默认编译器检测。

set(CMAKE_SYSTEM_NAME Linux)
set(CMAKE_SYSTEM_PROCESSOR aarch64)

set(CMAKE_C_COMPILER aarch64-linux-gnu-gcc)
set(CMAKE_CXX_COMPILER aarch64-linux-gnu-g++)
