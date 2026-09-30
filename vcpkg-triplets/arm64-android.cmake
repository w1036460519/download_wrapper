# Android arm64-v8a triplet
set(VCPKG_TARGET_ARCHITECTURE arm64)
set(VCPKG_CRT_LINKAGE dynamic)
set(VCPKG_LIBRARY_LINKAGE dynamic)

set(VCPKG_CMAKE_SYSTEM_NAME Android)
set(VCPKG_CMAKE_SYSTEM_VERSION 28)

# 共享库需 -fPIC（Android 构建 .so 必须）
set(VCPKG_C_FLAGS "-std=c99 -fPIC")
set(VCPKG_CXX_FLAGS "-std=c++20 -fPIC")

# 仅构建 Release：分发产物不消费 Debug 库，跳过多余的 Debug 构建
set(VCPKG_BUILD_TYPE release)
