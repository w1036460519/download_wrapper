# Android x86_64 triplet
# 覆盖 vcpkg 内置 x64-android，仅修改项目必需的设置：
# - VCPKG_LIBRARY_LINKAGE: dynamic（FFI 需要 .so）
# - VCPKG_CMAKE_CONFIGURE_OPTIONS: 传递 ANDROID_ABI 给 cmake-get-vars probe
# 其余设置与 vcpkg 内置 triplet 保持一致。
set(VCPKG_TARGET_ARCHITECTURE x64)
set(VCPKG_CRT_LINKAGE dynamic)
set(VCPKG_LIBRARY_LINKAGE dynamic)
set(VCPKG_CMAKE_SYSTEM_NAME Android)
set(VCPKG_CMAKE_SYSTEM_VERSION 28)
set(VCPKG_MAKE_BUILD_TRIPLET "--host=x86_64-linux-android")
set(VCPKG_CMAKE_CONFIGURE_OPTIONS -DANDROID_ABI=x86_64)

# 仅构建 Release：分发产物不消费 Debug 库，跳过多余的 Debug 构建
set(VCPKG_BUILD_TYPE release)
