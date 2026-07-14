# 覆盖 vcpkg 默认 arm64-osx triplet：在默认配置基础上追加 macOS 最低部署目标 12.0。
#
# 默认 osx triplet 不设置 VCPKG_OSX_DEPLOYMENT_TARGET，依赖会按构建机 SDK 默认版本
# （macos-14 runner 上为 14.x）编译，导致最终 libdownload_wrapper.dylib 的 minos 被抬到
# 14.0，App（目标 12.0）链接时报 "built for newer version 14.0" 告警且在 macOS 12/13
# 上存在运行风险。此处将依赖与主库、App 统一锁定到 12.0。
set(VCPKG_TARGET_ARCHITECTURE arm64)
set(VCPKG_CRT_LINKAGE dynamic)
set(VCPKG_LIBRARY_LINKAGE static)

set(VCPKG_CMAKE_SYSTEM_NAME Darwin)
set(VCPKG_OSX_ARCHITECTURES arm64)
set(VCPKG_OSX_DEPLOYMENT_TARGET "12.0")
