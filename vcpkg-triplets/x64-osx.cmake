# 覆盖 vcpkg 默认 x64-osx triplet：在默认配置基础上追加 macOS 最低部署目标 12.0。
# 语义同 arm64-osx，见该文件注释。
set(VCPKG_TARGET_ARCHITECTURE x64)
set(VCPKG_CRT_LINKAGE dynamic)
set(VCPKG_LIBRARY_LINKAGE static)

set(VCPKG_CMAKE_SYSTEM_NAME Darwin)
set(VCPKG_OSX_ARCHITECTURES x86_64)
set(VCPKG_OSX_DEPLOYMENT_TARGET "12.0")
