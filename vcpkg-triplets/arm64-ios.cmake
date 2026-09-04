# iOS 真机 arm64 triplet
set(VCPKG_TARGET_ARCHITECTURE arm64)
set(VCPKG_CRT_LINKAGE dynamic)
set(VCPKG_LIBRARY_LINKAGE static)

set(VCPKG_CMAKE_SYSTEM_NAME iOS)
set(VCPKG_OSX_SYSROOT iphoneos)
set(VCPKG_OSX_ARCHITECTURES arm64)
set(VCPKG_OSX_DEPLOYMENT_TARGET "16.3")

# C++20 支持（std::jthread / std::stop_token 需要）
set(VCPKG_C_FLAGS "-std=c99")
set(VCPKG_CXX_FLAGS "-std=c++20")
