"""
依赖版本配置（单一版本源）

所有构建脚本和 CI workflow 都应从此文件读取版本号。
修改版本号后：
1. 触发对应的 build workflow 编译新版本
2. 新版本会上传到对应的 Release
3. build_wrapper.py 会自动下载新版本
"""

# libtorrent-rasterbar 版本
# Release tag: libtorrent-v{version}
# 示例: libtorrent-v2.1.2
LIBTORRENT_VERSION = "2.1.2"

# libwebrtc 版本（webrtc-sdk 发布标签）
# Release tag: libwebrtc-{version}
# 示例: libwebrtc-m144.7559.09
LIBWEBRTC_VERSION = "m144.7559.09"

# download_wrapper 自身版本
# Release tag: wrapper-v{version}
# 示例: wrapper-v0.0.1
WRAPPER_VERSION = "0.0.1"

# ── Release 配置 ──

def get_libtorrent_release_tag() -> str:
    """获取 libtorrent Release tag"""
    return f"libtorrent-v{LIBTORRENT_VERSION}"

def get_libwebrtc_release_tag() -> str:
    """获取 libwebrtc Release tag"""
    return f"libwebrtc-{LIBWEBRTC_VERSION}"

def get_wrapper_release_tag() -> str:
    """获取 wrapper Release tag"""
    return f"wrapper-v{WRAPPER_VERSION}"

def get_libtorrent_asset_name(platform: str) -> str:
    """获取 libtorrent asset 文件名"""
    return f"libtorrent-v{LIBTORRENT_VERSION}-{platform}.tar.gz"

def get_libwebrtc_asset_name(platform: str) -> str:
    """获取 libwebrtc asset 文件名"""
    return f"libwebrtc-{LIBWEBRTC_VERSION}-{platform}.tar.gz"

def get_wrapper_asset_name(platform: str) -> str:
    """获取 wrapper asset 文件名"""
    return f"wrapper-v{WRAPPER_VERSION}-{platform}.tar.gz"
