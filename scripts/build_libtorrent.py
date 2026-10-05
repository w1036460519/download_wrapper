#!/usr/bin/env python3
"""
从源码编译 libtorrent-rasterbar 静态库。

输出目录结构：
  {output_dir}/
    ├── include/        # libtorrent 头文件
    └── lib/
        └── libtorrent-rasterbar.a  # 静态库

用法：
  # 本地构建（使用 versions.py 中的版本）
  python3 scripts/build_libtorrent.py

  # CI 构建（指定输出目录）
  python3 scripts/build_libtorrent.py --platform macos --arch universal --output-dir /tmp/libtorrent-macos

  # 指定版本（覆盖 versions.py）
  python3 scripts/build_libtorrent.py --version 2.1.2
"""

import argparse
import os
import platform
import shutil
import subprocess
import sys
import tarfile
from pathlib import Path

# Windows 默认 cp1252 无法输出中文，强制 UTF-8
if hasattr(sys.stdout, 'reconfigure'):
    sys.stdout.reconfigure(encoding='utf-8', errors='replace')
    sys.stderr.reconfigure(encoding='utf-8', errors='replace')

# ── 常量 ──
SCRIPT_DIR = Path(__file__).parent
PROJECT_DIR = SCRIPT_DIR.parent
DEFAULT_BUILD_DIR = PROJECT_DIR / "build-libtorrent"

# 从 versions.py 读取版本
sys.path.insert(0, str(SCRIPT_DIR))
from versions import LIBTORRENT_VERSION
DEFAULT_VERSION = LIBTORRENT_VERSION

# vcpkg 路径（resolve 确保绝对路径，避免 Path("") 退化为相对路径）
_vcpkg_root = os.environ.get("VCPKG_ROOT", "")
if _vcpkg_root:
    VCPKG_DIR = Path(_vcpkg_root).resolve()
else:
    # 本地开发默认路径
    VCPKG_DIR = (Path.home() / "Documents" / "code" / "cpp" / "vcpkg").resolve()


def get_platform_arch():
    """检测当前平台和架构"""
    system = platform.system().lower()
    machine = platform.machine().lower()
    
    if system == "darwin":
        plat = "macos"
    elif system == "linux":
        plat = "linux"
    elif system == "windows":
        plat = "windows"
    else:
        raise RuntimeError(f"不支持的平台: {system}")
    
    if machine in ("arm64", "aarch64"):
        arch = "arm64"
    elif machine in ("x86_64", "amd64"):
        arch = "x64"
    else:
        arch = machine
    
    return plat, arch


def run(cmd, cwd=None, check=True, env=None):
    """执行命令"""
    print(f"\n>>> {' '.join(str(c) for c in cmd)}")
    result = subprocess.run(cmd, cwd=cwd, env=env)
    if check and result.returncode != 0:
        raise RuntimeError(f"命令失败: {cmd}")
    return result.returncode


def build_libtorrent(version: str, plat: str, arch: str, output_dir: Path, 
                     clean: bool = False, vcpkg_triplet: str = None):
    """编译 libtorrent"""
    src_dir = output_dir / "src"
    build_subdir = output_dir / "build"
    
    if clean and output_dir.exists():
        print(f"清理目录: {output_dir}")
        shutil.rmtree(output_dir)
    
    output_dir.mkdir(parents=True, exist_ok=True)
    
    # 克隆或更新源码
    if not src_dir.exists():
        print(f"克隆 libtorrent {version}...")
        run([
            "git", "clone",
            "--depth", "1",
            "--branch", version,
            "--recurse-submodules",
            "--shallow-submodules",
            "https://github.com/arvidn/libtorrent.git",
            str(src_dir)
        ])
    else:
        print(f"源码已存在: {src_dir}")
        # 确保 submodules 已初始化
        run(["git", "submodule", "update", "--init", "--recursive"], cwd=src_dir, check=False)
    
    build_subdir.mkdir(exist_ok=True)
    
    # vcpkg toolchain
    toolchain = VCPKG_DIR / "scripts" / "buildsystems" / "vcpkg.cmake"
    if not toolchain.exists():
        raise RuntimeError(f"vcpkg toolchain 不存在: {toolchain}\n请设置 VCPKG_ROOT 环境变量")
    
    # 创建临时 vcpkg.json 声明依赖
    vcpkg_json = build_subdir / "vcpkg.json"
    vcpkg_json.write_text('''{
  "name": "libtorrent-build",
  "version": "1.0.0",
  "dependencies": [
    "openssl",
    "boost-asio",
    "boost-beast",
    "boost-url",
    "boost-json",
    "boost-system",
    "boost-random",
    "boost-crc",
    "boost-multi-index",
    "boost-variant",
    "boost-iterator",
    "boost-scope-exit",
    "boost-chrono",
    "boost-date-time",
    "boost-pool",
    "boost-multiprecision"
  ]
}
''')
    print(f"已创建 vcpkg.json: {vcpkg_json}")
    
    # CMake 配置
    # 参考: https://libtorrent.org/building.html
    print("\n=== CMake 配置 ===")
    
    # 构建环境（传递 vcpkg triplet）
    build_env = os.environ.copy()
    if vcpkg_triplet:
        build_env["VCPKG_DEFAULT_TRIPLET"] = vcpkg_triplet
        print(f"使用 vcpkg triplet: {vcpkg_triplet}")
    
    cmake_args = [
        "cmake", "-S", str(src_dir), "-B", str(build_subdir),
        f"-DCMAKE_TOOLCHAIN_FILE={toolchain}",
        # vcpkg.json 在 build 目录，需显式指定（toolchain 默认在 source 目录找）
        f"-DVCPKG_MANIFEST_DIR={build_subdir}",
        "-DCMAKE_BUILD_TYPE=Release",
        "-DCMAKE_CXX_STANDARD=20",
        # 库类型
        "-DBUILD_SHARED_LIBS=OFF",        # 静态库
        "-DCMAKE_POSITION_INDEPENDENT_CODE=ON",  # -fPIC（静态库需链接进 .so）
        "-DBUILD_TESTING=OFF",
        "-DBUILD_TOOLS=OFF",              # 不需要独立工具程序
        "-DBUILD_EXAMPLES=OFF",
        # 核心功能（必须）
        "-Dencryption=ON",                # TLS + 混淆协议
        "-Ddht=ON",                       # DHT 无追踪器支持
        "-Dlogging=ON",                   # 日志警报
        "-Dextensions=ON",                # BT 协议扩展 (BEP 10)
        "-Dstreaming=ON",                 # set_piece_deadline (边下边播)
        "-Dmmap-disk-io=ON",              # mmap 磁盘 IO
        "-Ddeprecated-functions=ON",      # 兼容旧 API
        # 做种功能（保留以支持分享）
        "-Dsuper-seeding=ON",             # 超级做种（首次做种优化）
        "-Dshare-mode=ON",                # 分享模式（最大化分享率）
        "-Dpredictive-pieces=ON",         # 预测分片通告
        # 不需要的功能
        "-Di2p=OFF",                      # I2P 匿名网络
        "-Dwebtorrent=OFF",               # WebTorrent（需要 libdatachannel）
    ]
    
    # 平台特定配置
    if plat == "macos":
        cmake_args.extend([
            "-DCMAKE_OSX_DEPLOYMENT_TARGET=14.0",
        ])
        if arch == "universal":
            cmake_args.extend(["-DCMAKE_OSX_ARCHITECTURES=arm64;x86_64"])
        elif arch == "arm64":
            cmake_args.extend(["-DCMAKE_OSX_ARCHITECTURES=arm64"])
        elif arch == "x64":
            cmake_args.extend(["-DCMAKE_OSX_ARCHITECTURES=x86_64"])
    
    elif plat == "ios":
        # 关键：必须显式指定 OSX_SYSROOT。
        # CMAKE_SYSTEM_NAME=iOS 默认用 iphoneos SDK；universal 若不指定 sim SDK，
        # arm64 会以真机平台编译（platform=IOS），与 x86_64 的模拟器平台混在同一份
        # 产物里，宿主 App 链接时报 "built for 'iOS'" 平台不匹配错误。
        if arch == "arm64":
            # 真机：iphoneos SDK
            cmake_args.extend([
                "-DCMAKE_SYSTEM_NAME=iOS",
                "-DCMAKE_OSX_SYSROOT=iphoneos",
                "-DCMAKE_OSX_ARCHITECTURES=arm64",
                "-DCMAKE_OSX_DEPLOYMENT_TARGET=16.0",
            ])
        elif arch == "universal":
            # iOS 模拟器 universal：双架构都用 iphonesimulator SDK
            cmake_args.extend([
                "-DCMAKE_SYSTEM_NAME=iOS",
                "-DCMAKE_OSX_SYSROOT=iphonesimulator",
                "-DCMAKE_OSX_ARCHITECTURES=arm64;x86_64",
                "-DCMAKE_OSX_DEPLOYMENT_TARGET=16.0",
            ])
    
    elif plat == "android":
        ndk = os.environ.get("ANDROID_NDK", os.environ.get("ANDROID_NDK_HOME", ""))
        if not ndk:
            raise RuntimeError("Android 构建需要设置 ANDROID_NDK 环境变量")
        cmake_args.extend([
            f"-DCMAKE_ANDROID_NDK={ndk}",
            "-DCMAKE_SYSTEM_NAME=Android",
            "-DCMAKE_ANDROID_STL_TYPE=c++_static",
        ])
        if arch == "arm64":
            cmake_args.extend(["-DCMAKE_ANDROID_ARCH_ABI=arm64-v8a"])
            cmake_args.extend(["-DANDROID_ABI=arm64-v8a"])
        elif arch == "x64":
            cmake_args.extend(["-DCMAKE_ANDROID_ARCH_ABI=x86_64"])
            cmake_args.extend(["-DANDROID_ABI=x86_64"])
    
    elif plat == "linux":
        if arch == "arm64":
            cmake_args.extend([
                "-DCMAKE_C_COMPILER=aarch64-linux-gnu-gcc",
                "-DCMAKE_CXX_COMPILER=aarch64-linux-gnu-g++",
            ])
    
    elif plat == "windows":
        if arch == "arm64":
            cmake_args.extend(["-A", "ARM64"])
        elif arch == "x64":
            cmake_args.extend(["-A", "x64"])
    
    run(cmake_args, env=build_env)
    
    # 编译
    print("\n=== 编译 ===")
    cpu_count = os.cpu_count() or 4
    run([
        "cmake", "--build", str(build_subdir),
        "--config", "Release",
        "-j", str(cpu_count)
    ], env=build_env)
    
    # 复制产物
    print("\n=== 复制产物 ===")
    include_dir = output_dir / "include"
    lib_dir = output_dir / "lib"
    
    # 头文件
    if include_dir.exists():
        shutil.rmtree(include_dir)
    shutil.copytree(src_dir / "include", include_dir)
    
    # 静态库
    lib_dir.mkdir(exist_ok=True)
    
    if plat == "windows":
        src_lib = build_subdir / "Release" / "torrent-rasterbar.lib"
    else:
        src_lib = build_subdir / "libtorrent-rasterbar.a"
    
    if src_lib.exists():
        dst_lib = lib_dir / src_lib.name
        shutil.copy2(src_lib, dst_lib)
        print(f"✓ 库文件: {dst_lib}")
    else:
        raise RuntimeError(f"找不到库文件: {src_lib}")
    
    print(f"\n✅ 构建完成: {output_dir}")
    print(f"   头文件: {include_dir}")
    print(f"   库文件: {lib_dir}")


def pack_release(output_dir: Path, version: str, plat: str, arch: str,
                 tag: str | None = None) -> Path:
    """将构建产物打包为 Release tar.gz。

    tag: 统一平台标识（优先）；缺省时由 plat/arch 推导
    """
    # 确定平台标识
    platform_tag = tag or (plat if arch == "universal" else f"{plat}-{arch}")
    
    # 文件名: libtorrent-v2.1.2-macos-universal.tar.gz
    asset_name = f"libtorrent-v{version}-{platform_tag}.tar.gz"
    asset_path = output_dir.parent / asset_name
    
    print(f"\n=== 打包 Release: {asset_name} ===")
    
    with tarfile.open(asset_path, "w:gz") as tar:
        # 添加 include/
        include_dir = output_dir / "include"
        if include_dir.exists():
            tar.add(include_dir, arcname="include")
        
        # 添加 lib/
        lib_dir = output_dir / "lib"
        if lib_dir.exists():
            tar.add(lib_dir, arcname="lib")
    
    print(f"✓ 打包完成: {asset_path}")
    print(f"  大小: {asset_path.stat().st_size / 1024 / 1024:.1f} MB")
    
    return asset_path


def main():
    parser = argparse.ArgumentParser(description="编译 libtorrent-rasterbar 静态库")
    parser.add_argument("--version", default=DEFAULT_VERSION, 
                        help=f"libtorrent 版本 (默认: {DEFAULT_VERSION})")
    parser.add_argument("--platform", help="目标平台 (macos/linux/windows/android/ios)")
    parser.add_argument("--arch", help="目标架构 (arm64/x64/universal)")
    parser.add_argument("--output-dir", type=Path, 
                        help="输出目录 (默认: build-libtorrent/{platform}-{arch})")
    parser.add_argument("--vcpkg-triplet", 
                        help="vcpkg triplet (如 arm64-osx, x64-linux)")
    parser.add_argument("--pack", action="store_true", 
                        help="打包为 Release tar.gz")
    parser.add_argument("--tag", help="统一平台标识（用于 Release 资产命名）")
    parser.add_argument("--clean", action="store_true", help="清理后重新构建")
    args = parser.parse_args()
    
    # 检测平台和架构
    if args.platform and args.arch:
        plat, arch = args.platform, args.arch
    else:
        plat, arch = get_platform_arch()
        print(f"检测到平台: {plat}-{arch}")
    
    # 确定输出目录
    if args.output_dir:
        output_dir = args.output_dir
    else:
        output_dir = DEFAULT_BUILD_DIR / f"{plat}-{arch}"
    
    # 版本号处理：git clone 需要 v2.1.2 格式
    git_version = args.version
    if not git_version.startswith("v"):
        git_version = f"v{git_version}"
    
    try:
        build_libtorrent(
            version=git_version,
            plat=plat,
            arch=arch,
            output_dir=output_dir,
            clean=args.clean,
            vcpkg_triplet=args.vcpkg_triplet,
        )
        
        # 打包 Release
        if args.pack:
            pack_release(output_dir, args.version, plat, arch, tag=args.tag)
            
    except Exception as e:
        print(f"\n❌ 构建失败: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
