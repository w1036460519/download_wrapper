#!/usr/bin/env python3
"""
编译 download_wrapper 全平台产物。

统一处理 vcpkg 依赖安装、libwebrtc 预编译产物下载、CMake 配置/编译、
产物后处理（strip/lipo/libtool 合并），消除 CI YAML 中的 bash 字符串拼接。

用法：
  python3 scripts/build_wrapper.py --platform macos-universal
  python3 scripts/build_wrapper.py --platform linux-x64 --vcpkg-ref latest
"""

import argparse
import os
import platform
import shutil
import subprocess
import sys
import urllib.request
import zipfile
from pathlib import Path

# Windows 默认 cp1252 无法输出中文，强制 UTF-8
if hasattr(sys.stdout, 'reconfigure'):
    sys.stdout.reconfigure(encoding='utf-8', errors='replace')
    sys.stderr.reconfigure(encoding='utf-8', errors='replace')

# ── 常量 ──
LIBWEBRTC_RELEASE_TAG = "libwebrtc-v1"

# ── 平台配置 ──
# universal=True: 双架构编译后合并（macOS lipo / iOS libtool+lipo）
# static=True: 生成静态库（iOS）
# simulator=True: iOS 模拟器构建
PLATFORMS = {
    "macos-universal": {
        "os": "macos",
        "universal": True,
        "output": "download-macos-universal.dylib",
        "deploy_target": "14.0",
        "overlay_triplets": True,
    },
    "linux-x64": {
        "os": "linux",
        "triplet": "x64-linux",
        "output": "download-linux-x64.so",
    },
    "linux-arm64": {
        "os": "linux",
        "triplet": "arm64-linux",
        "output": "download-linux-arm64.so",
    },
    "windows-x64": {
        "os": "windows",
        "triplet": "x64-windows-static-md",
        "output": "download-windows-x64.dll",
    },
    "windows-arm64": {
        "os": "windows",
        "triplet": "arm64-windows-static-md",
        "output": "download-windows-arm64.dll",
        "cmake_arch": "ARM64",
    },
    "ios-arm64": {
        "os": "ios",
        "triplet": "arm64-ios",
        "static": True,
        "output": "download-ios-arm64.a",
        "overlay_triplets": True,
    },
    "ios-simulator-universal": {
        "os": "ios",
        "universal": True,
        "simulator": True,
        "output": "download-ios-simulator-universal.a",
        "overlay_triplets": True,
    },
    "android-arm64": {
        "os": "android",
        "triplet": "arm64-android",
        "abi": "arm64-v8a",
        "api": 28,
        "output": "download-android-arm64.so",
    },
    "android-x64": {
        "os": "android",
        "triplet": "x64-android",
        "abi": "x86_64",
        "api": 28,
        "output": "download-android-x64.so",
    },
}

# 平台名 → libwebrtc 产物 zip 名
LIBWEBRTC_ASSETS = {
    "macos-universal": "libwebrtc-macos-release.zip",
    "linux-x64": "libwebrtc-linux-x64-release.zip",
    "linux-arm64": "libwebrtc-linux-arm64-release.zip",
    "windows-x64": "libwebrtc-windows-x64-release.zip",
    "windows-arm64": "libwebrtc-windows-arm64-release.zip",
    "ios-arm64": "libwebrtc-ios-arm64-release.zip",
    "ios-simulator-universal": "libwebrtc-ios-simulator-release.zip",
    "android-arm64": "libwebrtc-android-arm64-release.zip",
    "android-x64": "libwebrtc-android-x64-release.zip",
}


def run(cmd, **kwargs):
    """执行命令，实时打印输出，失败时抛出异常。"""
    cmd_str = ' '.join(str(c) for c in cmd)
    print(f"\n$ {cmd_str}")
    process = subprocess.Popen(
        cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
        encoding='utf-8', errors='replace', bufsize=1, **kwargs
    )
    for line in process.stdout:
        print(line, end='')
    process.wait()
    if process.returncode != 0:
        print(f"\n[ERROR] 命令失败 (exit {process.returncode}): {cmd_str}")
        raise subprocess.CalledProcessError(process.returncode, cmd)


def run_capture(cmd, **kwargs):
    """执行命令并捕获输出。"""
    result = subprocess.run(cmd, capture_output=True, encoding='utf-8', errors='replace', **kwargs)
    return result


# ── Step 1: vcpkg ──

def setup_vcpkg(workspace: Path, vcpkg_ref: str, temp_dir: Path) -> Path:
    """克隆 vcpkg 并 bootstrap，返回 vcpkg 根目录。
    vcpkg_ref 为空或 'latest' 时跟踪 master 最新。"""
    vcpkg_dir = temp_dir / "vcpkg"
    if vcpkg_dir.exists():
        shutil.rmtree(vcpkg_dir)

    run(["git", "clone", "https://github.com/microsoft/vcpkg.git", str(vcpkg_dir)])
    if vcpkg_ref and vcpkg_ref != "latest":
        run(["git", "checkout", vcpkg_ref], cwd=str(vcpkg_dir))

    # bootstrap
    if platform.system() == "Windows":
        run([str(vcpkg_dir / "bootstrap-vcpkg.bat"), "-disableMetrics"])
    else:
        run([str(vcpkg_dir / "bootstrap-vcpkg.sh"), "-disableMetrics"])

    os.environ["VCPKG_ROOT"] = str(vcpkg_dir)
    return vcpkg_dir


# ── Step 2: libwebrtc 预编译产物 ──

def download_libwebrtc(workspace: Path, platform_name: str) -> Path | None:
    """从 GitHub Release 下载 libwebrtc 预编译产物，解压到 third_party/libwebrtc/。"""
    asset = LIBWEBRTC_ASSETS.get(platform_name)
    if not asset:
        print(f"跳过: 平台 {platform_name} 无 libwebrtc 产物")
        return None

    dest = workspace / "third_party" / "libwebrtc"
    dest.mkdir(parents=True, exist_ok=True)

    repo = os.environ.get("GITHUB_REPOSITORY", "")
    if not repo:
        # 本地测试时从 git remote 推断
        result = run_capture(["git", "remote", "get-url", "origin"], cwd=str(workspace))
        if result.returncode == 0:
            # 从 URL 提取 owner/repo
            url = result.stdout.strip()
            parts = url.rstrip("/").split("/")
            repo = f"{parts[-2]}/{parts[-1].replace('.git', '')}"

    url = f"https://github.com/{repo}/releases/download/{LIBWEBRTC_RELEASE_TAG}/{asset}"
    print(f"下载 libwebrtc: {url}")

    zip_path = workspace / "libwebrtc.zip"
    try:
        urllib.request.urlretrieve(url, zip_path)
    except Exception as e:
        print(f"警告: libwebrtc 产物下载失败 ({asset})，P2P 模块将跳过\n       {e}")
        if dest.exists():
            shutil.rmtree(dest)
        return None

    # 解压
    tmp_dir = workspace / "lw_tmp"
    if tmp_dir.exists():
        shutil.rmtree(tmp_dir)
    with zipfile.ZipFile(zip_path, 'r') as zf:
        zf.extractall(tmp_dir)

    # libwebrtc-pkg/ 顶层目录处理
    pkg_dir = tmp_dir / "libwebrtc-pkg"
    src_dir = pkg_dir if pkg_dir.exists() else tmp_dir
    if src_dir.exists():
        for item in src_dir.iterdir():
            shutil.move(str(item), str(dest / item.name))

    # 清理
    zip_path.unlink()
    if tmp_dir.exists():
        shutil.rmtree(tmp_dir)

    print(f"libwebrtc 文件数: include={sum(1 for _ in (dest / 'include').rglob('*') if _.is_file())}")
    return dest


# ── Step 3: CMake 构建 ──

def cmake_configure(build_dir: Path, workspace: Path, cfg: dict,
                    vcpkg_dir: Path, libwebrtc_dir: Path | None,
                    extra_args: list[str] = None):
    """构造并执行 cmake configure 命令。"""
    toolchain = vcpkg_dir / "scripts" / "buildsystems" / "vcpkg.cmake"

    cmd = [
        "cmake", "-B", str(build_dir),
        "-DCMAKE_BUILD_TYPE=Release",
        f"-DCMAKE_TOOLCHAIN_FILE={toolchain}",
    ]

    # vcpkg triplet
    triplet = cfg.get("triplet")
    if triplet:
        cmd.append(f"-DVCPKG_TARGET_TRIPLET={triplet}")

    # linux-arm64 交叉编译：注入 chainload 工具链（指定 aarch64 交叉编译器）
    if triplet == "arm64-linux":
        cross_tc = workspace / "cmake" / "aarch64-linux-toolchain.cmake"
        if cross_tc.exists():
            cmd.append(f"-DVCPKG_CHAINLOAD_TOOLCHAIN_FILE={cross_tc}")

    # overlay triplets（macOS/iOS 需要自定义部署目标）
    if cfg.get("overlay_triplets"):
        overlay = workspace / "vcpkg-triplets"
        if overlay.exists():
            cmd.append(f"-DVCPKG_OVERLAY_TRIPLETS={overlay}")

    # libwebrtc
    if libwebrtc_dir:
        cmd.append(f"-DDW_LIBWEBRTC_DIR={libwebrtc_dir}")

    # iOS 特殊处理
    if cfg.get("os") == "ios":
        cmd.append("-DDW_BUILD_SHARED=OFF")
        if cfg.get("simulator"):
            cmd.extend([
                "-DCMAKE_SYSTEM_NAME=iOS",
                "-DCMAKE_OSX_SYSROOT=iphonesimulator",
            ])
        else:
            cmd.extend([
                "-DCMAKE_SYSTEM_NAME=iOS",
                "-DCMAKE_OSX_SYSROOT=iphoneos",
            ])

    # macOS 部署目标
    if cfg.get("deploy_target"):
        cmd.append(f"-DCMAKE_OSX_DEPLOYMENT_TARGET={cfg['deploy_target']}")

    # Windows ARM64 架构
    if cfg.get("cmake_arch"):
        cmd.extend(["-A", cfg["cmake_arch"]])

    # Android NDK chainload
    if cfg.get("os") == "android":
        ndk = os.environ.get("ANDROID_NDK_HOME", os.environ.get("ANDROID_NDK_LATEST_HOME", ""))
        if ndk:
            ndk_toolchain = Path(ndk) / "build" / "cmake" / "android.toolchain.cmake"
            cmd.extend([
                f"-DVCPKG_CHAINLOAD_TOOLCHAIN_FILE={ndk_toolchain}",
                f"-DANDROID_ABI={cfg['abi']}",
                f"-DANDROID_PLATFORM=android-{cfg['api']}",
            ])

    if extra_args:
        cmd.extend(extra_args)

    run(cmd, cwd=str(workspace))


def cmake_build(build_dir: Path):
    """执行 cmake --build。"""
    run(["cmake", "--build", str(build_dir), "--config", "Release"])


def find_output(build_dir: Path, pattern: str) -> Path | None:
    """在构建目录中查找产物文件。"""
    matches = list(build_dir.rglob(pattern))
    return matches[0] if matches else None


def find_vcpkg_installed(workspace: Path, triplet: str) -> Path | None:
    """查找 vcpkg_installed 目录。"""
    matches = list(workspace.rglob(f"vcpkg_installed/{triplet}/lib"))
    return matches[0] if matches else None


# ── Step 4: 后处理 ──

def strip_binary(filepath: Path, platform_os: str):
    """strip 符号裁剪。"""
    if platform_os == "linux":
        run(["strip", "--strip-unneeded", str(filepath)])
    elif platform_os == "macos":
        run(["strip", "-x", str(filepath)])
    elif platform_os == "android":
        ndk = os.environ.get("ANDROID_NDK_HOME", os.environ.get("ANDROID_NDK_LATEST_HOME", ""))
        if ndk:
            strip_bin = next(Path(ndk).rglob("llvm-strip"), None)
            if strip_bin:
                run([str(strip_bin), "--strip-unneeded", str(filepath)])


def merge_universal_dylib(arm64_lib: Path, x64_lib: Path, output: Path):
    """lipo 合并 macOS universal binary。"""
    output.parent.mkdir(parents=True, exist_ok=True)
    run(["lipo", "-create", str(arm64_lib), str(x64_lib), "-output", str(output)])


def merge_ios_static_libs(workspace: Path, cfg: dict, vcpkg_dir: Path) -> Path:
    """合并 iOS 静态库（自身 + vcpkg 依赖）为自包含胖库。"""
    dist = workspace / "dist"
    dist.mkdir(parents=True, exist_ok=True)

    if cfg.get("universal") and cfg.get("simulator"):
        # iOS simulator universal: arm64 + x64 各合并依赖后再 lipo
        arm64_self = find_output(workspace / "build-sim-arm64", "download-ios-simulator-arm64.a")
        x64_self = find_output(workspace / "build-sim-x64", "download-ios-simulator-x64.a")
        arm64_deps = find_vcpkg_installed(workspace, "arm64-ios-simulator")
        x64_deps = find_vcpkg_installed(workspace, "x64-ios-simulator")

        temp_arm64 = Path(os.environ.get("RUNNER_TEMP", "/tmp")) / "sim-arm64.a"
        temp_x64 = Path(os.environ.get("RUNNER_TEMP", "/tmp")) / "sim-x64.a"

        run(["libtool", "-static", "-o", str(temp_arm64), str(arm64_self)] +
            [str(f) for f in arm64_deps.glob("*.a")])
        run(["libtool", "-static", "-o", str(temp_x64), str(x64_self)] +
            [str(f) for f in x64_deps.glob("*.a")])

        output = dist / cfg["output"]
        run(["lipo", "-create", str(temp_arm64), str(temp_x64), "-output", str(output)])
        return output
    else:
        # iOS arm64 单架构
        self_lib = find_output(workspace / "build", "download-ios-arm64.a")
        deps = find_vcpkg_installed(workspace, "arm64-ios")

        output = dist / cfg["output"]
        run(["libtool", "-static", "-o", str(output), str(self_lib)] +
            [str(f) for f in deps.glob("*.a")])
        return output


# ── 主流程 ──

def build_single_arch(workspace: Path, cfg: dict, vcpkg_dir: Path,
                      libwebrtc_dir: Path | None, build_name: str):
    """单架构构建（Linux/Windows/iOS-arm64/Android）。"""
    build_dir = workspace / "build"
    cmake_configure(build_dir, workspace, cfg, vcpkg_dir, libwebrtc_dir)
    cmake_build(build_dir)

    # 查找并复制产物
    output = find_output(build_dir, cfg["output"])
    if not output:
        raise FileNotFoundError(f"未找到产物: {cfg['output']}")

    dist = workspace / "dist"
    dist.mkdir(parents=True, exist_ok=True)
    shutil.copy2(output, dist / cfg["output"])

    # strip（Windows DLL 无需 strip）
    if cfg["os"] in ("linux", "android"):
        strip_binary(dist / cfg["output"], cfg["os"])

    return dist / cfg["output"]


def build_macos_universal(workspace: Path, cfg: dict, vcpkg_dir: Path,
                          libwebrtc_dir: Path | None):
    """macOS universal: arm64 + x64 编译后 lipo 合并。"""
    deploy = cfg["deploy_target"]

    # arm64
    cmake_configure(workspace / "build-arm64", workspace,
                    {**cfg, "triplet": "arm64-osx", "deploy_target": deploy},
                    vcpkg_dir, libwebrtc_dir,
                    extra_args=["-DCMAKE_OSX_ARCHITECTURES=arm64"])
    cmake_build(workspace / "build-arm64")

    # x64
    cmake_configure(workspace / "build-x64", workspace,
                    {**cfg, "triplet": "x64-osx", "deploy_target": deploy},
                    vcpkg_dir, libwebrtc_dir,
                    extra_args=["-DCMAKE_OSX_ARCHITECTURES=x86_64"])
    cmake_build(workspace / "build-x64")

    # 合并
    arm64_lib = find_output(workspace / "build-arm64", "download-macos-arm64.dylib")
    x64_lib = find_output(workspace / "build-x64", "download-macos-x64.dylib")

    dist = workspace / "dist"
    dist.mkdir(parents=True, exist_ok=True)
    output = dist / cfg["output"]
    merge_universal_dylib(arm64_lib, x64_lib, output)

    # strip + 验证
    strip_binary(output, "macos")
    run(["lipo", "-info", str(output)])
    return output


def build_ios_simulator_universal(workspace: Path, cfg: dict, vcpkg_dir: Path,
                                  libwebrtc_dir: Path | None):
    """iOS simulator universal: arm64 + x64 各编译后合并。"""
    # arm64 模拟器
    cmake_configure(workspace / "build-sim-arm64", workspace,
                    {**cfg, "triplet": "arm64-ios-simulator", "simulator": True},
                    vcpkg_dir, libwebrtc_dir,
                    extra_args=["-DCMAKE_OSX_ARCHITECTURES=arm64"])
    cmake_build(workspace / "build-sim-arm64")

    # x64 模拟器
    cmake_configure(workspace / "build-sim-x64", workspace,
                    {**cfg, "triplet": "x64-ios-simulator", "simulator": True},
                    vcpkg_dir, libwebrtc_dir,
                    extra_args=["-DCMAKE_OSX_ARCHITECTURES=x86_64"])
    cmake_build(workspace / "build-sim-x64")

    # 合并为 universal
    return merge_ios_static_libs(workspace, cfg, vcpkg_dir)


def main():
    parser = argparse.ArgumentParser(description="编译 download_wrapper 全平台产物")
    parser.add_argument("--platform", required=True, choices=PLATFORMS.keys(),
                        help="目标平台")
    parser.add_argument("--vcpkg-ref", default=os.environ.get("VCPKG_REF", "latest"),
                        help="vcpkg 版本 tag")
    parser.add_argument("--workspace", default=os.getcwd(),
                        help="项目根目录")
    parser.add_argument("--temp-dir",
                        default=os.environ.get("RUNNER_TEMP", "/tmp"),
                        help="临时目录")
    parser.add_argument("--libwebrtc-dir",
                        help="本地 libwebrtc 目录（跳过下载）")
    args = parser.parse_args()

    workspace = Path(args.workspace).resolve()
    temp_dir = Path(args.temp_dir)
    cfg = PLATFORMS[args.platform]

    print("== 构建配置 ==")
    print(f"  平台:     {args.platform}")
    print(f"  OS:       {cfg['os']}")
    print(f"  triplet:  {cfg.get('triplet', 'N/A')}")
    print(f"  vcpkg:    {args.vcpkg_ref}")
    print(f"  工作目录: {workspace}")

    # 1. vcpkg
    print("\n== Step 1: vcpkg ==")
    vcpkg_dir = setup_vcpkg(workspace, args.vcpkg_ref, temp_dir)

    # 2. libwebrtc
    print("\n== Step 2: libwebrtc ==")
    if args.libwebrtc_dir:
        # 使用本地 libwebrtc 目录（合并工作流场景）
        libwebrtc_dir = Path(args.libwebrtc_dir).resolve()
        print(f"使用本地 libwebrtc: {libwebrtc_dir}")
    else:
        # 从 GitHub Release 下载
        libwebrtc_dir = download_libwebrtc(workspace, args.platform)

    # 3. 构建
    print("\n== Step 3: CMake 构建 ==")
    if args.platform == "macos-universal":
        output = build_macos_universal(workspace, cfg, vcpkg_dir, libwebrtc_dir)
    elif args.platform == "ios-simulator-universal":
        output = build_ios_simulator_universal(workspace, cfg, vcpkg_dir, libwebrtc_dir)
    elif cfg.get("static") and cfg["os"] == "ios":
        # iOS arm64 单架构静态库
        build_dir = workspace / "build"
        cmake_configure(build_dir, workspace, cfg, vcpkg_dir, libwebrtc_dir)
        cmake_build(build_dir)
        output = merge_ios_static_libs(workspace, cfg, vcpkg_dir)
    else:
        output = build_single_arch(workspace, cfg, vcpkg_dir, libwebrtc_dir, args.platform)

    print(f"\n== 构建完成 ==")
    print(f"产物: {output}")
    if output.exists():
        print(f"大小: {output.stat().st_size / 1024 / 1024:.1f} MB")


if __name__ == "__main__":
    main()
