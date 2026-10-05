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
# 从 versions.py 读取版本
sys.path.insert(0, str(Path(__file__).parent))
from versions import (
    LIBTORRENT_VERSION, LIBWEBRTC_VERSION,
    get_libtorrent_release_tag, get_libwebrtc_release_tag,
    get_libtorrent_asset_name, get_libwebrtc_asset_name,
)

# 兼容旧代码
LIBWEBRTC_RELEASE_TAG = get_libwebrtc_release_tag()

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
        "overlay_triplets": True,
    },
    "android-x64": {
        "os": "android",
        "triplet": "x64-android",
        "abi": "x86_64",
        "api": 28,
        "output": "download-android-x64.so",
        "overlay_triplets": True,
    },
}

# ── 平台名 → libtorrent 构建参数映射 ──
# wrapper 平台名 → build_libtorrent 的 (plat, arch)，仅服务于自动编译兜底；
# Release 资产命名直接使用统一平台 tag（= wrapper 平台名）
LIBTORRENT_PLATFORM_MAP = {
    "macos-universal": ("macos", "universal"),
    "linux-x64": ("linux", "x64"),
    "linux-arm64": ("linux", "arm64"),
    "windows-x64": ("windows", "x64"),
    "windows-arm64": ("windows", "arm64"),
    "ios-arm64": ("ios", "arm64"),
    "ios-simulator-universal": ("ios", "universal"),
    "android-arm64": ("android", "arm64"),
    "android-x64": ("android", "x64"),
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


# ── 通用 Release 下载 ──

def _get_github_repo(workspace: Path) -> str:
    """获取 GitHub 仓库 owner/repo"""
    repo = os.environ.get("GITHUB_REPOSITORY", "")
    if not repo:
        result = run_capture(["git", "remote", "get-url", "origin"], cwd=str(workspace))
        if result.returncode == 0:
            url = result.stdout.strip()
            parts = url.rstrip("/").split("/")
            repo = f"{parts[-2]}/{parts[-1].replace('.git', '')}"
    return repo


def _download_from_release(workspace: Path, release_tag: str, asset: str,
                           component: str, version: str, platform_tag: str = None) -> Path | None:
    """从 GitHub Release 下载并解压预编译产物。
    
    Args:
        workspace: 项目根目录
        release_tag: Release tag (如 'libtorrent-v2.1.2')
        asset: 文件名 (如 'libtorrent-v2.1.2-macos-universal.tar.gz')
        component: 组件名 (如 'libtorrent', 'libwebrtc')
        version: 版本号
        platform_tag: 平台标识 (如 'windows-arm64')，用于区分架构
    
    Returns:
        解压目录路径，失败返回 None
    """
    # 路径包含平台标识，避免不同架构产物混用
    subdir = f"{component}-{version}-{platform_tag}" if platform_tag else f"{component}-{version}"
    dest = workspace / "third_party" / subdir
    
    # 已存在则跳过
    if (dest / "lib").exists():
        print(f"{component} 已存在: {dest}")
        return dest
    
    dest.mkdir(parents=True, exist_ok=True)
    
    repo = _get_github_repo(workspace)
    if not repo:
        print(f"警告: 无法获取 GitHub 仓库信息")
        return None
    
    url = f"https://github.com/{repo}/releases/download/{release_tag}/{asset}"
    print(f"下载 {component}: {url}")
    
    # 支持 token（私有仓库）
    token = os.environ.get("GITHUB_TOKEN")
    headers = {"Authorization": f"token {token}"} if token else {}
    
    tar_path = workspace / f"{component}.tar.gz"
    try:
        req = urllib.request.Request(url, headers=headers)
        with urllib.request.urlopen(req) as response, open(tar_path, 'wb') as f:
            f.write(response.read())
    except Exception as e:
        print(f"{component} 下载失败: {e}")
        if dest.exists():
            shutil.rmtree(dest)
        return None
    
    # 解压 tar.gz
    import tarfile
    with tarfile.open(tar_path, 'r:gz') as tar:
        tar.extractall(dest)
    
    # 清理
    tar_path.unlink()
    
    file_count = sum(1 for _ in (dest / 'include').rglob('*') if _.is_file())
    print(f"{component} 下载完成: {dest} (include 文件数: {file_count})")
    return dest


# ── Step 2: libwebrtc 预编译产物 ──

def download_libwebrtc(workspace: Path, platform_name: str) -> Path | None:
    """从 GitHub Release 下载 libwebrtc 预编译产物。"""
    asset = get_libwebrtc_asset_name(platform_name)
    release_tag = get_libwebrtc_release_tag()
    return _download_from_release(workspace, release_tag, asset, "libwebrtc", LIBWEBRTC_VERSION, platform_name)


def _libwebrtc_dir_valid(d: Path) -> bool:
    """校验 libwebrtc 目录完整性（判定条件与 CMakeLists 保持一致）。"""
    return (d / "include" / "libwebrtc.h").exists() and (d / "lib").exists()


def ensure_libwebrtc(workspace: Path, libwebrtc_dir: Path | None,
                     platform_name: str) -> Path | None:
    """确保 libwebrtc 可用：本地目录有效则使用，否则回退 Release 下载。

    Returns:
        有效目录；未指定本地目录且下载失败时返回 None（表示跳过 P2P）
    """
    if libwebrtc_dir and _libwebrtc_dir_valid(libwebrtc_dir):
        print(f"libwebrtc 已存在: {libwebrtc_dir}")
        return libwebrtc_dir
    if libwebrtc_dir:
        print(f"本地 libwebrtc 无效: {libwebrtc_dir}，回退 Release 下载")
    release_dir = download_libwebrtc(workspace, platform_name)
    if release_dir:
        return release_dir
    # 显式指定了本地目录却仍不可用属配置/产物错误；未指定则允许跳过 P2P
    if libwebrtc_dir:
        raise RuntimeError(
            f"libwebrtc 不可用：本地目录无效且 Release 下载失败 ({platform_name})")
    return None


# ── Step 2.5: libtorrent 获取 ──

def download_libtorrent(workspace: Path, platform_name: str) -> Path | None:
    """从 GitHub Release 下载 libtorrent 预编译产物。"""
    if platform_name not in LIBTORRENT_PLATFORM_MAP:
        return None
    asset = get_libtorrent_asset_name(platform_name)
    release_tag = get_libtorrent_release_tag()
    return _download_from_release(workspace, release_tag, asset, "libtorrent", LIBTORRENT_VERSION, platform_name)


def ensure_libtorrent(workspace: Path, libtorrent_dir: Path | None,
                      platform_name: str, vcpkg_dir: Path) -> Path | None:
    """确保 libtorrent 存在。
    
    优先级：
    1. 指定目录有效 → 使用
    2. 从 Release 下载
    3. 自动编译
    """
    # 1. 检查指定目录
    if libtorrent_dir and (libtorrent_dir / "lib").exists():
        print(f"libtorrent 已存在: {libtorrent_dir}")
        return libtorrent_dir
    
    # 2. 尝试从 Release 下载
    print("\n== 尝试从 Release 下载 libtorrent ==")
    release_dir = download_libtorrent(workspace, platform_name)
    if release_dir:
        return release_dir
    
    # 3. 自动编译
    print("\n== 自动编译 libtorrent ==")
    if platform_name not in LIBTORRENT_PLATFORM_MAP:
        print(f"警告: 平台 {platform_name} 无 libtorrent 映射，跳过")
        return None
    
    lt_platform, lt_arch = LIBTORRENT_PLATFORM_MAP[platform_name]
    output_dir = libtorrent_dir or (workspace / "build-libtorrent" / platform_name)
    
    # 检查是否已编译
    if (output_dir / "lib").exists():
        print(f"libtorrent 已存在: {output_dir}")
        return output_dir
    
    print(f"  平台: {lt_platform}-{lt_arch}")
    print(f"  输出: {output_dir}")
    
    build_script = workspace / "scripts" / "build_libtorrent.py"
    if not build_script.exists():
        print(f"警告: 找不到 {build_script}，跳过 libtorrent 编译")
        return None
    
    cmd = [
        sys.executable, str(build_script),
        "--platform", lt_platform,
        "--arch", lt_arch,
        "--output-dir", str(output_dir),
    ]
    
    env = os.environ.copy()
    env["VCPKG_ROOT"] = str(vcpkg_dir)
    
    print(f"\n>>> {' '.join(cmd)}")
    result = subprocess.run(cmd, env=env)
    if result.returncode != 0:
        raise RuntimeError(f"libtorrent 编译失败: {result.returncode}")
    
    return output_dir


# ── Step 3: CMake 构建 ──

def cmake_configure(build_dir: Path, workspace: Path, cfg: dict,
                    vcpkg_dir: Path, libwebrtc_dir: Path | None,
                    libtorrent_dir: Path | None = None,
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

    # overlay triplets（macOS/iOS/Android 需要自定义设置）
    if cfg.get("overlay_triplets"):
        overlay = workspace / "vcpkg-triplets"
        if overlay.exists():
            cmd.append(f"-DVCPKG_OVERLAY_TRIPLETS={overlay}")

    # libwebrtc
    # iOS 平台不传：上游无 iOS 预编译库（包 lib/ 为空），且 iOS 强制 P2P OFF（见 CMakeLists），
    # 传入空壳包目录会让链接段 find_library 触发 FATAL
    if cfg.get("os") == "ios":
        cmd.append("-DDW_ENABLE_P2P=OFF")
    elif libwebrtc_dir:
        cmd.append(f"-DDW_LIBWEBRTC_DIR={libwebrtc_dir}")

    # libtorrent
    if libtorrent_dir:
        cmd.append(f"-DDW_LIBTORRENT_DIR={libtorrent_dir}")

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
                "-DCMAKE_OSX_ARCHITECTURES=arm64",
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

def strip_binary(filepath: Path, platform_os: str, triplet: str = ""):
    """strip 符号裁剪。"""
    if platform_os == "linux":
        if triplet == "arm64-linux":
            # 交叉编译场景：x64 runner 上的系统 strip 无法识别 arm64 ELF
            run(["aarch64-linux-gnu-strip", "--strip-unneeded", str(filepath)])
        else:
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
    """lipo 合并 macOS universal binary，并统一 install name。

    两个单架构产物的 install name 分别为 @rpath/download-macos-arm64.dylib
    和 @rpath/download-macos-x64.dylib；合并后宿主只嵌入 universal 一份文件，
    必须把所有 slice 的 install name 统一重写为 universal 名称，
    否则运行时动态链接器按旧名称查找会命中旧文件或加载失败。
    """
    output.parent.mkdir(parents=True, exist_ok=True)
    run(["lipo", "-create", str(arm64_lib), str(x64_lib), "-output", str(output)])
    run(["install_name_tool", "-id", f"@rpath/{output.name}", str(output)])


def _thin_lib(src: Path, arch: str, temp_dir: Path) -> Path:
    """按指定架构切出 thin 静态库；thin 输入直接返回原文件。

    libtool -static 接受 fat 输入但不报错，会让合并产物变成 fat，
    导致后续 lipo -create 合并双架构产物时报 same architectures。
    因此 fat 库（如 libtorrent universal 包）必须先按架构切 thin。
    """
    info = subprocess.run(["lipo", "-info", str(src)],
                          capture_output=True, encoding='utf-8')
    stdout = (info.stdout or "") + (info.stderr or "")
    if "Non-fat" in stdout or info.returncode != 0:
        # 已是单架构文件，无需切分
        return src
    out = temp_dir / f"{src.stem}-{arch}.a"
    run(["lipo", "-thin", arch, str(src), "-output", str(out)])
    return out


def merge_ios_static_libs(workspace: Path, cfg: dict, vcpkg_dir: Path,
                          libtorrent_dir: Path | None = None) -> Path:
    """合并 iOS 静态库（自身 + libtorrent + vcpkg 依赖）为自包含胖库。"""
    dist = workspace / "dist"
    dist.mkdir(parents=True, exist_ok=True)
    temp_dir = Path(os.environ.get("RUNNER_TEMP", dist / "tmp"))
    temp_dir.mkdir(parents=True, exist_ok=True)

    # libtorrent 独立于 vcpkg_installed（由 DW_LIBTORRENT_DIR 提供），
    # 若不并入合并列表，产物链接期会报 libtorrent 符号缺失
    libtorrent_libs = []
    if libtorrent_dir and (libtorrent_dir / "lib").exists():
        libtorrent_libs = [str(f) for f in (libtorrent_dir / "lib").glob("*.a")]

    if cfg.get("universal") and cfg.get("simulator"):
        # iOS simulator universal: arm64 + x64 各合并依赖后再 lipo
        arm64_self = find_output(workspace / "build-sim-arm64", "download-ios-simulator-arm64.a")
        x64_self = find_output(workspace / "build-sim-x64", "download-ios-simulator-x64.a")
        # 自身库缺失属致命错误：libtool 对缺失文件仅告警，会静默产出缺自身符号的库
        if not arm64_self or not x64_self:
            raise FileNotFoundError(
                f"未找到 wrapper 自身静态库: arm64={arm64_self} x64={x64_self}，"
                "请检查 CMake 产物命名与脚本预期是否一致")
        arm64_deps = find_vcpkg_installed(workspace, "arm64-ios-simulator")
        x64_deps = find_vcpkg_installed(workspace, "x64-ios-simulator")

        temp_arm64 = temp_dir / "sim-arm64.a"
        temp_x64 = temp_dir / "sim-x64.a"

        # libtorrent universal 包为 fat 库，须先按架构切 thin（见 _thin_lib 说明）
        lt_arm64 = [_thin_lib(Path(f), "arm64", temp_dir) for f in libtorrent_libs]
        lt_x64 = [_thin_lib(Path(f), "x86_64", temp_dir) for f in libtorrent_libs]

        run(["libtool", "-static", "-o", str(temp_arm64), str(arm64_self)] +
            lt_arm64 + [str(f) for f in arm64_deps.glob("*.a")])
        run(["libtool", "-static", "-o", str(temp_x64), str(x64_self)] +
            lt_x64 + [str(f) for f in x64_deps.glob("*.a")])

        output = dist / cfg["output"]
        run(["lipo", "-create", str(temp_arm64), str(temp_x64), "-output", str(output)])
        return output
    else:
        # iOS arm64 单架构
        self_lib = find_output(workspace / "build", "download-ios-arm64.a")
        if not self_lib:
            raise FileNotFoundError("未找到 wrapper 自身静态库 download-ios-arm64.a")
        deps = find_vcpkg_installed(workspace, "arm64-ios")

        output = dist / cfg["output"]
        run(["libtool", "-static", "-o", str(output), str(self_lib)] +
            libtorrent_libs + [str(f) for f in deps.glob("*.a")])
        return output


# ── 主流程 ──

def build_single_arch(workspace: Path, cfg: dict, vcpkg_dir: Path,
                      libwebrtc_dir: Path | None, libtorrent_dir: Path | None,
                      build_name: str):
    """单架构构建（Linux/Windows/iOS-arm64/Android）。"""
    build_dir = workspace / "build"
    cmake_configure(build_dir, workspace, cfg, vcpkg_dir, libwebrtc_dir, libtorrent_dir)
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
        strip_binary(dist / cfg["output"], cfg["os"], cfg.get("triplet", ""))

    return dist / cfg["output"]


def build_macos_universal(workspace: Path, cfg: dict, vcpkg_dir: Path,
                          libwebrtc_dir: Path | None, libtorrent_dir: Path | None):
    """macOS universal: arm64 + x64 编译后 lipo 合并。"""
    deploy = cfg["deploy_target"]

    # arm64
    cmake_configure(workspace / "build-arm64", workspace,
                    {**cfg, "triplet": "arm64-osx", "deploy_target": deploy},
                    vcpkg_dir, libwebrtc_dir, libtorrent_dir,
                    extra_args=["-DCMAKE_OSX_ARCHITECTURES=arm64"])
    cmake_build(workspace / "build-arm64")

    # x64
    cmake_configure(workspace / "build-x64", workspace,
                    {**cfg, "triplet": "x64-osx", "deploy_target": deploy},
                    vcpkg_dir, libwebrtc_dir, libtorrent_dir,
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
                                  libwebrtc_dir: Path | None, libtorrent_dir: Path | None):
    """iOS simulator universal: arm64 + x64 各编译后合并。"""
    # arm64 模拟器
    cmake_configure(workspace / "build-sim-arm64", workspace,
                    {**cfg, "triplet": "arm64-ios-simulator", "simulator": True},
                    vcpkg_dir, libwebrtc_dir, libtorrent_dir,
                    extra_args=["-DCMAKE_OSX_ARCHITECTURES=arm64"])
    cmake_build(workspace / "build-sim-arm64")

    # x64 模拟器
    cmake_configure(workspace / "build-sim-x64", workspace,
                    {**cfg, "triplet": "x64-ios-simulator", "simulator": True},
                    vcpkg_dir, libwebrtc_dir, libtorrent_dir,
                    extra_args=["-DCMAKE_OSX_ARCHITECTURES=x86_64"])
    cmake_build(workspace / "build-sim-x64")

    # 合并为 universal
    return merge_ios_static_libs(workspace, cfg, vcpkg_dir, libtorrent_dir)


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
    parser.add_argument("--libtorrent-dir",
                        help="本地 libtorrent 目录（跳过 FetchContent）")
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

    # 2. libwebrtc（本地目录优先，无效回退 Release 下载）
    # iOS 无 libwebrtc 预编译库，跳过下载（P2P 强制 OFF，见 CMakeLists）
    print("\n== Step 2: libwebrtc ==")
    if cfg.get("os") == "ios":
        libwebrtc_dir = None
        print("iOS 平台: 跳过 libwebrtc 下载（P2P 强制 OFF）")
    else:
        libwebrtc_dir = Path(args.libwebrtc_dir).resolve() if args.libwebrtc_dir else None
        libwebrtc_dir = ensure_libwebrtc(workspace, libwebrtc_dir, args.platform)

    # 3. libtorrent（自动检测/编译）
    print("\n== Step 3: libtorrent ==")
    libtorrent_dir = Path(args.libtorrent_dir).resolve() if args.libtorrent_dir else None
    libtorrent_dir = ensure_libtorrent(workspace, libtorrent_dir, args.platform, vcpkg_dir)

    # 4. 构建
    print("\n== Step 4: CMake 构建 ==")
    if args.platform == "macos-universal":
        output = build_macos_universal(workspace, cfg, vcpkg_dir, libwebrtc_dir, libtorrent_dir)
    elif args.platform == "ios-simulator-universal":
        output = build_ios_simulator_universal(workspace, cfg, vcpkg_dir, libwebrtc_dir, libtorrent_dir)
    elif cfg.get("static") and cfg["os"] == "ios":
        # iOS arm64 单架构静态库
        build_dir = workspace / "build"
        cmake_configure(build_dir, workspace, cfg, vcpkg_dir, libwebrtc_dir, libtorrent_dir)
        cmake_build(build_dir)
        # 单架构分支同样并入 libtorrent，否则产物缺 BT 符号
        output = merge_ios_static_libs(workspace, cfg, vcpkg_dir, libtorrent_dir)
    else:
        output = build_single_arch(workspace, cfg, vcpkg_dir, libwebrtc_dir, libtorrent_dir, args.platform)

    print(f"\n== 构建完成 ==")
    print(f"产物: {output}")
    if output.exists():
        print(f"大小: {output.stat().st_size / 1024 / 1024:.1f} MB")


if __name__ == "__main__":
    main()
