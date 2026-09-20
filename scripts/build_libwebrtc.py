#!/usr/bin/env python3
"""
从源码编译 webrtc-sdk/libwebrtc（DataChannel 裁剪版）。

统一处理全平台构建逻辑：depot_tools 初始化、gclient sync、GN 配置、Ninja 编译、产物打包。
消除 CI YAML 中的 bash 字符串拼接与引号转义问题。

用法：
  python3 scripts/build_libwebrtc.py --platform macos
  python3 scripts/build_libwebrtc.py --platform linux-x64 --webrtc-branch m144_release
"""

import argparse
import os
import platform
import shutil
import subprocess
import sys
import zipfile
from pathlib import Path

# Windows 默认 cp1252 无法输出中文，强制 UTF-8
if hasattr(sys.stdout, 'reconfigure'):
    sys.stdout.reconfigure(encoding='utf-8', errors='replace')
    sys.stderr.reconfigure(encoding='utf-8', errors='replace')

# ── 常量 ──
LIBWEBRTC_TAG = "libwebrtc.m144.7559.09"

# ── 平台配置 ──
# 每个平台的 GN 参数由 Python dict 构造，彻底消除 bash 引号转义问题。
PLATFORMS = {
    "macos": {
        "target_os": "mac",
        "universal": True,
        "asset": "libwebrtc-macos-release.zip",
    },
    "linux-x64": {
        "target_os": "linux",
        "target_cpu": "x64",
        "asset": "libwebrtc-linux-x64-release.zip",
    },
    "linux-arm64": {
        "target_os": "linux",
        "target_cpu": "arm64",
        "asset": "libwebrtc-linux-arm64-release.zip",
    },
    "windows-x64": {
        "target_os": "win",
        "target_cpu": "x64",
        "asset": "libwebrtc-windows-x64-release.zip",
    },
    "windows-arm64": {
        "target_os": "win",
        "target_cpu": "arm64",
        "asset": "libwebrtc-windows-arm64-release.zip",
    },
    "android-arm64": {
        "target_os": "android",
        "target_cpu": "arm64",
        "needs_ndk": True,
        "extra": {"android_static_analysis": "off"},
        "asset": "libwebrtc-android-arm64-release.zip",
    },
    "android-x64": {
        "target_os": "android",
        "target_cpu": "x64",
        "needs_ndk": True,
        "extra": {"android_static_analysis": "off"},
        "asset": "libwebrtc-android-x64-release.zip",
    },
    "ios-arm64": {
        "target_os": "ios",
        "target_cpu": "arm64",
        "extra": {"target_environment": "device"},
        "asset": "libwebrtc-ios-arm64-release.zip",
    },
    "ios-simulator": {
        "target_os": "ios",
        "universal": True,
        "target_environment": "simulator",
        "asset": "libwebrtc-ios-simulator-release.zip",
    },
}


def run(cmd, **kwargs):
    """执行命令，实时打印输出，失败时抛出异常。"""
    cmd_str = ' '.join(str(c) for c in cmd)
    print(f"\n$ {cmd_str}")
    # 实时流式输出（不缓冲），便于监控长命令进度
    process = subprocess.Popen(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        bufsize=1,
        **kwargs
    )
    for line in process.stdout:
        print(line, end='')
    process.wait()
    if process.returncode != 0:
        print(f"\n[ERROR] 命令失败 (exit {process.returncode}): {cmd_str}")
        raise subprocess.CalledProcessError(process.returncode, cmd)


def gclient_cmd(*args):
    """构造 gclient 命令。

    Windows: 通过 cmd.exe /c 调用 gclient.bat（触发 depot_tools bootstrap，捆绑 git 等工具）
    其他平台: 直接调用 gclient
    """
    if platform.system() == "Windows":
        return ["cmd.exe", "/c", "gclient.bat"] + list(args)
    return ["gclient"] + list(args)


def _depot_cmd(name: str, *args):
    """构造 depot_tools 命令（gn / ninja 等）。

    Windows 上 depot_tools 通过 .bat wrapper 调用，需经 cmd.exe。
    """
    if platform.system() == "Windows":
        return ["cmd.exe", "/c", f"{name}.bat"] + list(args)
    return [name] + list(args)


def gn_args_string(args: dict) -> str:
    """将 dict 转为 GN --args 格式字符串（空格分隔）。"""
    parts = []
    for k, v in args.items():
        if isinstance(v, bool):
            parts.append(f"{k}={'true' if v else 'false'}")
        elif isinstance(v, str):
            parts.append(f'{k}="{v}"')
        else:
            parts.append(f"{k}={v}")
    return " ".join(parts)


# ── Step 1: depot_tools ──

def setup_depot_tools(temp_dir: Path) -> Path:
    """克隆 depot_tools 并加入 PATH，返回路径。"""
    depot = temp_dir / "depot_tools"
    if not depot.exists():
        run(["git", "clone", "--depth", "1",
             "https://chromium.googlesource.com/chromium/tools/depot_tools.git",
             str(depot)])

    depot_str = str(depot)
    os.environ["PATH"] = f"{depot_str}{os.pathsep}{os.environ['PATH']}"

    # Windows git 配置
    if platform.system() == "Windows":
        for key, val in [("core.autocrlf", "false"),
                         ("core.filemode", "false"),
                         ("core.fscache", "true")]:
            run(["git", "config", "--global", key, val])

    # 触发 depot_tools 初始化（bootstrap 捆绑 git 等工具）
    run(gclient_cmd("--version"))

    return depot


# ── Step 2: gclient 配置 + 同步 ──

def configure_gclient(temp_dir: Path, webrtc_branch: str, target_os: str) -> Path:
    """创建 .gclient 配置并执行 gclient sync，返回源码路径。"""
    src_dir = temp_dir / "webrtc_src"
    src_dir.mkdir(parents=True, exist_ok=True)

    gclient_file = src_dir / ".gclient"
    gclient_file.write_text(f"""\
solutions = [
  {{
    "name"        : "src",
    "url"         : "https://github.com/webrtc-sdk/webrtc.git@{webrtc_branch}",
    "deps_file"   : "DEPS",
    "managed"     : False,
    "custom_deps" : {{}},
    "custom_vars" : {{}},
  }},
]
target_os = ['{target_os}']
""")
    webrtc_src = src_dir / "src"

    # Windows: 使用公开桶下载工具链（绕过私有桶 401 错误）
    if platform.system() == "Windows":
        os.environ["DEPOT_TOOLS_WIN_TOOLCHAIN"] = "1"
        os.environ["DEPOT_TOOLS_WIN_TOOLCHAIN_BASE_URL"] = \
            "https://commondatastorage.googleapis.com/chrome-wintoolchain/"

    run(gclient_cmd("sync", "--no-history", "--shallow", "--jobs", "8", "-D"),
        cwd=str(src_dir))

    return webrtc_src


# ── Step 3: libwebrtc 封装集成 ──

def integrate_libwebrtc(webrtc_src: Path):
    """克隆 webrtc-sdk/libwebrtc 到源码树并打补丁。"""
    libwebrtc_dir = webrtc_src / "libwebrtc"
    if not libwebrtc_dir.exists():
        run(["git", "clone", "--depth", "1", "--branch", LIBWEBRTC_TAG,
             "https://github.com/webrtc-sdk/libwebrtc.git",
             str(libwebrtc_dir)])

    # 应用补丁（如果存在）
    patch = libwebrtc_dir / "patches" / "custom_audio_source_m144.patch"
    if patch.exists():
        try:
            run(["git", "apply", str(patch)], cwd=str(webrtc_src))
        except subprocess.CalledProcessError:
            print("警告: 补丁应用失败，跳过")

    # 将 libwebrtc 加入 BUILD.gn 依赖
    build_gn = webrtc_src / "BUILD.gn"
    content = build_gn.read_text()
    if "libwebrtc" not in content:
        content = content.replace(
            'deps = [ ":webrtc" ]',
            'deps = [ ":webrtc", "//libwebrtc" ]'
        )
        build_gn.write_text(content)


# ── Step 4: GN 配置 ──

def build_common_args() -> dict:
    """DataChannel 裁剪公共 GN 参数。"""
    return {
        "is_debug": False,
        "is_component_build": False,
        "rtc_include_tests": False,
        "rtc_build_examples": False,
        "rtc_build_tools": False,
        "rtc_enable_protobuf": False,
        "rtc_use_h264": False,
        "rtc_use_vp8": False,
        "rtc_use_vp9": False,
        "rtc_use_av1": False,
        "rtc_include_ilbc": False,
        "rtc_include_builtin_audio_codecs": False,
        "rtc_include_builtin_video_codecs": False,
        "rtc_enable_sctp": True,
        "treat_warnings_as_errors": False,
        "rtc_libvpx_build_vp8": False,
        "use_libcxx_modules": False,
    }


def gn_gen(webrtc_src: Path, platform_name: str, ndk_path: str = ""):
    """根据平台配置生成 GN 构建目录。"""
    cfg = PLATFORMS[platform_name]
    common = build_common_args()

    def _gn(out_dir: str, extra: dict):
        args = {**common, **extra}
        args_str = gn_args_string(args)
        cmd = _depot_cmd("gn", "gen", out_dir, f"--args={args_str}")
        result = subprocess.run(cmd, cwd=str(webrtc_src), capture_output=True, text=True)
        if result.returncode != 0:
            output = (result.stdout or "") + (result.stderr or "")
            if output.strip():
                print(f"\n[GN 错误输出]\n{output.strip()}")
            raise subprocess.CalledProcessError(result.returncode, cmd)

    if cfg.get("universal"):
        # macOS / iOS simulator：双架构
        if platform_name == "macos":
            _gn("out/Release", {"target_cpu": "arm64", "target_os": "mac", "is_clang": True})
            _gn("out/Release-x64", {"target_cpu": "x64", "target_os": "mac", "is_clang": True})
        else:
            # iOS simulator（CI 无代码签名证书）
            env = cfg.get("target_environment", "simulator")
            ios_sign = {"ios_enable_code_signing": False}
            _gn("out/Release", {"target_cpu": "arm64", "target_os": "ios",
                                "is_clang": True, "target_environment": env, **ios_sign})
            _gn("out/Release-x64", {"target_cpu": "x64", "target_os": "ios",
                                    "is_clang": True, "target_environment": env, **ios_sign})
    else:
        extra = {"target_cpu": cfg["target_cpu"], "target_os": cfg["target_os"], "is_clang": True}
        if "extra" in cfg:
            extra.update(cfg["extra"])
        if cfg.get("needs_ndk") and ndk_path:
            extra["android_ndk_root"] = ndk_path
        # iOS 设备构建同样禁用代码签名
        if cfg["target_os"] == "ios":
            extra["ios_enable_code_signing"] = False
        _gn("out/Release", extra)


# ── Step 5: Ninja 编译 ──

def ninja_build(webrtc_src: Path, platform_name: str):
    """编译并合并 universal 产物。"""
    cfg = PLATFORMS[platform_name]

    env = dict(os.environ)

    dirs = ["out/Release"]
    if cfg.get("universal"):
        dirs.append("out/Release-x64")

    for d in dirs:
        result = subprocess.run(
            _depot_cmd("ninja", "-C", d, "default"),
            cwd=str(webrtc_src), env=env, capture_output=True, text=True
        )
        if result.returncode != 0:
            output = (result.stdout or "") + (result.stderr or "")
            lines = output.strip().splitlines()
            print(f"\n[Ninja 最后 {min(50, len(lines))} 行输出]")
            for line in lines[-50:]:
                print(line)
            raise subprocess.CalledProcessError(result.returncode, "ninja")

    # universal 合并
    if cfg.get("universal"):
        _merge_universal(webrtc_src)


def _merge_universal(webrtc_src: Path):
    """lipo 合并 arm64 + x64 为 universal binary。"""
    arm64_dir = webrtc_src / "out" / "Release"
    x64_dir = webrtc_src / "out" / "Release-x64"

    for ext in ("*.a", "*.so", "*.dylib"):
        for f in arm64_dir.glob(ext):
            x64_f = x64_dir / f.name
            if x64_f.exists():
                out = f.with_suffix(f.suffix + ".universal")
                run(["lipo", "-create", str(f), str(x64_f), "-output", str(out)])
                out.rename(f)


# ── Step 6: 打包产物 ──

def package(webrtc_src: Path, platform_name: str, temp_dir: Path):
    """打包 include/ + lib/ + src/ 为 zip。"""
    cfg = PLATFORMS[platform_name]
    pkg_dir = temp_dir / "libwebrtc-pkg"

    if pkg_dir.exists():
        shutil.rmtree(pkg_dir)
    (pkg_dir / "include").mkdir(parents=True)
    (pkg_dir / "lib").mkdir(parents=True)
    (pkg_dir / "src" / "base").mkdir(parents=True)

    # 1. libwebrtc 公共头文件
    libwebrtc_include = webrtc_src / "libwebrtc" / "include"
    if libwebrtc_include.exists():
        shutil.copytree(libwebrtc_include, pkg_dir / "include", dirs_exist_ok=True)

    # 2. Google WebRTC 内部头文件
    for d in ["api", "rtc_base", "pc", "media", "call", "modules", "system_wrappers"]:
        src = webrtc_src / d
        if src.is_dir():
            shutil.copytree(src, pkg_dir / "include" / d, dirs_exist_ok=True)

    # abseil-cpp
    absl = webrtc_src / "third_party" / "abseil-cpp" / "absl"
    if absl.exists():
        dest = pkg_dir / "include" / "third_party" / "absl"
        shutil.copytree(absl, dest, dirs_exist_ok=True)

    # 3. 编译产物
    out_dir = webrtc_src / "out" / "Release"
    for ext in ("*.a", "*.so", "*.dylib", "*.lib"):
        for f in out_dir.glob(ext):
            shutil.copy2(f, pkg_dir / "lib")

    # 4. 封装实现源码
    libwebrtc_src = webrtc_src / "libwebrtc" / "src"
    if libwebrtc_src.exists():
        for f in libwebrtc_src.iterdir():
            if f.suffix in (".cc", ".h"):
                shutil.copy2(f, pkg_dir / "src")
        base_dir = libwebrtc_src / "base"
        if base_dir.exists():
            for f in base_dir.iterdir():
                if f.suffix == ".cc":
                    shutil.copy2(f, pkg_dir / "src" / "base")

    # 统计
    for sub in ("include", "lib", "src"):
        count = sum(1 for _ in (pkg_dir / sub).rglob("*") if _.is_file())
        print(f"  {sub}/: {count} 文件")

    # 打包 zip
    asset = cfg["asset"]
    zip_path = temp_dir / asset
    if zip_path.exists():
        zip_path.unlink()
    with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as zf:
        for root, _, files in os.walk(pkg_dir):
            for file in files:
                fp = Path(root) / file
                zf.write(fp, fp.relative_to(pkg_dir))

    print(f"\n产物: {zip_path} ({zip_path.stat().st_size / 1024 / 1024:.1f} MB)")
    return zip_path


# ── 入口 ──

def main():
    parser = argparse.ArgumentParser(description="编译 libwebrtc（DataChannel 裁剪版）")
    parser.add_argument("--platform", required=True, choices=PLATFORMS.keys(),
                        help="目标平台")
    parser.add_argument("--webrtc-branch", default="m144_release",
                        help="webrtc-sdk/webrtc 分支")
    parser.add_argument("--temp-dir",
                        default=os.environ.get("DW_TEMP", os.environ.get("RUNNER_TEMP", "/tmp")),
                        help="临时目录")
    args = parser.parse_args()

    temp_dir = Path(args.temp_dir)
    cfg = PLATFORMS[args.platform]

    print("== 构建配置 ==")
    print(f"  平台:       {args.platform}")
    print(f"  目标 OS:    {cfg['target_os']}")
    print(f"  分支:       {args.webrtc_branch}")
    print(f"  libwebrtc:  {LIBWEBRTC_TAG}")
    print(f"  临时目录:   {temp_dir}")

    # 1. depot_tools
    print("\n== Step 1: depot_tools ==")
    setup_depot_tools(temp_dir)

    # 2. gclient sync
    print("\n== Step 2: gclient sync ==")
    webrtc_src = configure_gclient(temp_dir, args.webrtc_branch, cfg["target_os"])

    # 2.5 交叉编译 sysroot（x86_64 runner 编译 arm64 目标时需额外 sysroot）
    if cfg["target_os"] == "linux" and cfg.get("target_cpu") == "arm64":
        print("\n== Step 2.5: 安装 arm64 sysroot ==")
        run(["python3", "build/linux/sysroot_scripts/install-sysroot.py", "--arch=arm64"],
            cwd=str(webrtc_src))

    # 3. libwebrtc 集成
    print("\n== Step 3: libwebrtc 集成 ==")
    integrate_libwebrtc(webrtc_src)

    # 4. GN 配置
    print("\n== Step 4: GN gen ==")
    ndk = os.environ.get("ANDROID_NDK", "")
    gn_gen(webrtc_src, args.platform, ndk)

    # 5. 编译
    print("\n== Step 5: Ninja build ==")
    ninja_build(webrtc_src, args.platform)

    # 6. 打包
    print("\n== Step 6: 打包产物 ==")
    package(webrtc_src, args.platform, temp_dir)

    print("\n== 构建完成 ==")


if __name__ == "__main__":
    main()
