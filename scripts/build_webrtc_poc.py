#!/usr/bin/env python3
"""Phase 0 PoC：在 CI runner 上验证「stock WebRTC 纯数据通道」构建可行性。

与生产脚本 build_libwebrtc.py 的关键差异（均为方案 C 的核心）：
  1. 源码来自 Google 官方 webrtc.googlesource.com/src（非 webrtc-sdk 镜像）；
  2. 不克隆 libwebrtc、不打 custom_audio_source patch（根除补丁漂移问题）；
  3. GN 仅配置 data-only 参数，ninja 只构建 //poc:datachannel_loopback；
  4. 回环测试用 gn 的 executable 目标在源码树内编译，复用 WebRTC 自身的
     编译/链接参数（libc++/abseil/SCTP），规避外部手工拼装链接命令的坑。

复用 build_libwebrtc.py 的通用工具（run / gn_args_string / _depot_cmd /
gclient_cmd / setup_depot_tools），仅实现 PoC 专属的取源、GN、打包与验证逻辑。

用法：
  python3 scripts/build_webrtc_poc.py --temp-dir "$RUNNER_TEMP"
  python3 scripts/build_webrtc_poc.py --ref branch-heads/7559 --skip-package
"""

import argparse
import glob
import os
import platform
import shutil
import subprocess
import sys
import time
from pathlib import Path

# 复用生产脚本中已验证的通用工具，避免重复实现（复用优先）。
sys.path.insert(0, str(Path(__file__).resolve().parent))
from build_libwebrtc import (  # noqa: E402
    _depot_cmd,
    gclient_cmd,
    gn_args_string,
    run,
    setup_depot_tools,
)

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")

# 官方源与版本 pin：release 分支 branch-heads/7559 对应 m144。
DEFAULT_REF = "branch-heads/7559"
OFFICIAL_SRC_URL = "https://webrtc.googlesource.com/src"

# data-only 最小 ninja 目标：仅依赖工厂入口，媒体模块不会被编译/链接。
# 该目标的 deps 已核实不含 media（api:create_modular_peer_connection_factory）。
DATAONLY_DEPS = [
    "//api:create_modular_peer_connection_factory",
    "//api:peer_connection_interface",
    "//api:jsep",
    "//rtc_base:threading",
]


def banner(title: str):
    print(f"\n{'=' * 60}\n{title}\n{'=' * 60}", flush=True)


# ── Step A: gclient 同步官方源 ──

def sync_official_src(temp_dir: Path, ref: str, target_os: str) -> Path:
    """从 webrtc.googlesource.com 同步 stock WebRTC，返回 src 目录。"""
    src_dir = temp_dir / "webrtc_src"
    src_dir.mkdir(parents=True, exist_ok=True)

    (src_dir / ".gclient").write_text(f"""\
solutions = [
  {{
    "name"        : "src",
    "url"         : "{OFFICIAL_SRC_URL}@{ref}",
    "deps_file"   : "DEPS",
    "managed"     : False,
    "custom_deps" : {{}},
    "custom_vars" : {{}},
  }},
]
target_os = ['{target_os}']
""")

    if platform.system() == "Windows":
        os.environ["DEPOT_TOOLS_WIN_TOOLCHAIN"] = "0"

    run(gclient_cmd("sync", "--no-history", "--shallow", "--jobs", "8", "-D"),
        cwd=str(src_dir))

    webrtc_src = src_dir / "src"
    # 记录并 pin 实际检出的 commit SHA（可复现性）。
    sha = subprocess.run(["git", "rev-parse", "HEAD"], cwd=str(webrtc_src),
                         capture_output=True, text=True).stdout.strip()
    print(f"\n[PIN] stock WebRTC commit: {sha} (ref: {ref})")
    return webrtc_src


# ── Step B: 注入 PoC gn 目标 + 拷贝回环测试 ──

def inject_poc_target(webrtc_src: Path, poc_cc: Path):
    """把回环测试拷进 src/poc/ 并写 BUILD.gn（executable + 完整静态库）。"""
    poc_dir = webrtc_src / "poc"
    poc_dir.mkdir(parents=True, exist_ok=True)
    shutil.copy2(poc_cc, poc_dir / "datachannel_loopback.cc")

    deps_gn = ",\n".join(f'    "{d}"' for d in DATAONLY_DEPS)
    (poc_dir / "BUILD.gn").write_text(f"""\
# Phase 0 PoC 自动生成：data-only 回环验证目标。
# executable 直接随源码树编译，复用 WebRTC 全部默认 config（defines/include）。

executable("datachannel_loopback") {{
  sources = [ "datachannel_loopback.cc" ]
  deps = [
{deps_gn},
  ]
}}

# 次级目标：把所有传递依赖打包进单个完整静态库，用于测量 data-only 体积、
# 验证「可被外部消费的单归档」打包路径（Phase 3 打包逻辑的前置探针）。
static_library("webrtc_dataonly") {{
  complete_static_lib = true
  deps = [
{deps_gn},
  ]
}}
""")
    print(f"[OK] 注入 PoC 目标: {poc_dir / 'BUILD.gn'}")


# ── Step C: GN 配置（data-only） ──

def gn_gen(webrtc_src: Path, target_os: str, target_cpu: str):
    args = {
        "is_debug": False,
        "is_component_build": False,
        "treat_warnings_as_errors": False,
        "rtc_include_tests": False,
        "rtc_build_examples": False,
        "rtc_build_tools": False,
        "is_clang": True,
        "target_os": target_os,
        "target_cpu": target_cpu,
    }
    args_str = gn_args_string(args)
    print(f"[GN args] {args_str}")
    cmd = _depot_cmd("gn", "gen", "out/Release", f"--args={args_str}")
    result = subprocess.run(cmd, cwd=str(webrtc_src), capture_output=True,
                            encoding="utf-8", errors="replace")
    if result.returncode != 0:
        print(f"\n[GN 错误输出]\n{(result.stdout or '') + (result.stderr or '')}")
        raise subprocess.CalledProcessError(result.returncode, cmd)
    print((result.stdout or "")[-2000:])


# ── Step D: ninja 编译 + 运行回环测试（主门禁） ──

def build_and_run_loopback(webrtc_src: Path) -> bool:
    out = webrtc_src / "out" / "Release"
    run(_depot_cmd("ninja", "-C", "out/Release", "poc:datachannel_loopback"),
        cwd=str(webrtc_src))

    # 定位可执行文件（gn 默认输出到 out 根目录）。
    exe = out / "datachannel_loopback"
    if not exe.exists():
        hits = glob.glob(str(out / "**" / "datachannel_loopback*"), recursive=True)
        hits = [h for h in hits if os.path.isfile(h) and os.access(h, os.X_OK)]
        if not hits:
            print("[ERROR] 未找到编译出的 datachannel_loopback 可执行文件")
            return False
        exe = Path(hits[0])

    banner("运行 data-only 回环测试")
    print(f"$ {exe}")
    proc = subprocess.run([str(exe)], cwd=str(out),
                          encoding="utf-8", errors="replace", timeout=120)
    return proc.returncode == 0


# ── Step E: 打包完整静态库 + 头文件（次级信号，失败不影响主门禁结论） ──

def package_dataonly(webrtc_src: Path, temp_dir: Path):
    out = webrtc_src / "out" / "Release"
    run(_depot_cmd("ninja", "-C", "out/Release", "poc:webrtc_dataonly"),
        cwd=str(webrtc_src))

    libs = glob.glob(str(out / "obj" / "**" / "libwebrtc_dataonly.a"),
                     recursive=True)
    if libs:
        size_mb = os.path.getsize(libs[0]) / 1024 / 1024
        print(f"[OK] data-only 完整静态库: {libs[0]} ({size_mb:.1f} MB)")

    pkg = temp_dir / "webrtc_build"
    if pkg.exists():
        shutil.rmtree(pkg)
    (pkg / "include").mkdir(parents=True)
    (pkg / "lib").mkdir(parents=True)

    # 头文件：以 src 根为基准归拢外部消费所需目录 + 生成头 + abseil。
    for d in ["api", "rtc_base", "p2p", "pc", "call", "modules",
              "system_wrappers", "logging", "media"]:
        s = webrtc_src / d
        if s.is_dir():
            shutil.copytree(s, pkg / "include" / d, dirs_exist_ok=True)
    gen = out / "gen"
    if gen.is_dir():
        shutil.copytree(gen, pkg / "include" / "gen", dirs_exist_ok=True)
    absl = webrtc_src / "third_party" / "abseil-cpp" / "absl"
    if absl.exists():
        shutil.copytree(absl, pkg / "include" / "third_party" / "absl",
                        dirs_exist_ok=True)
    for f in glob.glob(str(out / "obj" / "**" / "*.a"), recursive=True):
        shutil.copy2(f, pkg / "lib")

    lib_count = len(list((pkg / "lib").glob("*.a")))
    total_mb = sum(f.stat().st_size for f in pkg.rglob("*") if f.is_file()) / 1024 / 1024
    print(f"[OK] 打包 webrtc_build/: {lib_count} 个 .a, 总计 {total_mb:.1f} MB")
    return pkg


# ── 入口 ──

def main():
    parser = argparse.ArgumentParser(description="stock WebRTC data-only PoC 构建")
    parser.add_argument("--ref", default=DEFAULT_REF, help="官方源 ref")
    parser.add_argument("--temp-dir",
                        default=os.environ.get("DW_TEMP",
                                               os.environ.get("RUNNER_TEMP", "/tmp")))
    parser.add_argument("--poc-cc",
                        default=str(Path(__file__).resolve().parent / "poc" /
                                    "datachannel_loopback.cc"))
    parser.add_argument("--skip-package", action="store_true",
                        help="跳过静态库打包（只跑主门禁回环测试）")
    args = parser.parse_args()

    temp_dir = Path(args.temp_dir)
    system = platform.system()
    target_os = {"Linux": "linux", "Darwin": "mac", "Windows": "win"}[system]
    machine = platform.machine().lower()
    target_cpu = "arm64" if machine in ("arm64", "aarch64") else "x64"

    banner("Phase 0 PoC 配置")
    print(f"  官方源 ref: {args.ref}")
    print(f"  target:     {target_os}/{target_cpu}")
    print(f"  临时目录:   {temp_dir}")

    t0 = time.time()
    results = {}

    banner("Step 1: depot_tools")
    setup_depot_tools(temp_dir)

    banner("Step 2: gclient sync 官方源")
    webrtc_src = sync_official_src(temp_dir, args.ref, target_os)

    banner("Step 3: 注入 PoC gn 目标")
    inject_poc_target(webrtc_src, Path(args.poc_cc))

    banner("Step 4: GN gen (data-only)")
    gn_gen(webrtc_src, target_os, target_cpu)

    banner("Step 5: ninja 编译 + 运行回环测试")
    try:
        loopback_ok = build_and_run_loopback(webrtc_src)
    except subprocess.CalledProcessError as e:
        print(f"[ERROR] 编译回环测试失败: {e}")
        loopback_ok = False
    results["loopback"] = "PASS" if loopback_ok else "FAIL"

    if not args.skip_package:
        banner("Step 6: 打包 data-only 静态库 + 头文件")
        try:
            package_dataonly(webrtc_src, temp_dir)
            results["package"] = "OK"
        except Exception as e:  # noqa: BLE001
            print(f"[WARN] 打包失败（不影响主门禁结论）: {e}")
            results["package"] = f"FAIL ({e})"

    banner("PoC 结论汇总")
    print(f"  回环收发 (主门禁): {results['loopback']}")
    print(f"  静态库打包 (次级): {results.get('package', 'skipped')}")
    print(f"  总耗时: {(time.time() - t0) / 60:.1f} 分钟")

    if not loopback_ok:
        sys.exit(1)


if __name__ == "__main__":
    main()
