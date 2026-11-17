#!/usr/bin/env python3
"""在 macOS 构建并打包 Wrapper，可选上传到 GitHub Release。"""
"""
# 只编译 iOS（真机 arm64 + 模拟器 Universal）
python3 scripts/build_apple_release.py --platform ios

# 只编译 macOS Universal（Apple Silicon + Intel）
python3 scripts/build_apple_release.py --platform macos

# 本地编译 macOS Universal、iOS 真机和 iOS 模拟器，产物放在 dist/
python3 scripts/build_apple_release.py

# 编译完成后上传到 GitHub Release，允许覆盖同名 Apple 产物
python3 scripts/build_apple_release.py --upload --clobber
"""

import argparse
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tarfile
import tempfile

from build_wrapper import PLATFORMS, setup_vcpkg
from versions import WRAPPER_VERSION, get_wrapper_asset_name, get_wrapper_release_tag

ROOT = Path(__file__).resolve().parents[1]
APPLE_TARGETS = {
    "macos": ("macos-universal",),
    "ios": ("ios-arm64", "ios-simulator-universal"),
}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--platform", choices=("all", *APPLE_TARGETS), default="all")
    parser.add_argument("--upload", action="store_true", help="上传到 GitHub Release")
    parser.add_argument("--repo", help="GitHub 仓库 OWNER/REPO，默认由 gh 根据当前仓库选择")
    parser.add_argument("--clobber", action="store_true", help="允许覆盖同名 Release 资产")
    parser.add_argument("--jobs", type=int, default=os.cpu_count() or 1, help="CMake 并行编译数")
    parser.add_argument("--vcpkg-dir", type=Path, default=os.environ.get("VCPKG_ROOT"),
                        help="复用已 bootstrap 的 vcpkg，默认使用 VCPKG_ROOT")
    parser.add_argument("--vcpkg-ref", default=os.environ.get("VCPKG_REF", "latest"))
    args = parser.parse_args()

    if sys.platform != "darwin":
        parser.error("Apple 平台构建需要 macOS 和 Xcode")
    if args.jobs < 1:
        parser.error("--jobs 必须大于 0")
    if args.upload and shutil.which("gh") is None:
        parser.error("上传需要 GitHub CLI（gh）")

    targets = APPLE_TARGETS["macos"] + APPLE_TARGETS["ios"] if args.platform == "all" else APPLE_TARGETS[args.platform]
    dist = ROOT / "dist"
    dist.mkdir(exist_ok=True)
    assets = []
    with tempfile.TemporaryDirectory(prefix="wrapper-apple-") as temp:
        temp_dir = Path(temp)
        vcpkg_dir = setup_vcpkg(ROOT, args.vcpkg_ref, temp_dir, args.vcpkg_dir)
        env = {**os.environ, "CMAKE_BUILD_PARALLEL_LEVEL": str(args.jobs), "RUNNER_TEMP": temp}
        for target in targets:
            binary = dist / PLATFORMS[target]["output"]
            subprocess.run([
                sys.executable, str(ROOT / "scripts/build_wrapper.py"),
                "--platform", target, "--workspace", str(ROOT),
                "--temp-dir", temp, "--vcpkg-dir", str(vcpkg_dir),
            ], cwd=ROOT, env=env, check=True)
            architectures = ["arm64"] if target == "ios-arm64" else ["arm64", "x86_64"]
            subprocess.run(["lipo", "-verify_arch", *architectures, str(binary)], check=True)

            asset = dist / get_wrapper_asset_name(target)
            with tarfile.open(asset, "w:gz") as archive:
                archive.add(binary, arcname=binary.name)
                archive.add(ROOT / "include", arcname="include")
            assets.append(asset)
            print(f"已打包 {asset}", flush=True)

    if args.upload:
        tag = get_wrapper_release_tag()
        repo = ["--repo", args.repo] if args.repo else []
        release = subprocess.run(
            ["gh", "release", "view", tag, *repo], cwd=ROOT, stdout=subprocess.DEVNULL,
        )
        if release.returncode:
            commit = subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=ROOT, text=True).strip()
            subprocess.run([
                "gh", "release", "create", tag, *repo, "--target", commit,
                "--title", f"wrapper v{WRAPPER_VERSION}",
                "--notes", f"download_wrapper v{WRAPPER_VERSION} 预编译产物",
            ], cwd=ROOT, check=True)
        subprocess.run([
            "gh", "release", "upload", tag, *repo, *(["--clobber"] if args.clobber else []),
            *(str(asset) for asset in assets),
        ], cwd=ROOT, check=True)


if __name__ == "__main__":
    main()
