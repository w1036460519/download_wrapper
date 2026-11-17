#!/usr/bin/env python3
"""按平台下载 libtorrent 的 GitHub Release 预编译库。"""
"""
python3 scripts/fetch_libtorrent.py --platform macos-universal
python3 scripts/fetch_libtorrent.py --platform ios-arm64
python3 scripts/fetch_libtorrent.py --platform windows-x64
"""

import argparse
import json
import os
from pathlib import Path
import shutil
import ssl
import sys
import tarfile
import tempfile
from urllib.error import URLError
from urllib.request import Request, urlopen

from build_wrapper import LIBTORRENT_PLATFORM_MAP
from versions import get_libtorrent_asset_name, get_libtorrent_release_tag


def build_ssl_context():
    context = ssl.create_default_context()
    try:
        import certifi
    except ImportError:
        pass
    else:
        context.load_verify_locations(cafile=certifi.where())

    # python.org 的 macOS 解释器可能没有安装 OpenSSL 默认 CA 文件。
    system_ca = Path("/etc/ssl/cert.pem")
    if sys.platform == "darwin" and system_ca.is_file():
        context.load_verify_locations(cafile=str(system_ca))
    return context


def fetch(url, token, accept, context):
    request = Request(url, headers={"Accept": accept})
    if token:
        request.add_unredirected_header("Authorization", f"Bearer {token}")
    return urlopen(request, timeout=60, context=context)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--platform", required=True, choices=LIBTORRENT_PLATFORM_MAP,
                        help="Release 产物的平台标识")
    parser.add_argument("--repo", default=os.getenv("GITHUB_REPOSITORY") or "w1036460519/download_wrapper",
                        help="GitHub 仓库 OWNER/REPO")
    args = parser.parse_args()

    token = os.getenv("GITHUB_TOKEN") or os.getenv("GH_TOKEN") or os.getenv("GIT_TOKEN")
    root = Path(__file__).resolve().parents[1]
    dest = root / "build-libtorrent" / args.platform
    tag = get_libtorrent_release_tag()
    name = get_libtorrent_asset_name(args.platform)
    library = "torrent-rasterbar.lib" if args.platform.startswith("windows-") else "libtorrent-rasterbar.a"
    release_url = f"https://api.github.com/repos/{args.repo}/releases/tags/{tag}"

    context = build_ssl_context()
    with fetch(release_url, token, "application/vnd.github+json", context) as response:
        release = json.load(response)
    asset = next((item for item in release["assets"] if item["name"] == name), None)
    if asset is None:
        raise SystemExit(f"Release {tag} 中找不到 {name}")

    print(f"下载 {name}", flush=True)
    with tempfile.TemporaryDirectory() as temp:
        staging = Path(temp) / "libtorrent"
        with tempfile.TemporaryFile() as archive:
            with fetch(asset["url"], token, "application/octet-stream", context) as response:
                shutil.copyfileobj(response, archive)
            archive.seek(0)
            with tarfile.open(fileobj=archive, mode="r:gz") as tar:
                tar.extractall(staging, filter="data")
        for relative in ("include/libtorrent/version.hpp", f"lib/{library}"):
            if not (staging / relative).is_file():
                raise SystemExit(f"预编译包缺少 {relative}")
        shutil.copytree(staging, dest, dirs_exist_ok=True)

    print(f"下载完成：{dest}")
    print(f"CMake 参数：-DDW_LIBTORRENT_DIR={dest}")


if __name__ == "__main__":
    try:
        main()
    except (URLError, OSError, tarfile.TarError) as error:
        raise SystemExit(f"获取 libtorrent 失败：{error}") from None
