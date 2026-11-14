"""Install the official k6 binary with its published checksum into /data."""
import hashlib
import json
import os
from pathlib import Path
import shutil
import tarfile
import re
import time
import urllib.error
import urllib.request
import subprocess

import runtime


def install_github():
    destination = runtime.ROOT / "toolchain/k6"
    receipt = runtime.ROOT / "config/k6-receipt.json"
    if (destination / "k6").exists() and receipt.exists():
        print("k6 ready", json.loads(receipt.read_text())["version"])
        return
    request = urllib.request.Request("https://api.github.com/repos/grafana/k6/releases/latest", headers={"User-Agent": "llm-gateway-lab", "Accept": "application/vnd.github+json"})
    try:
        with runtime.HTTP.open(request, timeout=30) as response:
            release = json.load(response)
    except urllib.error.HTTPError as exc:
        if exc.code != 403: raise
        # Public release HTML remains available when the unauthenticated API
        # quota is exhausted. Every asset still comes from the official repo.
        with runtime.HTTP.open("https://github.com/grafana/k6/releases/latest", timeout=30) as response:
            version = response.url.rsplit("/",1)[-1]
        if not re.fullmatch(r"v[0-9]+\.[0-9]+\.[0-9]+", version): raise RuntimeError("Unexpected official release tag")
        with runtime.HTTP.open("https://github.com/grafana/k6/releases/expanded_assets/"+version,timeout=30) as response:
            html=response.read().decode()
        links=re.findall(r'href="(/grafana/k6/releases/download/[^"?#]+)"',html)
        release={"tag_name":version,"assets":[{"name":url.rsplit("/",1)[-1],"browser_download_url":"https://github.com"+url} for url in links]}
    assets = release["assets"]
    archive_asset = next(a for a in assets if a["name"].endswith("linux-amd64.tar.gz"))
    checksum_asset = next(a for a in assets if "checksum" in a["name"].lower() and a["name"].endswith(".txt"))
    with runtime.HTTP.open(checksum_asset["browser_download_url"], timeout=30) as response:
        checksums = response.read().decode()
    expected = next(line.split()[0] for line in checksums.splitlines() if line.split()[-1].lstrip("*") == archive_asset["name"])
    archive = runtime.ROOT / "tmp" / archive_asset["name"]
    for attempt in range(6):
        offset=archive.stat().st_size if archive.exists() else 0
        request=urllib.request.Request(archive_asset["browser_download_url"],headers={"Range":f"bytes={offset}-"} if offset else {})
        try:
            with runtime.HTTP.open(request,timeout=30) as response:
                if offset and response.status!=206:offset=0
                with archive.open("ab" if offset else "wb") as target:
                    total=offset;last_logged=total
                    while block:=response.read(65536):
                        total+=len(block)
                        if total>128*1024*1024:raise RuntimeError("Unexpected k6 archive size")
                        target.write(block)
                        if total-last_logged>=2*1024*1024:
                            print("k6 download bytes",total,flush=True);last_logged=total
            break
        except (TimeoutError,ConnectionError,OSError) as exc:
            print("download interrupted; resuming",type(exc).__name__,flush=True)
            if attempt==5:raise
            time.sleep(1)
    digest = hashlib.sha256()
    with archive.open("rb") as source:
        while block:=source.read(1024*1024):digest.update(block)
    if digest.hexdigest() != expected:
        raise RuntimeError("Official k6 archive checksum mismatch")
    destination.mkdir(parents=True, exist_ok=True)
    with tarfile.open(archive) as bundle:
        member = next(m for m in bundle.getmembers() if m.name.endswith("/k6") and m.isfile())
        with bundle.extractfile(member) as source, (destination / "k6").open("wb") as target:
            shutil.copyfileobj(source, target)
    (destination / "k6").chmod(0o700)
    receipt.write_text(json.dumps({"version":release["tag_name"], "archive":archive_asset["name"], "sha256":expected,
                                   "source":archive_asset["browser_download_url"]}, indent=2))
    print("Official k6 installed", release["tag_name"], "sha256", expected)


def main():
    destination=runtime.ROOT/"toolchain/k6"
    receipt=runtime.ROOT/"config/k6-receipt.json"
    if (destination/"k6").exists() and receipt.exists():
        print("k6 ready",json.loads(receipt.read_text())["version"]);return
    # Official Linux package CDN avoids slow GitHub release-asset transfers.
    url="https://dl.k6.io/deb/dists/stable/main/binary-amd64/Packages"
    with runtime.HTTP.open(url,timeout=30) as response:text=response.read().decode()
    packages=[]
    for stanza in text.split("\n\n"):
        fields=dict(line.split(": ",1) for line in stanza.splitlines() if ": " in line and not line.startswith(" "))
        if fields.get("Package")=="k6" and fields.get("Architecture")=="amd64":packages.append(fields)
    chosen=max(packages,key=lambda value:tuple(map(int,re.findall(r"[0-9]+",value["Version"]))))
    asset_url="https://dl.k6.io/deb/"+chosen["Filename"]
    archive=runtime.ROOT/"tmp"/Path(chosen["Filename"]).name
    digest=hashlib.sha256()
    with runtime.HTTP.open(asset_url,timeout=30) as response,archive.open("wb") as target:
        while block:=response.read(65536):target.write(block);digest.update(block)
    if digest.hexdigest()!=chosen["SHA256"]:raise RuntimeError("Official package SHA256 mismatch")
    extracted=runtime.ROOT/"toolchain/k6-package"
    subprocess.run(["dpkg-deb","--extract",str(archive),str(extracted)],check=True)
    destination.mkdir(parents=True,exist_ok=True)
    shutil.copyfile(extracted/"usr/bin/k6",destination/"k6");(destination/"k6").chmod(0o700)
    receipt.write_text(json.dumps({"version":"v"+chosen["Version"],"sha256":chosen["SHA256"],"source":asset_url,"package_index":url},indent=2))
    print("Official k6 package installed",chosen["Version"],flush=True)


if __name__ == "__main__":main()
