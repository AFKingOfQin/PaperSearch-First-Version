# AI-generated code (Codex): read-only host and service tool diagnostics.
import ctypes
import os
import platform
import shutil
import subprocess
import sys

import psutil


def probe(label: str, args: list[str]) -> str | None:
    if not shutil.which(args[0]):
        print(f"BLOCKED {label}: {args[0]} missing from PATH")
        return None
    try:
        result = subprocess.run(args, capture_output=True, timeout=15)
    except (OSError, subprocess.TimeoutExpired) as error:
        print(f"BLOCKED {label}: {error}")
        return None
    raw = result.stdout + result.stderr
    encoding = "utf-16-le" if b"\x00" in raw else "utf-8"
    message = raw.decode(encoding, errors="replace").strip().replace("\r", "")
    print(f"{'OK' if result.returncode == 0 else 'BLOCKED'} {label}: {message or '(no output)'}")
    return message if result.returncode == 0 else None


def doctor() -> bool:
    print(f"{platform.platform()} {platform.machine()} | Python {platform.python_version()} | {sys.executable}")
    ram = psutil.virtual_memory()
    print(f"CPU: {psutil.cpu_count(logical=False)} physical / {os.cpu_count()} logical; "
          f"RAM: {ram.available / 2**30:.2f} GiB available / {ram.total / 2**30:.2f} GiB usable")
    for disk in psutil.disk_partitions():
        if "cdrom" in disk.opts or not disk.fstype:
            continue
        space = shutil.disk_usage(disk.mountpoint)
        print(f"{disk.mountpoint} {space.free / 2**30:.2f} GiB free / {space.total / 2**30:.2f} GiB")
    git = probe("Git", ["git", "--version"])
    wsl2 = True
    if os.name == "nt":
        firmware = ctypes.windll.kernel32.IsProcessorFeaturePresent(21)
        print(f"Windows firmware virtualization flag: {bool(firmware)} (confirm in Task Manager if false)")
        probe("WSL package", ["wsl", "--version"])
        distros = probe("WSL distributions", ["wsl", "--list", "--verbose"])
        wsl2 = any(line.split()[-1:] == ["2"] for line in (distros or "").splitlines())
        if not wsl2:
            print("BLOCKED WSL 2: no distribution verified as version 2")
    cli = probe("Docker CLI", ["docker", "--version"])
    compose = probe("Compose plugin", ["docker", "compose", "version"])
    engine = probe("Docker engine (OS, version, CPUs, RAM bytes, root directory)",
                   ["docker", "info", "--format",
                    "{{.OSType}} {{.ServerVersion}} {{.NCPU}} {{.MemTotal}} {{.DockerRootDir}}"])
    if engine and not engine.startswith("linux "):
        print("BLOCKED Linux engine: check Docker's active context and container mode")
    return bool(git and wsl2 and cli and compose and engine and engine.startswith("linux "))
