"""
Build first, then prove each wheel installs alone without dependency resolution.
"""

from __future__ import annotations

import pathlib
import subprocess
import sys
import tempfile
import zipfile


def main() -> None:
    """
    Install each built wheel independently and verify its import-time isolation.
    """
    root: pathlib.Path = pathlib.Path(__file__).resolve().parents[1]
    package: str
    for package in ("log", "evt", "sig"):
        module: str = package
        distribution: str = f"tix-{package}"
        wheels: list[pathlib.Path] = list((root / "dist").glob(f"tix_{package}-*.whl"))
        if len(wheels) != 1:
            raise RuntimeError(f"Expected exactly one wheel for {module}; build into a clean dist")
        wheel: pathlib.Path = wheels[0]
        archive: zipfile.ZipFile
        with zipfile.ZipFile(wheel) as archive:
            assert f"{module}/py.typed" in archive.namelist()
        temporary: str
        with tempfile.TemporaryDirectory(prefix=f"tix-{package}-") as temporary:
            environment: pathlib.Path = pathlib.Path(temporary) / "env"
            subprocess.run(["uv", "venv", "--python", sys.executable, str(environment)], check=True)
            python: pathlib.Path = environment / (
                "Scripts/python.exe" if sys.platform == "win32" else "bin/python"
            )
            subprocess.run(
                [
                    "uv",
                    "pip",
                    "install",
                    "--python",
                    str(python),
                    "--no-deps",
                    "--link-mode=copy",
                    str(wheel),
                ],
                check=True,
            )
            checks: str = f"""
# Preload stdlib asyncio: it registers its own named loggers on first import.
import asyncio, importlib, importlib.metadata, importlib.util, logging, signal, threading
before = (list(logging.getLogger().handlers), dict(logging.Logger.manager.loggerDict),
          signal.getsignal(signal.SIGINT), tuple(threading.enumerate()),
          logging.getLevelNamesMapping())
module = importlib.import_module({module!r})
after = (list(logging.getLogger().handlers), dict(logging.Logger.manager.loggerDict),
         signal.getsignal(signal.SIGINT), tuple(threading.enumerate()),
         logging.getLevelNamesMapping())
assert before == after, 'Import performed global configuration or started a thread'
assert not importlib.metadata.requires({distribution!r})
for other in ('log', 'evt', 'sig'):
    if other != {module!r}:
        assert importlib.util.find_spec(other) is None, 'Unexpected sibling installed'
"""
            subprocess.run([str(python), "-I", "-c", checks], cwd=temporary, check=True)
        print(f"Verified independent wheel: {wheel.name}", flush=True)


if __name__ == "__main__":
    main()
