"""Step 10 figure/table generators (scripts/figures/)."""

from __future__ import annotations

from importlib.util import module_from_spec, spec_from_file_location
from pathlib import Path

_DIR = Path(__file__).resolve().parent


def load_module(stem: str):
    """
    Import a sibling module by filename stem.

    Required because Python's built-in ``_io`` extension shadows ``_io.py``
    on normal ``import _io`` — load from the explicit file path instead.
    """
    path = _DIR / f"{stem}.py"
    spec = spec_from_file_location(f"equitable.figures.{stem}", path)
    if spec is None or spec.loader is None:
        raise ImportError(f"cannot load {path}")
    mod = module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod
