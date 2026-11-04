"""Provide tests for the UI metadata tree parser."""

# Authors: Federico Raimondo <f.raimondo@fz-juelich.de>
# License: AGPL

import json
from pathlib import Path

from joblib_htcondor.backend import _BackendMeta
from joblib_htcondor.ui.treeparser import clear_finished


def _write_meta(
    meta_dir: Path, name: str, parent: str | None, running: bool
) -> None:
    """Write a metadata file as the backend does."""
    level = int(name.split("-l")[-1])
    data_dir = meta_dir.parent / name
    if running:
        data_dir.mkdir()
    meta = _BackendMeta(
        uuid=name,
        parent=parent,
        recursion_level=level,
        shared_data_dir=data_dir,
    )
    with (meta_dir / f"{name}.json").open("w") as f:
        json.dump(meta.asdict(), f)


def test_clear_finished(tmp_path: Path) -> None:
    """Test that only the files of the finished trees are removed."""
    meta_dir = tmp_path / ".jht-meta"
    meta_dir.mkdir()
    # Finished tree, with two nested levels
    _write_meta(meta_dir, "jht-a-l0", None, running=False)
    _write_meta(meta_dir, "jht-a1-l1", "jht-a-l0", running=False)
    _write_meta(meta_dir, "jht-a2-l1", "jht-a-l0", running=False)
    _write_meta(meta_dir, "jht-a11-l2", "jht-a1-l1", running=False)
    # Running tree, with a finished and a running nested level
    _write_meta(meta_dir, "jht-b-l0", None, running=True)
    _write_meta(meta_dir, "jht-b1-l1", "jht-b-l0", running=False)
    _write_meta(meta_dir, "jht-b2-l1", "jht-b-l0", running=True)
    _write_meta(meta_dir, "jht-b11-l2", "jht-b1-l1", running=False)
    # A file being written can't be read, so it is kept
    (meta_dir / "jht-c-l0.json").write_text("{")

    removed = clear_finished(meta_dir)

    assert sorted(f.name for f in removed) == [
        "jht-a-l0.json",
        "jht-a1-l1.json",
        "jht-a11-l2.json",
        "jht-a2-l1.json",
    ]
    assert sorted(f.name for f in meta_dir.iterdir()) == [
        "jht-b-l0.json",
        "jht-b1-l1.json",
        "jht-b11-l2.json",
        "jht-b2-l1.json",
        "jht-c-l0.json",
    ]


def test_clear_finished_empty(tmp_path: Path) -> None:
    """Test clearing a metadata directory without files."""
    assert clear_finished(tmp_path) == []
