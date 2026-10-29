"""Provide tests for the executor."""

# Authors: Federico Raimondo <f.raimondo@fz-juelich.de>
# License: AGPL

import pickle
import subprocess
import sys
from pathlib import Path

import pytest

from joblib_htcondor import executor
from joblib_htcondor.delayed_submission import DelayedSubmission


def _run_executor(fname: Path) -> subprocess.CompletedProcess:
    """Run the executor on a task file."""
    return subprocess.run(
        [
            sys.executable,
            "-m",
            "joblib_htcondor.executor",
            "--lock-lifetime",
            "10",
            str(fname),
        ],
        capture_output=True,
        text=True,
        timeout=60,
    )


def _load_result(fname: Path) -> DelayedSubmission:
    """Load the result written by the executor for a task file."""
    out_fname = fname.with_stem(f"{fname.stem}_out")
    ds = DelayedSubmission.load(out_fname, lock_lifetime=10)
    assert ds is not None
    return ds


def test_executor_runs_task(tmp_path) -> None:
    """Test that the executor runs the task and dumps the result."""
    fname = tmp_path / "task-1.pickle"
    DelayedSubmission(max, 1, 2).dump(fname)
    proc = _run_executor(fname)
    assert proc.returncode == 0, proc.stdout + proc.stderr
    ds = _load_result(fname)
    assert ds.done()
    assert not ds.error()
    assert ds.result() == 2


def test_executor_missing_task(tmp_path) -> None:
    """Test that a missing task file sends the error back."""
    fname = tmp_path / "task-1.pickle"
    proc = _run_executor(fname)
    assert proc.returncode == 1
    ds = _load_result(fname)
    assert ds.done()
    assert ds.error()
    assert isinstance(ds.result(), FileNotFoundError)


def test_executor_unimportable_module(tmp_path) -> None:
    """Test that a task that needs a missing module sends the error back."""
    module_dir = tmp_path / "module"
    module_dir.mkdir()
    (module_dir / "jht_test_module.py").write_text("def f():\n    return 1\n")
    fname = tmp_path / "task-1.pickle"
    # Dump the task in a process that can import the module
    subprocess.run(
        [
            sys.executable,
            "-c",
            "import sys; sys.path.insert(0, sys.argv[1]); "
            "import jht_test_module; "
            "from joblib_htcondor.delayed_submission import "
            "DelayedSubmission; "
            "DelayedSubmission(jht_test_module.f).dump(sys.argv[2])",
            str(module_dir),
            str(fname),
        ],
        check=True,
    )
    proc = _run_executor(fname)
    assert proc.returncode == 1
    ds = _load_result(fname)
    assert ds.error()
    assert isinstance(ds.result(), ModuleNotFoundError)
    assert "jht_test_module" in str(ds.result())


def test_executor_corrupt_task(tmp_path) -> None:
    """Test that a corrupt task file sends the error back."""
    fname = tmp_path / "task-1.pickle"
    fname.write_text("This is not a pickle file.")
    proc = _run_executor(fname)
    assert proc.returncode == 1
    ds = _load_result(fname)
    assert ds.error()
    assert isinstance(ds.result(), pickle.UnpicklingError)


def test_load_task_retries(tmp_path, monkeypatch) -> None:
    """Test that loading is retried a limited number of times."""
    monkeypatch.setattr(executor, "_LOAD_ATTEMPTS", 3)
    monkeypatch.setattr(executor.time, "sleep", lambda seconds: None)
    fname = tmp_path / "task-1.pickle"
    fname.touch()
    ds = DelayedSubmission(max, 1, 2)
    results = iter([None, None, ds])
    calls = []

    def load(fname, lock_lifetime):
        calls.append(fname)
        return next(results)

    monkeypatch.setattr(DelayedSubmission, "load", load)
    assert executor._load_task(fname, lock_lifetime=10) is ds
    assert len(calls) == 3

    results = iter([None, None, None, ds])
    with pytest.raises(pickle.UnpicklingError, match="after 3 attempts"):
        executor._load_task(fname, lock_lifetime=10)
