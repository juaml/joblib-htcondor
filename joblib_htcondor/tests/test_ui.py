"""Provide tests for the UI."""

# Authors: Federico Raimondo <f.raimondo@fz-juelich.de>
# License: AGPL

import argparse
import asyncio
import json
from datetime import datetime, timedelta
from pathlib import Path

import pytest
from textual.widgets import DataTable, Static

from joblib_htcondor.backend import _BackendMeta, _TaskMeta
from joblib_htcondor.ui.__main__ import dir_or_file_path
from joblib_htcondor.ui.app import MonitorApp, OpenScreen
from joblib_htcondor.ui.utils import (
    COLOR_DONE,
    COLOR_QUEUED,
    COLOR_RUNNING,
    COLOR_SENT,
    format_elapsed,
    progress_bar,
)


RUNNING = "jht-b-l0"
FINISHED = "jht-a-l0"


def _write_meta(
    shared_dir: Path,
    name: str,
    parent: str | None,
    n_done: int,
    n_running: int,
    running: bool,
    start: datetime,
) -> None:
    """Write a metadata file as the backend does."""
    if running:
        (shared_dir / name).mkdir()
    t0 = start
    tasks = [
        _TaskMeta(1, t0, t0, t0, t0 + timedelta(hours=1), 1)
        for _ in range(n_done)
    ] + [_TaskMeta(1, t0, t0, t0, None, 1) for _ in range(n_running)]
    meta = _BackendMeta(
        uuid=name,
        parent=parent,
        recursion_level=int(name.split("-l")[-1]),
        throttle=10,
        shared_data_dir=shared_dir / name,
        n_tasks=len(tasks),
        start_timestamp=start,
        update_timestamp=start + timedelta(hours=2),
        task_status=tasks,
    )
    with (shared_dir / ".jht-meta" / f"{name}.json").open("w") as f:
        json.dump(meta.asdict(), f)


@pytest.fixture
def shared_dir(tmp_path: Path) -> Path:
    """Provide a shared data dir with a running and a finished run."""
    (tmp_path / ".jht-meta").mkdir()
    now = datetime.now()
    old = now - timedelta(days=1)
    _write_meta(tmp_path, FINISHED, None, 3, 0, False, old)
    _write_meta(tmp_path, RUNNING, None, 2, 1, True, now)
    _write_meta(tmp_path, "jht-b1-l1", RUNNING, 1, 1, True, now)
    _write_meta(tmp_path, "jht-b11-l2", "jht-b1-l1", 0, 2, True, now)
    return tmp_path


def test_progress_bar() -> None:
    """Test the length and the colors of the progress bar."""
    bar = progress_bar(10, 1, 1, 1, 0)
    assert len(bar) == 10
    # The rounding remainders go to the largest shares
    lengths = {
        span.style: span.end - span.start
        for span in bar.spans
        if span.end > span.start
    }
    assert lengths == {COLOR_DONE: 4, COLOR_RUNNING: 3, COLOR_SENT: 3}
    empty = progress_bar(10, 0, 0, 0, 0)
    assert len(empty) == 10
    assert empty.style == COLOR_QUEUED


def test_format_elapsed() -> None:
    """Test formatting time intervals."""
    assert format_elapsed(timedelta(seconds=3725)) == "01h 02m 05s"
    assert format_elapsed(timedelta(days=2, seconds=5)) == "2d 00h 00m 05s"
    assert format_elapsed(timedelta(seconds=-5)) == "00h 00m 00s"


def test_dir_or_file_path(shared_dir: Path) -> None:
    """Test the paths accepted by the UI."""
    meta_dir = shared_dir / ".jht-meta"
    assert dir_or_file_path(str(shared_dir)) == meta_dir
    assert dir_or_file_path(str(meta_dir)) == meta_dir
    fname = meta_dir / f"{RUNNING}.json"
    assert dir_or_file_path(str(fname)) == fname
    with pytest.raises(argparse.ArgumentTypeError):
        dir_or_file_path(str(shared_dir / RUNNING))


async def _settle(app: MonitorApp, pilot) -> None:
    """Wait for the trees to load and the screen to update."""
    await app.workers.wait_for_complete()
    await pilot.pause()


def _info(app: MonitorApp) -> str:
    """Return the run status text."""
    return str(app.main_screen.query_one("#info", Static).render())


def test_app(shared_dir: Path) -> None:
    """Test opening, showing and clearing runs."""

    async def run() -> None:
        app = MonitorApp(shared_dir / ".jht-meta", refresh=60)
        async with app.run_test(size=(140, 40)) as pilot:
            # Without a run, the list of runs opens, newest first
            assert isinstance(app.screen, OpenScreen)
            runs = app.screen.query_one("#runs", DataTable)
            assert [str(k.value) for k in runs.rows] == [
                str(shared_dir / ".jht-meta" / f"{n}.json")
                for n in [RUNNING, FINISHED]
            ]
            assert "No run open" in _info(app)

            # Open the running run
            await pilot.press("enter")
            await _settle(app, pilot)
            assert not isinstance(app.screen, OpenScreen)
            assert "Running" in _info(app)
            summary = app.main_screen.query_one("#summary", DataTable)
            assert summary.row_count == 4  # 3 levels and the total
            total = summary.get_row_at(3)
            # Done, running and total tasks of all the levels
            assert [str(total[i]) for i in [1, 2, 5]] == ["3", "4", "7"]
            batches = app.main_screen.query_one("#batches", DataTable)
            assert [str(r[0]).strip("└ ") for r in _rows(batches)] == [
                RUNNING,
                "jht-b1-l1",
                "jht-b11-l2",
            ]

            # Open the finished run
            await pilot.press("o", "down", "enter")
            await _settle(app, pilot)
            assert "Finished" in _info(app)
            # The elapsed time stops at the last update
            assert "Elapsed: 02h 00m 00s" in _info(app)
            assert (
                app.main_screen.query_one("#summary", DataTable).row_count == 1
            )

            # Clear the finished runs, which closes the open run
            await pilot.press("o", "l")
            await pilot.pause()
            runs = app.screen.query_one("#runs", DataTable)
            assert runs.row_count == 1
            await pilot.press("escape", "r")
            await _settle(app, pilot)
            assert "No run open" in _info(app)
            assert not (shared_dir / ".jht-meta" / f"{FINISHED}.json").exists()

    asyncio.run(run())


def _rows(table: DataTable) -> list[list]:
    """Return the rows of a table."""
    return [table.get_row_at(i) for i in range(table.row_count)]
