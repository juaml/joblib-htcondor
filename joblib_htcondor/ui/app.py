"""The joblib htcondor UI application."""

# Authors: Federico Raimondo <f.raimondo@fz-juelich.de>
# License: AGPL

import json
import platform
import shutil
from datetime import datetime
from json.decoder import JSONDecodeError
from pathlib import Path
from typing import Any

from rich.text import Text
from textual import work
from textual.app import App, ComposeResult
from textual.binding import Binding
from textual.containers import Horizontal, Vertical
from textual.screen import ModalScreen, Screen
from textual.theme import Theme
from textual.widgets import DataTable, Footer, Header, Static

from .treeparser import MetaTree, clear_finished, parse
from .uilogging import logger
from .utils import (
    COLOR_DONE_TEXT,
    COLOR_RUNNING,
    format_elapsed,
    legend,
    progress_bar,
    space_to_unit,
)


__all__ = ["MonitorApp"]

# Length of the progress bars in the tables
PBAR_LENGTH = 30
# Task counters shown in the tables
STATUS_KEYS = ["done", "running", "sent", "queued", "total", "throttle"]
STATUS_LABELS = ["Done", "Running", "Sent", "Queued", "Total", "Throttle"]
# Disk usage thresholds (in %) and their colors
DISK_COLORS = [
    (75, "color(9)"),
    (50, "color(13)"),
    (25, "color(4)"),
    (0, COLOR_DONE_TEXT),
]
# Neon synthwave theme
SYNTHWAVE = Theme(
    name="synthwave",
    primary="#ff2ad4",
    secondary="#5f00d7",
    accent="#00f0ff",
    warning="#ffd319",
    error="#ff0000",
    success="#7b2cff",
    foreground="#f0e6ff",
    background="#0d0221",
    surface="#160536",
    panel="#2a0f5c",
    dark=True,
)


def _status_cells(status: dict[str, Any]) -> list[Any]:
    """Build the table cells for the task counters of a batch or level."""
    cells: list[Any] = [
        Text(str(status[k]), justify="right") for k in STATUS_KEYS
    ]
    cells.append(
        progress_bar(
            PBAR_LENGTH,
            status["done"],
            status["running"],
            status["sent"],
            status["queued"],
        )
    )
    return cells


def _add_columns(table: DataTable, first: str) -> None:
    """Add the columns of a table with task counters."""
    table.add_column(first)
    for label in STATUS_LABELS:
        table.add_column(Text(label, justify="right"))
    table.add_column("Progress", width=PBAR_LENGTH)


def _walk(tree: MetaTree, level: int = 0):
    """Iterate over the batches of a tree, depth first."""
    yield tree, level
    for child in tree.children:
        yield from _walk(child, level + 1)


class OpenScreen(ModalScreen[Path | None]):
    """Screen to choose the run to open.

    Parameters
    ----------
    meta_dir : pathlib.Path
        The metadata directory.

    """

    BINDINGS = [  # noqa: RUF012
        Binding("escape", "close", "Close"),
        Binding("l", "clear_finished", "Clear finished"),
        Binding("r", "reload", "Refresh"),
    ]

    def __init__(self, meta_dir: Path) -> None:
        super().__init__()
        self.meta_dir = meta_dir

    def compose(self) -> ComposeResult:
        """Create the widgets."""
        with Vertical(id="dialog"):
            yield Static("Open run", classes="title")
            yield DataTable(id="runs", cursor_type="row")
            yield Static(f"No runs in {self.meta_dir}", id="empty")
            yield Footer()

    def on_mount(self) -> None:
        """Fill the list of runs."""
        table = self.query_one("#runs", DataTable)
        table.add_columns("Started", "Status", "Tasks", "Batch ID")
        self.action_reload()

    def action_reload(self) -> None:
        """Reload the list of runs, newest first."""
        runs = []
        for fname in self.meta_dir.glob("*-l0.json"):
            try:
                with fname.open("r") as fd:
                    data = json.load(fd)
                started = datetime.fromisoformat(data["start_timestamp"])
                n_tasks = data["n_tasks"]
            except (OSError, JSONDecodeError, KeyError, ValueError) as e:
                logger.error(f"Error loading {fname}: {e}")
                continue
            running = (self.meta_dir.parent / fname.stem).exists()
            runs.append((started, running, n_tasks, fname))
        runs.sort(reverse=True)
        table = self.query_one("#runs", DataTable)
        table.clear()
        for started, running, n_tasks, fname in runs:
            table.add_row(
                started.strftime("%Y-%m-%d %H:%M:%S"),
                Text("Running", style=COLOR_RUNNING)
                if running
                else Text("Finished", style=COLOR_DONE_TEXT),
                Text(str(n_tasks), justify="right"),
                fname.stem,
                key=str(fname),
            )
        self.query_one("#empty").display = len(runs) == 0
        table.display = len(runs) > 0

    def action_clear_finished(self) -> None:
        """Remove the metadata of the finished runs."""
        removed = clear_finished(self.meta_dir)
        self.notify(f"Removed {len(removed)} metadata files")
        self.action_reload()

    def action_close(self) -> None:
        """Close without opening a run."""
        self.dismiss(None)

    def on_data_table_row_selected(self, event: DataTable.RowSelected) -> None:
        """Open the selected run."""
        self.dismiss(Path(event.row_key.value))  # type: ignore


class MonitorApp(App):
    """The HTCondor Joblib Monitor.

    Parameters
    ----------
    path : pathlib.Path
        The metadata directory, or the metadata file of the run to open.
    refresh : float, optional
        The refresh interval in seconds (default 5).

    """

    TITLE = "HTCondor Joblib Monitor"
    CSS = """
    #info {
        height: auto;
        padding: 0 1;
    }
    .title {
        padding: 0 1;
        margin-top: 1;
        text-style: bold;
        color: $accent;
    }
    #summary {
        height: auto;
        max-height: 8;
    }
    #batches {
        height: 1fr;
    }
    #status {
        height: 1;
        padding: 0 1;
    }
    #legend {
        width: 1fr;
    }
    #disk {
        width: auto;
    }
    OpenScreen {
        align: center middle;
    }
    #dialog {
        width: 90%;
        max-width: 100;
        height: auto;
        max-height: 80%;
        border: round $primary;
        background: $surface;
    }
    #dialog .title {
        margin-top: 0;
    }
    #runs {
        height: auto;
        max-height: 20;
    }
    #empty {
        padding: 1 2;
    }
    """
    BINDINGS = [  # noqa: RUF012
        Binding("o", "open_run", "Open"),
        Binding("r", "reload", "Refresh"),
        Binding("q", "quit", "Quit"),
    ]

    def __init__(self, path: Path, refresh: float = 5) -> None:
        super().__init__()
        if path.is_file():
            self.meta_dir = path.parent
            self.run_path: Path | None = path
        else:
            self.meta_dir = path
            self.run_path = None
        self.refresh_interval = refresh
        self.meta_tree: MetaTree | None = None

    def compose(self) -> ComposeResult:
        """Create the widgets."""
        yield Header(show_clock=True)
        yield Static(id="info")
        yield Static("Summary", classes="title")
        yield DataTable(id="summary", cursor_type="none")
        yield Static("Batches", classes="title")
        yield DataTable(id="batches", cursor_type="none")
        with Horizontal(id="status"):
            yield Static(legend(), id="legend")
            yield Static(id="disk")
        yield Footer()

    @property
    def main_screen(self) -> Screen:
        """The screen with the run, also when the list of runs is open."""
        return self.screen_stack[0]

    def on_mount(self) -> None:
        """Set up the theme, the tables and the timers."""
        self.register_theme(SYNTHWAVE)
        self.theme = SYNTHWAVE.name
        self.sub_title = platform.node()
        _add_columns(
            self.main_screen.query_one("#summary", DataTable), "Level"
        )
        _add_columns(
            self.main_screen.query_one("#batches", DataTable), "Batch ID"
        )
        self.set_interval(self.refresh_interval, self.action_reload)
        self.set_interval(1, self.update_info)
        self.update_info()
        self.update_disk()
        if self.run_path is None:
            self.action_open_run()
        else:
            self.action_reload()

    def action_open_run(self) -> None:
        """Show the list of runs to open one."""
        if not isinstance(self.screen, OpenScreen):
            self.push_screen(OpenScreen(self.meta_dir), self._open)

    def _open(self, path: Path | None) -> None:
        """Open the run chosen in the list of runs."""
        if path is not None:
            self._set_run(path)

    def _set_run(self, path: Path | None) -> None:
        """Show the run with this metadata file, or no run if None."""
        self.run_path = path
        self.meta_tree = None
        self.main_screen.query_one("#summary", DataTable).clear()
        self.main_screen.query_one("#batches", DataTable).clear()
        self.update_info()
        self.action_reload()

    def _run_removed(self, path: Path) -> None:
        """Close the run if its metadata file was removed."""
        if path == self.run_path:
            self.notify(f"The metadata of {path.stem} was removed")
            self._set_run(None)

    def action_reload(self) -> None:
        """Reload the tree of the open run."""
        self.update_disk()
        if self.run_path is not None:
            self.load_tree(self.run_path)

    @work(thread=True, exclusive=True)
    def load_tree(self, path: Path) -> None:
        """Parse the tree in a thread, it can be slow on shared disks."""
        try:
            tree = parse(path)
        except FileNotFoundError:
            self.call_from_thread(self._run_removed, path)
            return
        except (OSError, JSONDecodeError, KeyError, ValueError) as e:
            logger.error(f"Error loading {path}: {e}")
            self.call_from_thread(
                self.notify,
                f"Error loading {path.name}: {e}",
                severity="error",
            )
            return
        self.call_from_thread(self.show_tree, tree)

    def show_tree(self, tree: MetaTree) -> None:
        """Show a parsed tree."""
        if tree.fname != self.run_path:
            # Another run was opened while parsing
            return
        self.meta_tree = tree
        summary = self.main_screen.query_one("#summary", DataTable)
        summary.clear()
        levels = tree.get_level_status_summary()
        if len(levels) > 1:
            for i, status in enumerate(levels):
                summary.add_row(f"Level {i}", *_status_cells(status))
        total: dict[str, Any] = {
            k: sum(s[k] for s in levels) for k in STATUS_KEYS[:-1]
        }
        total["throttle"] = levels[0]["throttle"] if len(levels) == 1 else ""
        summary.add_row(Text("Total", style="bold"), *_status_cells(total))

        batches = self.main_screen.query_one("#batches", DataTable)
        scroll_y = batches.scroll_y
        batches.clear()
        for batch, level in _walk(tree):
            status = batch.get_task_status() | {
                "throttle": batch.meta.throttle
            }
            name = Text(
                "  " * (level - 1) + "└ " if level > 0 else "",
                style="dim",
            )
            done = 0 < status["total"] == status["done"]
            name.append(batch.meta.uuid, style=COLOR_DONE_TEXT if done else "")
            batches.add_row(name, *_status_cells(status))
        batches.scroll_to(y=scroll_y, animate=False)
        self.update_info()

    def update_info(self) -> None:
        """Update the status of the open run."""
        info = self.main_screen.query_one("#info", Static)
        tree = self.meta_tree
        if tree is None:
            if self.run_path is None:
                info.update("No run open, press [b]o[/b] to open one.")
            else:
                info.update(f"Loading {self.run_path.name}...")
            return
        status = tree.get_task_status()
        if tree.is_running():
            state = Text("Running", style=f"bold {COLOR_RUNNING}")
            end = datetime.now()
        else:
            end = tree.last_update()
            if status["done"] == status["total"]:
                state = Text("Finished", style=f"bold {COLOR_DONE_TEXT}")
            else:
                state = Text("Stopped", style="bold red")
        elapsed = format_elapsed(end - tree.meta.start_timestamp)
        updated = tree.last_update().strftime("%Y-%m-%d %H:%M:%S")
        info.update(
            Text.assemble(
                (tree.meta.uuid, "bold"),
                "  ",
                state,
                f"  Elapsed: {elapsed}",
                f"  Updated: {updated}",
                f"  Core hours: {tree.get_core_hours():.2f}",
            )
        )

    def update_disk(self) -> None:
        """Update the usage of the disk with the shared data."""
        try:
            total, used, _ = shutil.disk_usage(self.meta_dir)
        except OSError as e:
            logger.error(f"Error reading disk usage: {e}")
            return
        percent = used / total * 100
        color = next(c for t, c in DISK_COLORS if percent >= t)
        u_used, unit_used = space_to_unit(used)
        u_total, unit_total = space_to_unit(total)
        self.main_screen.query_one("#disk", Static).update(
            Text.assemble(
                "Disk used: ",
                (f"{percent:.0f}%", f"bold {color}"),
                f" ({u_used:.1f} {unit_used} of {u_total:.1f} {unit_total})",
            )
        )
