"""The joblib htcondor UI Main Window."""

# Authors: Federico Raimondo <f.raimondo@fz-juelich.de>
# License: AGPL

import curses
import platform
import shutil
import time
from datetime import datetime

from .config import (
    COLOR_DONE,
    COLOR_NONE,
    COLOR_QUEUED,
    COLOR_RUNNING,
    COLOR_SENT,
    COLOR_TRESHOLDS,
    PBAR_CHAR,
)
from .monitor import TreeMonitor
from .open_window import OpenMenu
from .treeparser import MetaTree
from .uilogging import logger
from .utils import (
    align_text,
    progressbar,
    space_to_unit,
    table_cell,
    table_header,
)
from .window import Window


class MainWindow(Window):
    """Class for the main UI window."""

    def __init__(self, window, curpath=None, refresh=5):
        logger.debug("MAIN WINDOW: Initializing")

        self.win = window
        super().__init__(0, 0, window)
        self.subwindows: list[Window] = []
        self.win.attrset(curses.color_pair(5))
        self.curpath = curpath
        self.clear_tree()
        self.treemonitor = TreeMonitor(
            curpath=curpath,  # type: ignore
            refresh_interval=refresh,
        )
        self.treemonitor.start()
        self.refresh_interval = refresh

    def get_meta_dir(self):
        """Get metadata directory."""
        if self.curpath is None:
            return None
        if self.curpath.is_file():
            return self.curpath.parent
        return self.curpath

    def set_path(self, path):
        """Set path for monitor."""
        self.curpath = path
        self.treemonitor.set_path(path)
        self.refresh()

    def clear_tree(self):
        """Clear tree."""
        self.idx_selected = -1
        self.idx_first = 0

    def border(self):
        """Set border."""
        for x in range(1, self.w - 1):
            self.win.addch(0, x, curses.ACS_HLINE)
            self.win.addch(self.h - 1, x, curses.ACS_HLINE)
        for y in range(1, self.h - 1):
            self.win.addch(y, 0, curses.ACS_VLINE)
            self.win.addch(y, self.w - 1, curses.ACS_VLINE)
        self.win.addch(0, 0, curses.ACS_ULCORNER)
        self.win.addch(0, self.w - 1, curses.ACS_URCORNER)
        self.win.addch(self.h - 1, 0, curses.ACS_LLCORNER)
        try:
            self.win.addch(self.h - 1, self.w - 1, curses.ACS_LRCORNER)
        except curses.error:
            pass

    def get_menu(self):
        """Get context menu."""
        if len(self.subwindows) > 0:
            return self.subwindows[-1].get_menu()
        return [(0, "Open")]

    def update_frame(self):
        """Update window frame."""
        logger.log(level=9, msg="MAIN WINDOW: Updating frame")
        try:
            self.win.attrset(curses.color_pair(5))
            self.border()

            align_text(
                self.win,
                PBAR_CHAR + " queued ",
                1,
                -2,
                curses.color_pair(COLOR_QUEUED),
            )
            align_text(
                self.win,
                PBAR_CHAR + " sent",
                1,
                -12,
                curses.color_pair(COLOR_SENT),
            )
            align_text(
                self.win,
                PBAR_CHAR + " running",
                1,
                -19,
                curses.color_pair(COLOR_RUNNING),
            )
            align_text(
                self.win,
                PBAR_CHAR + " done",
                1,
                -29,
                curses.color_pair(COLOR_DONE),
            )
            y, xl, xr = align_text(
                self.win,
                f" {platform.node()} ",
                0,
                3,
            )
            self.win.addch(y, xl - 1, curses.ACS_URCORNER)
            self.win.addch(y, xr + 1, curses.ACS_ULCORNER)
            y, xl, xr = align_text(
                self.win,
                f" {datetime.now().strftime('%a %b %d %H:%M:%S %Y')} ",
                0,
                -3,
            )
            self.win.addch(y, xl - 1, curses.ACS_URCORNER)
            self.win.addch(y, xr + 1, curses.ACS_ULCORNER)
            y, xl, xr = align_text(
                self.win,
                " HTCondor Joblib Monitor ",
                0,
                "center",
            )
            self.win.addch(y, xl - 1, curses.ACS_URCORNER)
            self.win.addch(y, xr + 1, curses.ACS_ULCORNER)

            quit_text = "Quit"
            y, xl, xr = align_text(
                self.win,
                quit_text,
                -1,
                -3,
                fill=2,
                underline=0,
            )
            self.win.addch(y, xl - 1, curses.ACS_URCORNER)
            self.win.addch(y, xr + 1, curses.ACS_ULCORNER)
            curr_x = -3 - len(quit_text) - 4
            menu = self.get_menu()
            for i, t in menu:
                y, xl, xr = align_text(
                    self.win,
                    t,
                    -1,
                    curr_x,
                    fill=2,
                    underline=i,
                )
                self.win.addch(y, xl - 1, curses.ACS_URCORNER)
                self.win.addch(y, xr + 1, curses.ACS_ULCORNER)
                curr_x -= len(t) + 4

        except curses.error:
            # This is a hack to avoid the error when the terminal being resized
            # too fast that the update_frame() is called before the
            # update_lines_cols() is called.
            pass

    def resize(self):
        """Handle window resize."""
        super().resize()
        for win in self.subwindows:
            win.resize()

    def _render_tree_element(
        self,
        tree: MetaTree,
        y: int,
        level: int,
        idx_element: int,
        idx_skip: int,
    ) -> tuple[int, int, int]:
        """Render tree element."""
        if y >= self.h - 2:
            return y, idx_element, idx_skip
        if idx_skip > 0:
            idx_skip -= 1
        else:
            # We are rendering this element
            task_status = tree.get_task_status()
            if idx_element == self.idx_selected:
                self.win.attrset(curses.color_pair(15))
            else:
                if (
                    task_status["total"] > 0
                    and task_status["total"] == task_status["done"]
                ):
                    self.win.attrset(curses.color_pair(COLOR_DONE))

                else:
                    self.win.attrset(curses.color_pair(5))

            uuid_text = tree.meta.uuid[
                : self.batch_field_size - 2 * (level + 1)
            ]
            self.win.addstr(
                y,
                2,
                " " * (level * 2) + uuid_text,
            )

            if level > 0:
                self.win.addch(y, 2 + (level - 1) * 2, curses.ACS_LLCORNER)
                self.win.addch(y, 3 + (level - 1) * 2, curses.ACS_HLINE)

            table_cell(
                self.win,
                y,
                self.batch_field_size,
                8,
                f"{task_status['done']}",
            )
            table_cell(
                self.win,
                y,
                self.batch_field_size + 8,
                8,
                f"{task_status['running']}",
            )
            table_cell(
                self.win,
                y,
                self.batch_field_size + 16,
                8,
                f"{task_status['sent']}",
            )
            table_cell(
                self.win,
                y,
                self.batch_field_size + 24,
                8,
                f"{task_status['queued']}",
            )
            table_cell(
                self.win,
                y,
                self.batch_field_size + 32,
                8,
                f"{task_status['total']}",
            )

            table_cell(
                self.win,
                y,
                self.batch_field_size + 40,
                8,
                f"{tree.meta.throttle}",
            )

            progressbar(
                self.win,
                y,
                self.batch_field_size + 48,
                self.w - self.batch_field_size - 48 - 2,
                task_status["done"],
                task_status["running"],
                task_status["sent"],
                task_status["queued"],
            )
            y += 1
        idx_element += 1
        for c in tree.children:
            y, idx_element, idx_skip = self._render_tree_element(
                c, y, level + 1, idx_element, idx_skip
            )
        return y, idx_element, idx_skip

    def _render_summary_element(self, summary, y_start, title):
        """Render element summary."""
        table_cell(
            self.win,
            y_start,
            1,
            self.batch_field_size,
            title,
            align="left",
        )
        table_cell(
            self.win, y_start, self.batch_field_size, 8, str(summary["done"])
        )
        table_cell(
            self.win,
            y_start,
            self.batch_field_size + 8,
            8,
            str(summary["running"]),
        )
        table_cell(
            self.win,
            y_start,
            self.batch_field_size + 16,
            8,
            str(summary["sent"]),
        )
        table_cell(
            self.win,
            y_start,
            self.batch_field_size + 24,
            8,
            str(summary["queued"]),
        )
        table_cell(
            self.win,
            y_start,
            self.batch_field_size + 32,
            8,
            str(summary["total"]),
        )
        table_cell(
            self.win,
            y_start,
            self.batch_field_size + 40,
            8,
            str(summary["throttle"]),
        )
        progressbar(
            self.win,
            y_start,
            self.batch_field_size + 48,
            self.w - self.batch_field_size - 48 - 2,
            summary["done"],
            summary["running"],
            summary["sent"],
            summary["queued"],
        )
        y_start += 1

    def scroll_up(self):
        """Handle scroll up."""
        if self.idx_selected > 0:
            self.idx_selected = self.idx_selected - 1
            logger.debug(
                f"MAIN WINDOW: Scroll up: selected: {self.idx_selected}"
            )
            if self.idx_selected < self.idx_first:
                self.idx_first -= 1
                logger.debug(f"  Start reached: {self.idx_first}")

    def scroll_down(self):
        """Handle scroll down."""
        curtree = self.treemonitor.get_tree()
        header_size = curtree.depth() + 3
        if self.idx_selected < curtree.size() - 1:
            self.idx_selected = self.idx_selected + 1
            logger.debug(
                f"MAIN WINDOW: Scroll down: selected: {self.idx_selected}"
            )
            if self.idx_selected - self.idx_first >= self.h - header_size - 5:
                self.idx_first += 1
                logger.debug(f"  End reached: {self.idx_first}")

    def render_data(self, y_start=2):
        """Render data."""
        curtree = self.treemonitor.get_tree()
        self.batch_field_size = 50 if self.w > 140 else 20
        n_levels = self.render_summary(y_start)
        self.render_tree(y_start + n_levels + 2)
        self.win.attrset(curses.color_pair(9))
        elapsed = datetime.now() - curtree.meta.start_timestamp  # type: ignore
        days = int(elapsed.total_seconds() // 86400)
        hours = int(elapsed.total_seconds() % 86400) // 3600
        minutes = int(elapsed.total_seconds() % 3600) // 60
        seconds = int(elapsed.total_seconds() % 60)

        # Compute free/used space
        total, used, free = shutil.disk_usage(self.curpath)  # type: ignore

        r_used = used / total
        space_bar_len = 30
        logger.log(level=9, msg=f"Used space ratio: {r_used}")
        color_idx = int((r_used * 4) // 1)
        logger.log(level=9, msg=f"Color index: {color_idx}")
        color = COLOR_TRESHOLDS[color_idx]

        p_used = int(r_used * 100)

        l_used = int(round(r_used * space_bar_len, 1))
        l_free = space_bar_len - l_used
        logger.log(level=9, msg=f"Free space length: {l_free}")

        # Render free/used disk space with units
        u_used, unit_used = space_to_unit(used)
        u_total, unit_total = space_to_unit(total)
        label_free = f"{u_used:.1f} {unit_used} of {u_total:.1f} {unit_total}"
        self.win.addstr(
            self.h - 2,
            self.w - 2 - len(label_free),
            label_free,
            curses.color_pair(9),
        )

        # Render label with % used
        label_used = f"Disk used: {p_used}%"
        self.win.addstr(
            self.h - 2,
            self.w - space_bar_len - 4 - len(label_used) - len(label_free),
            label_used,
            curses.color_pair(9),
        )

        # Render progress bar, first free in grey, then used in color
        self.win.addstr(
            self.h - 2,
            self.w - space_bar_len - 3 + l_used - len(label_free),
            PBAR_CHAR * l_free,
            curses.color_pair(COLOR_NONE),
        )

        self.win.addstr(
            self.h - 2,
            self.w - space_bar_len - 3 - len(label_free),
            PBAR_CHAR * l_used,
            curses.color_pair(color),
        )

        text_elapsed = "Elapsed: "
        if days > 0:
            text_elapsed += f"{days}d "
        text_elapsed += f"{hours:02d}h {minutes:02d}m {seconds:02d}s "
        align_text(
            self.win,
            text_elapsed,
            self.h - 2,
            2,
        )
        # new_x = len(text_elapsed) + 3
        # text_batches = f"- Batches: {treesize}"
        # align_text(
        #     self.win,
        #     text_batches,
        #     self.h - 2,
        #     new_x,
        # )

        ts_update = curtree.last_update()  # type: ignore
        ts_update = ts_update.strftime("%d/%m/%Y %H:%M:%S")
        text_updated = f"- Updated: {ts_update}"

        # new_x = len(text_elapsed) + len(text_batches) + 3
        new_x = len(text_elapsed) + 2
        align_text(
            self.win,
            text_updated,
            self.h - 2,
            new_x,
        )

        if self.idx_first > 0:
            align_text(
                self.win,
                "▲",
                8,
                0,
            )
        if self.idx_first + self.h - n_levels - 5 < curtree.size():
            align_text(
                self.win,
                "▼",
                self.h - 3,
                0,
            )

    def render_summary(self, y_start=2):
        """Render summary."""
        curtree = self.treemonitor.get_tree()
        n_levels = curtree.depth()
        self.win.attrset(curses.color_pair(9))
        table_header(
            self.win,
            y_start,
            1,
            self.batch_field_size,
            "Summary",
            align="left",
        )
        table_header(self.win, y_start, self.batch_field_size, 8, "Done")
        table_header(self.win, y_start, self.batch_field_size + 8, 8, "Run")
        table_header(self.win, y_start, self.batch_field_size + 16, 8, "Sent")
        table_header(
            self.win, y_start, self.batch_field_size + 24, 8, "Queued"
        )
        table_header(self.win, y_start, self.batch_field_size + 32, 8, "Total")
        table_header(
            self.win, y_start, self.batch_field_size + 40, 8, "Throt."
        )
        table_header(
            self.win,
            y_start,
            self.batch_field_size + 48,
            self.w - self.batch_field_size - 48 - 2,
            "",
        )
        status_summary = curtree.get_level_status_summary()  # type: ignore
        y_start += 1
        for i, s in enumerate(status_summary):
            self.win.attrset(curses.color_pair(5))
            self._render_summary_element(
                s,
                y_start,
                title=f"Level {i}" if n_levels > 1 else "Total",
            )
            y_start += 1

        if n_levels > 1:
            total = {
                "done": 0,
                "running": 0,
                "sent": 0,
                "queued": 0,
                "total": 0,
                "throttle": "N/A",
            }
            for s in status_summary:
                for k, v in s.items():
                    if k != "throttle":
                        total[k] += v
            self.win.attrset(curses.color_pair(5))
            self._render_summary_element(total, y_start, title="Total")
            y_start += 1
            n_levels += 1
        return n_levels

    def render_tree(self, y_start=2):
        """Render tree."""
        # logger.debug(f"Tree: {self.curtree}")

        self.win.attrset(curses.color_pair(9))
        table_header(
            self.win,
            y_start,
            1,
            self.batch_field_size,
            "Batch ID",
            align="left",
        )
        table_header(self.win, y_start, self.batch_field_size, 8, "Done")
        table_header(self.win, y_start, self.batch_field_size + 8, 8, "Run")
        table_header(self.win, y_start, self.batch_field_size + 16, 8, "Sent")
        table_header(
            self.win, y_start, self.batch_field_size + 24, 8, "Queued"
        )
        table_header(self.win, y_start, self.batch_field_size + 32, 8, "Total")
        table_header(
            self.win, y_start, self.batch_field_size + 40, 8, "Throt."
        )
        table_header(
            self.win,
            y_start,
            self.batch_field_size + 48,
            self.w - self.batch_field_size - 48 - 2,
            "",
        )
        self.win.attrset(curses.color_pair(5))
        logger.log(level=9, msg=f"Rendering tree starting at {self.idx_first}")
        self._render_tree_element(
            self.treemonitor.get_tree(),  # type: ignore
            y_start + 1,
            0,
            idx_element=0,
            idx_skip=self.idx_first,
        )

    def render(self):
        """Render main window."""
        logger.debug("MAIN WINDOW: Rendering")
        self.win.clear()
        if self.w < 80 or self.h < 20:
            self.win.addstr(
                1,
                1,
                "Window too small. Please resize to at least 80x20",
                curses.color_pair(1),
            )
            self.refresh()
            return
        self.update_frame()

        self.refresh()

    def event_handler(self):
        """Handle event."""
        _continue = True
        self.win.timeout(1000)
        while _continue:
            self.update_frame()
            if (
                len(self.subwindows) == 0
                and (time.time() - self._last_refresh) > self.refresh_interval
            ):
                self.refresh()
            c = self.win.getch()
            logger.log(level=9, msg=f"Key pressed: {c}")
            if c == ord("q"):
                _continue = False  # Exit the while()
                self.treemonitor.stop()
            elif len(self.subwindows) > 0:
                self.subwindows[-1].action(c)
            elif c == ord("o"):
                if len(self.subwindows) == 0:
                    win = OpenMenu(self)
                    self.subwindows.append(win)
                    self.render()
            elif c == curses.KEY_UP:
                self.scroll_up()
                self.refresh()
            elif c == curses.KEY_DOWN:
                self.scroll_down()
                self.refresh()
            elif c == curses.KEY_RESIZE:
                curses.update_lines_cols()
                self.resize()
                self.render()

    def refresh(self):
        """Refresh main window."""
        logger.log(level=9, msg="MAIN WINDOW: Refreshing")
        self._last_refresh = time.time()
        if self.treemonitor.get_tree() is None:
            logger.log(level=9, msg="No tree to render")
        else:
            logger.log(level=9, msg="Rendering tree")
            self.render_data()
        for win in self.subwindows:
            logger.log(level=9, msg=f"Rendering subwindow: {win}")
            win.render()
        for win in self.subwindows:
            win.refresh()
        self.win.refresh()
