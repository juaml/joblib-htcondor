import argparse
import curses
import logging
import traceback
from datetime import datetime
from pathlib import Path
from typing import Any, List, Tuple, Union

from ..backend import _BackendMeta
from .treeparser import MetaTree, parse
from .uilogging import logger, init_logging



# Configuration constants
MAX_ENTRIES = 200  # Maximum number of entries to display in the pad
COLOR_DONE = 56
COLOR_RUNNING = 9
COLOR_SENT = 4
COLOR_QUEUED = 239


def align_text(
    screen: Any,
    text: str,
    y: Union[str, int],
    x: Union[str, int],
    *args: Any,
    fill: int = 0,
    underline: int = -1,
) -> Tuple[int, int, int]:
    maxy, maxx = screen.getmaxyx()
    orig_text = text
    if len(args) > 1:
        raise ValueError("Too many arguments")
    if isinstance(y, int):
        if y < 0:
            y = maxy + y
    elif y == "center":
        y = maxy // 2
    else:
        raise ValueError("Invalid y value")

    if isinstance(x, int):
        if fill != 0:
            text = text.center(len(text) + fill)
        if x < 0:
            x = maxx - len(text) + x + 1
    elif x == "center":
        if fill != 0:
            if fill < 0:
                x = -fill
            else:
                x = 0
            text = text.center(maxx - x - 1)
        else:
            x = maxx // 2 - len(text) // 2
    else:
        raise ValueError("Invalid x value")

    try:
        if underline < 0:
            screen.addstr(y, x, text, *args)
        else:
            logger.debug(f"Underline {orig_text} at {underline}")
            t_x = x
            underlined = False
            for ch in text:
                if orig_text[underline] == ch and not underlined:
                    if len(args) == 0:
                        screen.addch(y, t_x, ch, curses.A_UNDERLINE)
                    else:
                        screen.addch(y, t_x, ch, args[0] | curses.A_UNDERLINE)
                    underlined = True
                else:
                    screen.addch(y, t_x, ch, *args)
                t_x += 1
    except curses.error:
        # This is a hack to avoid the error when we are printing a string that
        # finishes at the very end of the screen.
        pass
    return y, x, x + len(text) - 1


def table_header(
    screen: Any,
    beginy: int,
    beginx: int,
    length: int,
    text: str,
    align: str = "right",
):
    text_len = len(text)
    if length < text_len + 2:
        raise ValueError("Length too short for table header")
    if text_len == 0:
        for i in range(0, length):
            screen.addch(beginy, beginx + i, curses.ACS_HLINE)
        return
    if align == "left":
        screen.addch(beginy, beginx, curses.ACS_URCORNER)
        screen.addch(beginy, beginx + text_len + 1, curses.ACS_ULCORNER)
        for i in range(2 + text_len, length):
            screen.addch(beginy, beginx + i, curses.ACS_HLINE)
        screen.addstr(beginy, beginx + 1, text)
    elif align == "right":
        for i in range(0, length - text_len - 2):
            screen.addch(beginy, beginx + i, curses.ACS_HLINE)
        screen.addch(
            beginy, beginx + length - text_len - 2, curses.ACS_URCORNER
        )
        screen.addch(beginy, beginx + length - 1, curses.ACS_ULCORNER)
        screen.addstr(beginy, beginx + length - text_len - 1, text)


def table_cell(
    screen: Any,
    beginy: int,
    beginx: int,
    length: int,
    text: str,
    align: str = "right",
):
    text_len = len(text)
    if length < text_len + 2:
        raise ValueError("Length too short for table cell")
    if align == "right":
        text = text.rjust(length - 2)
    elif align == "center":
        text = text.center(length - 2)
    else:
        text = text.ljust(length - 2)
    screen.addstr(beginy, beginx + 1, text)


def progressbar(
    screen: Any,
    beginy: int,
    beginx: int,
    length: int,
    done: int,
    running: int,
    sent: int,
    queued: int,
):
    total = done + running + sent + queued
    if total == 0:
        total = 1
    done_len = round(done / total * length)
    running_len = round(running / total * length)
    submitted_len = round(sent / total * length)
    queued_len = round(queued / total * length)
    all_len = done_len + running_len + submitted_len + queued_len
    while all_len < length:
        if queued_len > 0:
            queued_len += 1
        elif submitted_len > 0:
            submitted_len += 1
        elif running_len > 0:
            running_len += 1
        else:
            done_len += 1
        all_len += 1
    while all_len > length:
        if done_len > 0:
            done_len -= 1
        elif running_len > 0:
            running_len -= 1
        elif submitted_len > 0:
            submitted_len -= 1
        else:
            queued_len -= 1
        all_len -= 1
    logger.debug(
        f"Rendering progress bar: {done_len} {running_len} {submitted_len} {queued_len}"
    )
    screen.addstr(
        beginy,
        beginx,
        "■" * done_len,
        curses.color_pair(COLOR_DONE),
    )
    screen.addstr(
        beginy,
        beginx + done_len,
        "■" * running_len,
        curses.color_pair(COLOR_RUNNING),
    )
    screen.addstr(
        beginy,
        beginx + done_len + running_len,
        "■" * submitted_len,
        curses.color_pair(COLOR_SENT),
    )
    screen.addstr(
        beginy,
        beginx + done_len + running_len + submitted_len,
        "■" * queued_len,
        curses.color_pair(COLOR_QUEUED),
    )


class Window:
    def __init__(self, y, x, window):
        self.y = y
        self.x = x
        self.h, self.w = window.getmaxyx()
        self.win = window

    def render(self):
        pass

    def refresh(self):
        self.win.refresh()

    def scroll_up(self):
        pass

    def scroll_down(self):
        pass

    def scroll_left(self):
        pass

    def scroll_right(self):
        pass

    def action(self, action):
        pass

    def resize(self, w=None, h=None):
        maxy, maxx = self.win.getmaxyx()
        if w is None:
            w = maxx - self.y
        if h is None:
            h = maxy - self.x
        self.h = h
        self.w = w
        self.win.resize(self.h, self.w)

    def get_menu(self):
        return []


class MainWindow(Window):
    def __init__(self, window, curpath=None):
        logger.debug("MAIN WINDOW: Initializing")

        self.win = window
        super().__init__(0, 0, window)
        self.subwindows: List[Window] = []
        self.win.attrset(curses.color_pair(5))
        self.curpath = curpath
        self.clear_tree()

        if self.curpath.is_file():
            self.parse_tree()

    def get_meta_dir(self):
        if self.curpath.is_file():
            return self.curpath.parent
        return self.curpath

    def clear_tree(self):
        self.curtree = None
        self.treesize = 0
        self.idx_selected = -1
        self.idx_first = 0

    def parse_tree(self, fpath=None):
        if fpath is not None:
            self.curpath = fpath
            self.curtree = parse(self.curpath)
        elif self.curtree is None and self.curpath.is_file():
            self.curtree = parse(self.curpath)
        else:
            self.curtree.update()
        self.treesize = self.curtree.size()

    def border(self):
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
        if len(self.subwindows) > 0:
            return self.subwindows[-1].get_menu()
        return [(0, "Open")]

    def update_frame(self):
        logger.log(level=9, msg="MAIN WINDOW: Updating frame")
        try:
            self.win.attrset(curses.color_pair(5))
            self.border()
            y, xl, xr = align_text(
                self.win,
                f" {datetime.now().strftime('%H:%M:%S')} ",
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
            # This is a hack to avoid the error when the terminal being resized too
            # fast that the update_frame() is called before the update_lines_cols()
            # is called.
            pass

    def resize(self):
        super().resize()
        for win in self.subwindows:
            win.resize()

    def _get_task_status(self, meta: _BackendMeta) -> dict:
        n_running = len(list(meta.shared_data_dir.glob("*.run")))
        sent_tasks = [
            int(x.stem.split("-")[1])
            for x in meta.shared_data_dir.glob("task-*.pickle")
            if "out" not in x.name
        ]
        n_sent = len(sent_tasks)
        if n_sent == 0:
            n_done = meta.n_tasks
        else:
            n_done = max(sent_tasks) - n_sent
            n_sent -= n_running
        n_queued = meta.n_tasks - n_running - n_done - n_sent
        out = {
            "done": n_done,
            "running": n_running,
            "sent": n_sent,
            "queued": n_queued,
            "total": meta.n_tasks,
        }
        logger.debug(f"Task status: {out}")
        return out

    def _render_tree_element(
        self,
        tree: MetaTree,
        y: int,
        level: int,
        idx_element: int,
        idx_skip: int,
    ) -> Tuple[int, int, int]:
        if y >= self.h - 2:
            return y, idx_element, idx_skip
        if idx_skip > 0:
            idx_skip -= 1
        else:
            # We are rendering this element
            if idx_element == self.idx_selected:
                self.win.attrset(curses.color_pair(15))
            else:
                self.win.attrset(curses.color_pair(5))

            uuid_text = tree.meta.uuid[
                : self.batch_field_size - 2 * (level + 1)
            ]
            if level > 0:
                self.win.addch(y, 2 + (level - 1) * 2, curses.ACS_LLCORNER)
                self.win.addch(y, 3 + (level - 1) * 2, curses.ACS_HLINE)
            self.win.addstr(
                y,
                2 + level * 2,
                uuid_text,
            )
            task_status = tree.get_task_status()
            table_cell(
                self.win,
                y,
                self.batch_field_size,
                8,
                f"{task_status["done"]}",
            )
            table_cell(
                self.win,
                y,
                self.batch_field_size + 8,
                8,
                f"{task_status["running"]}",
            )
            table_cell(
                self.win,
                y,
                self.batch_field_size + 16,
                8,
                f"{task_status["sent"]}",
            )
            table_cell(
                self.win,
                y,
                self.batch_field_size + 24,
                8,
                f"{task_status["queued"]}",
            )
            table_cell(
                self.win,
                y,
                self.batch_field_size + 32,
                8,
                f"{task_status["total"]}",
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

    def scroll_up(self):
        if self.idx_selected > 0:
            self.idx_selected = self.idx_selected - 1
            logger.debug(
                f"MAIN WINDOW: Scroll up: selected: {self.idx_selected}"
            )
            if self.idx_selected < self.idx_first:
                self.idx_first -= 1
                logger.debug(f"  Start reached: {self.idx_first}")

    def scroll_down(self):
        if self.idx_selected < self.treesize - 1:
            self.idx_selected = self.idx_selected + 1
            logger.debug(
                f"MAIN WINDOW: Scroll down: selected: {self.idx_selected}"
            )
            if self.idx_selected - self.idx_first >= self.h - 5:
                self.idx_first += 1
                logger.debug(f"  End reached: {self.idx_first}")

    def render_tree(self):
        # logger.debug(f"Tree: {self.curtree}")
        self.batch_field_size = 50 if self.w > 140 else 20
        self.win.attrset(curses.color_pair(9))
        table_header(
            self.win, 2, 1, self.batch_field_size, "Batch ID", align="left"
        )
        table_header(self.win, 2, self.batch_field_size, 8, "Done")
        table_header(self.win, 2, self.batch_field_size + 8, 8, "Run")
        table_header(self.win, 2, self.batch_field_size + 16, 8, "Sent")
        table_header(self.win, 2, self.batch_field_size + 24, 8, "Queued")
        table_header(self.win, 2, self.batch_field_size + 32, 8, "Total")
        table_header(self.win, 2, self.batch_field_size + 40, 8, "Throt.")
        table_header(
            self.win,
            2,
            self.batch_field_size + 48,
            self.w - self.batch_field_size - 48 - 2,
            "",
        )
        self.win.attrset(curses.color_pair(5))
        logger.debug(f"Rendering tree starting at {self.idx_first}")
        self._render_tree_element(
            self.curtree, 3, 0, idx_element=0, idx_skip=self.idx_first
        )

    def render(self):
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
        _continue = True
        self.win.timeout(1000)
        while _continue:
            self.update_frame()
            if len(self.subwindows) == 0:
                self.refresh()
            c = self.win.getch()
            logger.log(level=9, msg=f"Key pressed: {c}")
            if c == ord("q"):
                _continue = False  # Exit the while()
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
        logger.debug("MAIN WINDOW: Refreshing")
        if self.curtree is None:
            logger.debug("No tree to render")
        else:
            logger.debug("Rendering tree")
            self.parse_tree()
            self.render_tree()
        for win in self.subwindows:
            logger.debug(f"Rendering subwindow: {win}")
            win.render()
        for win in self.subwindows:
            win.refresh()
        self.win.refresh()


class OpenMenu(Window):
    def __init__(self, parent):
        logger.debug("OPEN MENU: Initializing")
        self.selected = 0
        self.i_first = 0
        self.parent = parent
        self.parent = parent

        self._load_entries()

        y = 1
        w = 57
        x = (self.parent.w - w) // 2
        h = min(len(self.entries) + 2, parent.h - 2)
        logger.debug(f"Creating window {h}x{w} at {y},{x}")
        window = self.parent.win.subwin(h, w, y, x)
        super().__init__(y=y, x=x, window=window)
        self.win.attrset(curses.color_pair(6))

    def _load_entries(self):
        self.selected = 0
        self.i_first = 0
        self.entries = []
        meta_dir = self.parent.get_meta_dir()
        logger.debug(f"Looking for entries in {meta_dir}")
        fnames = list(meta_dir.glob("*-l0.json"))
        for f in fnames:
            logger.debug(f"Adding {f.name}")
            self.entries.append(f.name)

    def resize(self):
        old_h = self.h
        self.x = (self.parent.w - self.w) // 2
        self.h = min(len(self.entries) + 2, self.parent.h - 2)
        self.win.resize(self.h, self.w)
        self.win.mvderwin(self.y, self.x)
        self.render()
        if old_h > self.h:
            self.parent.render()
        logger.debug(f"OPEN MENU: Resize: {self.h}x57 at {self.y},{self.x}")

    def render(self):
        logger.debug("OPEN MENU: Rendering")
        self.win.border()
        y, xl, xr = align_text(
            self.win,
            " Open Joblib Tree ",
            0,
            "center",
            curses.color_pair(6),
        )

    def scroll_up(self):
        if self.selected > 0:
            self.selected = self.selected - 1
            logger.debug(f"Scroll up: selected: {self.selected}")
            if self.selected < self.i_first:
                self.i_first -= 1
                logger.debug(f"  Start reached: {self.i_first}")

    def scroll_down(self):
        if self.selected < len(self.entries) - 1:
            self.selected = self.selected + 1
            logger.debug(f"Scroll down: selected: {self.selected}")
            if self.selected - self.i_first >= self.h - 2:
                self.i_first += 1
                logger.debug(f"  End reached: {self.i_first}")

    def refresh(self):
        logger.debug("OPEN MENU: Refreshing")
        max_entries = self.h - 2
        for y, i in enumerate(range(self.i_first, self.i_first + max_entries)):
            if i >= len(self.entries):
                break
            align_text(
                self.win,
                self.entries[i],
                y + 1,
                "center",
                curses.color_pair(15 if i == self.selected else 6),
                fill=-1,
            )
        super().refresh()

    def get_menu(self):
        return [(0, "Close"), (1, "Clear"), (0, "Refresh")]

    def action(self, action):
        logger.debug(f"Action: {action}")
        if action == 10:  # Enter
            logger.debug(f"Selected: {self.entries[self.selected]}")
            fpath = self.parent.get_meta_dir() / self.entries[self.selected]
            self.parent.parse_tree(fpath)
            logger.debug("Self closing menu")
            self.parent.subwindows.pop()
            self.parent.render()
        elif action == ord("c"):
            logger.debug("Closing window")
            self.parent.subwindows.pop()
            self.parent.render()
        elif action == ord("l"):
            self.cleardir()
        elif action == ord("r"):
            self._load_entries()
            self.resize()
            self.render()
        elif action == curses.KEY_UP:
            self.scroll_up()
            self.refresh()
        elif action == curses.KEY_DOWN:
            self.scroll_down()
            self.refresh()

    def cleardir(self):
        logger.debug("OPEN WINDOW: Clearing directory")
        self.parent.clear_tree()
        meta_dir = self.parent.get_meta_dir()
        data_dir = meta_dir.parent
        for f in meta_dir.glob("*.json"):
            t_data_dir = data_dir / f.stem
            if not t_data_dir.exists():
                logger.debug(f"Removing meta file: {f}")
                f.unlink()
        self._load_entries()
        self.resize()


mainwin: MainWindow = None



def color_test():
    mainwin.win.clear()
    line = 0
    col = 0
    for i in range(0, 255):
        col = i % 10 + 3
        line = i // 10
        if col == 3:
            mainwin.win.addstr(line, 0, f"{line}", curses.color_pair(0))
        mainwin.win.addstr(line, col, "■", curses.color_pair(i))
    line = line + 1
    mainwin.win.addstr(line, 0, "■" * 10, curses.color_pair(COLOR_DONE))
    mainwin.win.addstr(line, 10, "■" * 15, curses.color_pair(COLOR_RUNNING))
    mainwin.win.addstr(line, 25, "■" * 30, curses.color_pair(COLOR_SENT))
    mainwin.win.addstr(line, 55, "■" * 10, curses.color_pair(COLOR_QUEUED))

    mainwin.win.refresh()

    while 1:
        c = mainwin.win.getch()
        if c == ord("q"):
            break


def align_test():
    mainwin.win.clear()

    align_text(mainwin.win, "Top left", 0, 0, curses.color_pair(5))
    align_text(mainwin.win, "Top Center", 0, "center", curses.color_pair(5))
    align_text(mainwin.win, "Top Right", 0, -1, curses.color_pair(5))

    align_text(mainwin.win, "Middle left", "center", 0, curses.color_pair(5))
    align_text(
        mainwin.win, "Middle Center", "center", "center", curses.color_pair(5)
    )
    align_text(mainwin.win, "Middle Right", "center", -1, curses.color_pair(5))

    align_text(mainwin.win, "Bottom left", -1, 0, curses.color_pair(5))
    align_text(
        mainwin.win, "Bottom Center", -1, "center", curses.color_pair(5)
    )
    align_text(mainwin.win, "Bottom Right", -1, -1, curses.color_pair(5))

    mainwin.win.refresh()

    while 1:
        c = mainwin.win.getch()
        if c == ord("q"):
            break


def main_ui(stdscr, args):
    global mainwin
    global curdir
    logger.info("==== Starting UI ====")
    logger.debug(f"Can change color: {curses.can_change_color()}")
    logger.debug(f"Has color: {curses.has_colors()}")
    logger.debug(f"Has extended color: {curses.has_extended_color_support()}")
    logger.info(f"Path: {args.path}")

    n_colors = curses.COLORS
    curses.use_default_colors()
    logger.debug(f"Number of colors: {n_colors}")
    for i in range(0, curses.COLORS - 4):
        curses.init_pair(i, i, -1)

    curses.curs_set(0)
    # curses.init_pair(0, curses.COLOR_WHITE, -1)
    # curses.init_pair(1, curses.COLOR_RED, -1)
    # curses.init_pair(2, curses.COLOR_GREEN, -1)
    # curses.init_pair(3, curses.COLOR_BLUE, -1)
    
    # center_text(stdscr, "HTCondor Joblib Monitor", 0, curses.color_pair(5))
    # stdscr.addstr(1,0, "RED ALERT!", curses.color_pair(5))
    if args.test == "color":
        mainwin = MainWindow(stdscr, args.path)
        color_test()
    elif args.test == "align":
        mainwin = MainWindow(stdscr, args.path)
        align_test()
    else:
        try:
            mainwin = MainWindow(stdscr, args.path)
            mainwin.render()
            mainwin.event_handler()
        except Exception as e:
            logger.error(traceback.format_exc())
            logger.error(e)


if __name__ == "__main__":

    def dir_or_file_path(string):
        t_path = Path(string)
        if t_path.is_file():
            return t_path
        elif t_path.is_dir():
            t_path = t_path / ".jht-meta"
            if t_path.exists():
                return t_path
        raise ValueError(f"This is not a shared data dir: {string}")

    parser = argparse.ArgumentParser(description="HTCondor Joblib Monitor")
    parser.add_argument(
        "--test",
        help="Run the specified test.",
        choices=["color", "align"],
    )
    parser.add_argument(
        "--path",
        type=dir_or_file_path,
        required=True,
        help="Directory to look for meta files or a specific meta file.",
    )
    parser.add_argument(
        "--verbose",
        type=int,
        default=logging.DEBUG,
        help="The logging verbosity to use.",
    )
    args = parser.parse_args()
    init_logging(args.verbose)
    curses.wrapper(main_ui, args)
