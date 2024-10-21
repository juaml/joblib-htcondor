"""The joblib htcondor UI module entrypoint."""

# Authors: Federico Raimondo <f.raimondo@fz-juelich.de>
# License: AGPL

import argparse
import curses
import logging
import traceback
from pathlib import Path
from typing import Union

from .config import (
    COLOR_DONE,
    COLOR_QUEUED,
    COLOR_RUNNING,
    COLOR_SENT,
    PBAR_CHAR,
)
from .main_window import MainWindow
from .uilogging import init_logging, logger
from .utils import (
    align_text,
)


mainwin: Union[MainWindow, None] = None


def color_test():
    """Test color."""
    mainwin.win.clear()  # type: ignore
    line = 0
    col = 0
    for i in range(0, 255):
        col = i % 10 + 3
        line = i // 10
        if col == 3:
            mainwin.win.addstr(line, 0, f"{line}", curses.color_pair(0))
        mainwin.win.addstr(line, col, "■", curses.color_pair(i))
    line = line + 1
    mainwin.win.addstr(line, 0, PBAR_CHAR * 10, curses.color_pair(COLOR_DONE))
    mainwin.win.addstr(
        line, 10, PBAR_CHAR * 15, curses.color_pair(COLOR_RUNNING)
    )
    mainwin.win.addstr(line, 25, PBAR_CHAR * 30, curses.color_pair(COLOR_SENT))
    mainwin.win.addstr(
        line, 55, PBAR_CHAR * 10, curses.color_pair(COLOR_QUEUED)
    )

    mainwin.win.refresh()

    while 1:
        c = mainwin.win.getch()
        if c == ord("q"):
            break


def align_test():
    """Test alignment."""
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
    """Set main UI."""
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
            mainwin = MainWindow(stdscr, args.path, args.refresh)
            mainwin.render()
            mainwin.event_handler()
        except curses.error as e:
            logger.error(traceback.format_exc())
            logger.error(e)


if __name__ == "__main__":

    def dir_or_file_path(string):
        """Handle directory or file and return path."""
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
        "--refresh",
        type=int,
        default=5,
        help="The refresh interval in seconds.",
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
        default=logging.INFO,
        help="The logging verbosity to use.",
    )
    args = parser.parse_args()
    init_logging(args.verbose)
    curses.wrapper(main_ui, args)
