"""The joblib htcondor UI Open Menu window."""

# Authors: Federico Raimondo <f.raimondo@fz-juelich.de>
# License: AGPL

import curses

from .uilogging import logger
from .utils import (
    align_text,
)
from .window import Window


class OpenMenu(Window):
    """Class for context menu."""

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
        """Load entries."""
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
        """Handle resize."""
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
        """Render context menu."""
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
        """Handle scroll up."""
        if self.selected > 0:
            self.selected = self.selected - 1
            logger.debug(f"Scroll up: selected: {self.selected}")
            if self.selected < self.i_first:
                self.i_first -= 1
                logger.debug(f"  Start reached: {self.i_first}")

    def scroll_down(self):
        """Handle scroll down."""
        if self.selected < len(self.entries) - 1:
            self.selected = self.selected + 1
            logger.debug(f"Scroll down: selected: {self.selected}")
            if self.selected - self.i_first >= self.h - 2:
                self.i_first += 1
                logger.debug(f"  End reached: {self.i_first}")

    def refresh(self):
        """Refresh context menu."""
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
        """Get context menu."""
        return [(0, "Close"), (1, "Clear"), (0, "Refresh")]

    def action(self, action):
        """Trigger action."""
        logger.debug(f"Action: {action}")
        if action == 10:  # Enter
            logger.debug(f"Selected: {self.entries[self.selected]}")
            fpath = self.parent.get_meta_dir() / self.entries[self.selected]
            self.parent.set_path(fpath)
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
        """Clear directory."""
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
