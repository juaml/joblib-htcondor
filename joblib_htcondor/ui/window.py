"""The joblib htcondor UI base Window class."""

# Authors: Federico Raimondo <f.raimondo@fz-juelich.de>
# License: AGPL

from typing import Any


class Window:
    """Base class for a UI window.

    Parameters
    ----------
    y : int
        The y coordinate of the window.
    x : int
        The x coordinate of the window.
    window : curses.window
        The curses window object.

    """

    def __init__(self, y: int, x: int, window: Any) -> None:
        self.y = y
        self.x = x
        self.h, self.w = window.getmaxyx()
        self.win = window

    def render(self) -> None:
        """Render the window."""
        pass

    def refresh(self) -> None:
        """Refresh the window."""
        self.win.refresh()

    def scroll_up(self) -> None:
        """Handle scroll up."""
        pass

    def scroll_down(self) -> None:
        """Handle scroll down."""
        pass

    def scroll_left(self) -> None:
        """Handle scroll left."""
        pass

    def scroll_right(self) -> None:
        """Handle scroll right."""
        pass

    def action(self, action) -> None:
        """Trigger action.

        Parameters
        ----------
        action : str
            The action to trigger.

        """
        pass

    def resize(self, w=None, h=None) -> None:
        """Resize window.

        Parameters
        ----------
        w : int
            The new width. If None, the maximum width is used.
        h : int
            The new height. If None, the maximum height is used.

        """
        maxy, maxx = self.win.getmaxyx()
        if w is None:
            w = maxx - self.y
        if h is None:
            h = maxy - self.x
        self.h = h
        self.w = w
        self.win.resize(self.h, self.w)

    def get_menu(self) -> list[tuple[int, str]]:
        """Get context menu.

        Returns
        -------
        list
            The context menu. Each item is a tuple with the first element
            being the index of the letter that triggers the action and the
            second the name of the action.

        """
        return []
