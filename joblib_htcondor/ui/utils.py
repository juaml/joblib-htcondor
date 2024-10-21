"""The joblib htcondor UI utils."""

# Authors: Federico Raimondo <f.raimondo@fz-juelich.de>
# License: AGPL

import curses
from typing import Any, Union

from .config import (
    COLOR_DONE,
    COLOR_QUEUED,
    COLOR_RUNNING,
    COLOR_SENT,
    PBAR_CHAR,
)
from .uilogging import logger


def space_to_unit(size: int) -> tuple[float, str]:
    """Convert size to human readable unit.

    Parameters
    ----------
    size : int
        The size to convert

    Returns
    -------
    tuple[int, str]
        The converted size and unit

    """
    for unit in ["B", "KB", "MB", "GB", "TB"]:
        if size < 1024:
            return size, unit
        size /= 1024  # type: ignore
    return size, "PB"


def align_text(  # noqa: C901
    screen: Any,
    text: str,
    y: Union[str, int],
    x: Union[str, int],
    *args: Any,
    fill: int = 0,
    underline: int = -1,
) -> tuple[int, int, int]:
    """Align text.

    Parameters
    ----------
    screen : Any
        The screen object to draw on.
    text : str
        The text to draw.
    y : Union[str, int]
        The y position to draw at. It can be an integer or "center".
    x : Union[str, int]
        The x position to draw at. It can be an integer or "center".
    *args : Any
        The arguments to pass to addstr.
    fill : int
        If > 0, fill the text with spaces on both sides to reach this length.
    underline : int
        The character to underline. If -1, do not underline any character.

    Returns
    -------
    y: int
        The y position of the text.
    x: int
        The x position of the text.
    x2: int
        The x position of the last character of the text.

    """
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
            logger.log(level=9, msg=f"Underline {orig_text} at {underline}")
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
                t_x += 1  # type: ignore
    except curses.error:
        # This is a hack to avoid the error when we are printing a string that
        # finishes at the very end of the screen.
        pass
    return y, x, x + len(text) - 1  # type: ignore


def table_header(
    screen: Any,
    beginy: int,
    beginx: int,
    length: int,
    text: str,
    align: str = "right",
) -> None:
    """Draw a table header cell.

    Parameters
    ----------
    screen : Any
        The screen object to draw on.
    beginy : int
        The y position to draw at.
    beginx : int
        The x position to draw at.
    length : int
        The length of the header.
    text : str
        The text to draw.
    align : str
        The alignment of the text.

    """
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
    """Draw a table cell.

    Parameters
    ----------
    screen : Any
        The screen object to draw on.
    beginy : int
        The y position to draw at.
    beginx : int
        The x position to draw at.
    length : int
        The length of the header.
    text : str
        The text to draw.
    align : str
        The alignment of the text.

    """
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
) -> None:
    """Draw a batch progress bar.

    Parameters
    ----------
    screen : Any
        The screen object to draw on.
    beginy : int
        The y position to draw at.
    beginx : int
        The x position to draw at.
    length : int
        The size of the progress bar.
    done : int
        The number of done tasks.
    running : int
        The number of running tasks.
    sent : int
        The number of sent tasks.
    queued : int
        The number of queued tasks.

    """
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
    logger.log(
        level=9,
        msg=f"Rendering progress bar: {done_len} {running_len} "
        f"{submitted_len} {queued_len}",
    )
    screen.addstr(
        beginy,
        beginx,
        PBAR_CHAR * done_len,
        curses.color_pair(COLOR_DONE),
    )
    screen.addstr(
        beginy,
        beginx + done_len,
        PBAR_CHAR * running_len,
        curses.color_pair(COLOR_RUNNING),
    )
    screen.addstr(
        beginy,
        beginx + done_len + running_len,
        PBAR_CHAR * submitted_len,
        curses.color_pair(COLOR_SENT),
    )
    screen.addstr(
        beginy,
        beginx + done_len + running_len + submitted_len,
        PBAR_CHAR * queued_len,
        curses.color_pair(COLOR_QUEUED),
    )
