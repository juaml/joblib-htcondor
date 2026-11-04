"""The joblib htcondor UI utils."""

# Authors: Federico Raimondo <f.raimondo@fz-juelich.de>
# License: AGPL

from datetime import timedelta

from rich.text import Text


# Colors for the different task states, from the 256 colors palette
COLOR_DONE = "color(56)"
COLOR_RUNNING = "color(9)"
COLOR_SENT = "color(4)"
COLOR_QUEUED = "color(239)"
# Brighter variant of COLOR_DONE, readable as text on the dark background
COLOR_DONE_TEXT = "#a87bff"

# Progress bar character
PBAR_CHAR = "■"


def space_to_unit(size: float) -> tuple[float, str]:
    """Convert size to human readable unit.

    Parameters
    ----------
    size : float
        The size to convert, in bytes.

    Returns
    -------
    tuple[float, str]
        The converted size and unit

    """
    for unit in ["B", "KB", "MB", "GB", "TB"]:
        if size < 1024:
            return size, unit
        size /= 1024
    return size, "PB"


def format_elapsed(elapsed: timedelta) -> str:
    """Format a time interval.

    Parameters
    ----------
    elapsed : datetime.timedelta
        The interval to format.

    Returns
    -------
    str
        The interval as days, hours, minutes and seconds.

    """
    total = max(0, int(elapsed.total_seconds()))
    days, rest = divmod(total, 86400)
    hours, rest = divmod(rest, 3600)
    minutes, seconds = divmod(rest, 60)
    out = f"{days}d " if days > 0 else ""
    return out + f"{hours:02d}h {minutes:02d}m {seconds:02d}s"


def progress_bar(
    length: int, done: int, running: int, sent: int, queued: int
) -> Text:
    """Build a progress bar with the task states.

    Parameters
    ----------
    length : int
        The length of the progress bar.
    done : int
        The number of done tasks.
    running : int
        The number of running tasks.
    sent : int
        The number of sent tasks.
    queued : int
        The number of queued tasks.

    Returns
    -------
    rich.text.Text
        The progress bar. It is all queued if there are no tasks.

    """
    counts = [done, running, sent, queued]
    total = sum(counts)
    if total == 0:
        return Text(PBAR_CHAR * length, style=COLOR_QUEUED)
    # Give each state its share, and the characters lost to rounding to the
    # states with the largest remainders
    shares = [c * length / total for c in counts]
    lens = [int(s) for s in shares]
    by_remainder = sorted(
        range(len(counts)), key=lambda i: shares[i] - lens[i], reverse=True
    )
    for i in by_remainder[: length - sum(lens)]:
        lens[i] += 1
    out = Text()
    colors = [COLOR_DONE, COLOR_RUNNING, COLOR_SENT, COLOR_QUEUED]
    for n, color in zip(lens, colors, strict=True):
        out.append(PBAR_CHAR * n, style=color)
    return out


def legend() -> Text:
    """Build the legend of the task states.

    Returns
    -------
    rich.text.Text
        The legend.

    """
    out = Text()
    states = [
        ("done", COLOR_DONE),
        ("running", COLOR_RUNNING),
        ("sent", COLOR_SENT),
        ("queued", COLOR_QUEUED),
    ]
    for name, color in states:
        out.append(f"{PBAR_CHAR} ", style=color)
        out.append(f"{name}  ")
    out.rstrip()
    return out
