"""Logging utilities for joblib htcondor."""

# Authors: Federico Raimondo <f.raimondo@fz-juelich.de>
#          Sami Hamdan <s.hamdan@fz-juelich.de>
# License: AGPL

import logging
import sys
import warnings
from pathlib import Path
from typing import Optional, Union


__all__ = ["logger"]


logger = logging.getLogger("joblib_htcondor.backend")


_logging_types = {
    "DEBUG": logging.DEBUG,
    "INFO": logging.INFO,
    "WARNING": logging.WARNING,
    "ERROR": logging.ERROR,
}


def _close_handlers(logger: logging.Logger) -> None:
    """Safely close relevant handlers for logger.

    Parameters
    ----------
    logger : logging.logger
        The logger to close handlers for.

    """
    for handler in list(logger.handlers):
        if isinstance(handler, (logging.FileHandler, logging.StreamHandler)):
            if isinstance(handler, logging.FileHandler):
                handler.close()
            logger.removeHandler(handler)


def configure_logging(
    level: Union[int, str] = "WARNING",
    fname: Optional[Union[str, Path]] = None,
    overwrite: Optional[bool] = None,
    output_format=None,
) -> None:
    """Configure the logging functionality.

    Parameters
    ----------
    level : int or {"DEBUG", "INFO", "WARNING", "ERROR"}
        The level of the messages to print. If string, it will be interpreted
        as elements of logging (default "WARNING").
    fname : str or pathlib.Path, optional
        Filename of the log to print to. If None, stdout is used
        (default None).
    overwrite : bool, optional
        Overwrite the log file (if it exists). Otherwise, statements
        will be appended to the log (default). None is the same as False,
        but additionally raises a warning to notify the user that log
        entries will be appended (default None).
    output_format : str, optional
        Format of the output messages. See the following for examples:
        https://docs.python.org/dev/howto/logging.html
        e.g., ``"%(asctime)s - %(levelname)s - %(message)s"``.
        If None, default string format is used
        (default ``"%(asctime)s - %(name)s - %(levelname)s - %(message)s"``).

    """
    _close_handlers(logger)  # close relevant logger handlers

    # Set logging level
    if isinstance(level, str):
        level = _logging_types[level]

    # Set logging output handler
    if fname is not None:
        # Convert str to Path
        if not isinstance(fname, Path):
            fname = Path(fname)
        if fname.exists() and overwrite is None:
            warnings.warn(
                f"File ({fname.absolute()!s}) exists. "
                "Messages will be appended. Use overwrite=True to "
                "overwrite or overwrite=False to avoid this message.",
                stacklevel=2,
            )
            overwrite = False
        mode = "w" if overwrite else "a"
        lh = logging.FileHandler(fname, mode=mode)
    else:
        lh = logging.StreamHandler(stream=sys.stdout)  # type: ignore

    # Set logging format
    if output_format is None:
        output_format = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    # Create logging formatter
    formatter = logging.Formatter(fmt=output_format)

    lh.setFormatter(formatter)  # set formatter
    logger.addHandler(lh)  # set handler
    logger.setLevel(level)  # set level
