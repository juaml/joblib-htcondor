"""The joblib htcondor UI logging."""

# Authors: Federico Raimondo <f.raimondo@fz-juelich.de>
# License: AGPL

import logging
from pathlib import Path


__all__ = ["init_logging"]


logger = logging.getLogger("joblib_htcondor.ui")


def init_logging(level: int, fname: Path) -> None:
    """Initialise logging for UI.

    The log goes to a file, as the terminal is used by the UI.

    Parameters
    ----------
    level : int
        The log level to set.
    fname : pathlib.Path
        The log file.

    """
    # Set logger level
    logger.setLevel(level)
    # Create log file handler
    fh = logging.FileHandler(fname)

    output_format = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    # Create logging formatter
    formatter = logging.Formatter(fmt=output_format)

    fh.setFormatter(formatter)  # set formatter
    # Add log file handler
    logger.addHandler(fh)
    # Set handler level
    fh.setLevel(level)
