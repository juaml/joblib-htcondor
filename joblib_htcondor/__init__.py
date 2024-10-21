"""Joblib HTCondor Backend."""

# Authors: Synchon Mandal <s.mandal@fz-juelich.de>
# License: AGPL

__all__ = ["register_htcondor"]


from typing import Union

from ._version import __version__


def register_htcondor(level: Union[int, str] = "WARNING") -> None:
    """Register htcondor backend into joblib.

    Parameters
    ----------
    level : int or {"DEBUG", "INFO", "WARNING", "ERROR"}
        The level of the messages to print. If string, it will be interpreted
        as elements of logging (default "WARNING").
    """
    from .backend import register
    from .logging import configure_logging
    configure_logging(level=level)
    register()
