"""Joblib HTCondor Backend."""

# Authors: Synchon Mandal <s.mandal@fz-juelich.de>
# License: AGPL
from . import delayed_submission

__all__ = ["register_htcondor"]


def register_htcondor() -> None:
    """Register htcondor backend into joblib."""
    from .backend import register
    register()
