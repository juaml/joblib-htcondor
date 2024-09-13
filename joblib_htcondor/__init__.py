"""Joblib HTCondor Backend."""

# Authors: Synchon Mandal <s.mandal@fz-juelich.de>
# License: AGPL

__all__ = ["register_htcondor"]


def register_htcondor() -> None:
    """Register htcondor backend into joblib."""
    from .backend import register
    register()
