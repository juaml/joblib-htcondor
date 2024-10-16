"""The joblib htcondor UI I/O."""

# Authors: Federico Raimondo <f.raimondo@fz-juelich.de>
# License: AGPL

import json
from datetime import datetime
from pathlib import Path
from typing import List, Optional

from ..backend import (
    TASK_STATUS_DONE,
    TASK_STATUS_QUEUED,
    TASK_STATUS_RUN,
    TASK_STATUS_SENT,
    _BackendMeta,
)
from .uilogging import logger


class MetaTree:
    def __init__(self, meta, fname):
    """Class for metadata management tree."""

        self.meta: _BackendMeta = meta
        self.children: List[MetaTree] = []
        if not isinstance(fname, Path):
            fname = Path(fname)
        self.fname = fname

    @classmethod
    def from_json(cls, fname):
        with open(fname, "r") as f:
        """Load object from JSON.

        Parameters
        ----------
        cls : MetaTree instance
            The type of instance to create.
        fname : pathlib.Path
            The path to the JSON file.

        Returns
        -------
        MetaTree instance
            The initialised object instance.

        """
            meta = _BackendMeta.from_json(json.load(f))
        return cls(meta, fname)

    def _update_from_list(self, all_meta):
        """Update from list of trees.

        Parameters
        ----------
        all_meta : list of MetaTree
            List of trees to update from.

        Raises
        ------
        OSError
            If file could not be opened.

        """
        # Reload the json file
        logger.debug(f"Updating {self.meta.uuid}")
        try:
            with self.fname.open("r") as fd:
                self.meta = _BackendMeta.from_json(json.load(fd))
        except Exception as e:
            logger.error(f"Error loading {self.fname}: {e}")
            return
        child_uuids = [c.meta.uuid for c in self.children]
        for tree in all_meta:
            if (
                tree.meta.parent is not None
                and tree.meta.parent == self.meta.uuid
                and tree.meta.uuid not in child_uuids
            ):
                logger.debug(f"Appending {tree.meta.uuid} to {self.meta.uuid}")
                self.children.append(tree)
        for c in self.children:
            c._update_from_list(all_meta)

    def update(self):
        """Update the tree.

        Raises
        ------
        OSError
            If the JSON file could not be opened.

        """
        all_meta = []
        for f in self.fname.parent.glob("*.json"):
            try:
                all_meta.append(MetaTree.from_json(f))
            except Exception as e:
                logger.error(f"Error loading {f}: {e}")
                continue
        self._update_from_list(all_meta)

    def __repr__(self) -> str:
        """Representation of object."""
        out = f"MetaTree({self.meta.uuid})"
        if len(self.children) > 0:
            out += f" [{len(self.children)}]-> \n"
            for c in self.children:
                out += f"\t{c}\n"
        return out

    def size(self) -> int:
        """Size of the tree."""
        return 1 + sum([c.size() for c in self.children])

    def depth(self) -> int:
        """Depth of the tree."""
        if len(self.children) == 0:
            return 1
        return 1 + max([c.depth() for c in self.children])

    def get_level_status_summary(self):
        """Get status summary of current level.

        Returns
        -------
        list
            The overall status summary of the current level as a list.

        """
        task_status = [x.get_status() for x in self.meta.task_status]
        n_queued = task_status.count(TASK_STATUS_QUEUED)
        n_sent = task_status.count(TASK_STATUS_SENT)
        n_running = task_status.count(TASK_STATUS_RUN)
        n_done = task_status.count(TASK_STATUS_DONE)
        n_tasks = self.meta.n_tasks
        this_level_summary = [
            {
                "done": n_done,
                "running": n_running,
                "sent": n_sent,
                "queued": n_queued,
                "total": n_tasks,
                "throttle": self.meta.throttle,
            }
        ]
        child_level_summary = [
            x.get_level_status_summary() for x in self.children
        ]
        if len(child_level_summary) > 0:
            n_child_levels = max([len(x) for x in child_level_summary])
            for i in range(n_child_levels):
                this_level_summary.append(
                    {
                        "done": 0,
                        "running": 0,
                        "sent": 0,
                        "queued": 0,
                        "total": 0,
                        "throttle": 0,
                    }
                )
                for t_child_summary in child_level_summary:
                    if i < len(t_child_summary):
                        for k, v in t_child_summary[i].items():
                            if k != "throttle":
                                this_level_summary[-1][k] += v
                            else:
                                this_level_summary[-1][k] = v
        return this_level_summary

    def get_task_status(self):
        """Get task status.

        Returns
        -------
        dict
            The overall status of the jobs as a dict.

        """
        task_status = [x.get_status() for x in self.meta.task_status]
        n_queued = task_status.count(TASK_STATUS_QUEUED)
        n_sent = task_status.count(TASK_STATUS_SENT)
        n_running = task_status.count(TASK_STATUS_RUN)
        n_done = task_status.count(TASK_STATUS_DONE)
        out = {
            "done": n_done,
            "running": n_running,
            "sent": n_sent,
            "queued": n_queued,
            "total": self.meta.n_tasks,
        }
        # logger.debug(f"Task status: {out}")
        return out

    def get_core_hours(self):
        """Get total core hours.

        Returns
        -------
        float
            Total core hours.

        """
        core_hours = 0
        for task in self.meta.task_status:
            if task.done_timestamp is not None:
                delta = task.done_timestamp - (
                    task.run_timestamp
                    if task.run_timestamp is not None
                    else task.sent_timestamp
                )
                core_hours += delta.total_seconds() / 3600 * task.request_cpus
        for c in self.children:
            core_hours += c.get_core_hours()
        return core_hours


def parse(root_fname):
    """Parse meta tree from file.

    Parameters
    ----------
    root_fname : pathlib.Path
        The path to the root JSON file.

    """
    logger.debug(f"Parsing {root_fname}")
    tree = MetaTree.from_json(root_fname)
    logger.debug(f"Updating tree {tree.meta.uuid}")
    tree.update()
    return tree
