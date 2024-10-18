"""The joblib htcondor UI I/O."""

# Authors: Federico Raimondo <f.raimondo@fz-juelich.de>
# License: AGPL

import json
from pathlib import Path

from ..backend import (
    _BackendMeta,
    _TaskStatus,
)
from .uilogging import logger


class MetaTree:
    """Class for managing a metadata tree.

    Parameters
    ----------
    meta : _BackendMeta
        The backend metadata object to use as a root.
    fname : pathlib.Path
        The path to the JSON file

    """

    def __init__(self, meta: _BackendMeta, fname) -> None:
        self.meta: _BackendMeta = meta
        self.children: list[MetaTree] = []
        if not isinstance(fname, Path):
            fname = Path(fname)
        self.fname = fname

    @classmethod
    def from_json(cls: type["MetaTree"], fname: Path) -> "MetaTree":
        """Load object from JSON.

        Parameters
        ----------
        cls : MetaTree type
            The type of instance to create.
        fname : pathlib.Path
            The path to the JSON file.

        Returns
        -------
        MetaTree instance
            The initialised object instance.

        """
        with open(fname) as f:
            meta = _BackendMeta.from_json(json.load(f))
        return cls(meta, fname)

    def _update_from_list(self, all_meta: list["MetaTree"]) -> None:
        """Update from list of MetaTrees nodes.

        Parameters
        ----------
        all_meta : list of MetaTree
            List of nodes to update from.

        Raises
        ------
        OSError
            If file could not be opened.

        """
        # Reload the json file
        child_uuids = [c.meta.uuid for c in self.children]
        for tree in all_meta:
            if (
                tree.meta.parent is not None
                and tree.meta.parent == self.meta.uuid
                and tree.meta.uuid not in child_uuids
            ):
                logger.log(
                    level=9,
                    msg=f"Appending {tree.meta.uuid} to {self.meta.uuid}",
                )
                self.children.append(tree)
        for c in self.children:
            c._update_from_list(all_meta)

    def update(self) -> None:
        """Update the tree.

        Raises
        ------
        OSError
            If the JSON file could not be opened.

        """
        # Read all the json files in the directory
        logger.log(level=10, msg=f"Updating tree {self.meta.uuid}")
        try:
            with self.fname.open("r") as fd:
                self.meta = _BackendMeta.from_json(json.load(fd))
        except OSError as e:
            logger.error(f"Error loading {self.fname}: {e}")
            return
        logger.log(
            level=10, msg=f"Updating list of files for {self.meta.uuid}"
        )
        all_meta = []
        for f in self.fname.parent.glob("*.json"):
            try:
                all_meta.append(MetaTree.from_json(f))
            except OSError as e:
                logger.error(f"Error loading {f}: {e}")
                continue
        # Update the tree from this list of files
        logger.log(
            level=10, msg=f"Updating children from files {self.meta.uuid}"
        )
        self._update_from_list(all_meta)
        logger.log(level=10, msg=f"Tree updated {self.meta.uuid}")

    def __repr__(self) -> str:
        """Representation of object."""
        out = f"MetaTree({self.meta.uuid})"
        if len(self.children) > 0:
            out += f" [{len(self.children)}]-> \n"
            for c in self.children:
                out += f"\t{c}\n"
        return out

    def size(self) -> int:
        """Size of the tree.

        Returns
        -------
        int
            The size of the tree.
        """
        return 1 + sum([c.size() for c in self.children])

    def depth(self) -> int:
        """Depth of the tree.

        Returns
        -------
        int
            The depth of the tree.
        """
        if len(self.children) == 0:
            return 1
        return 1 + max([c.depth() for c in self.children])

    def get_level_status_summary(self) -> list[dict[str, int]]:
        """Get status summary of current level.

        Returns
        -------
        list
            The overall status summary of the current level as a list of dicts
            with all the status counters.

        """
        task_status = [x.get_status() for x in self.meta.task_status]
        n_queued = task_status.count(_TaskStatus.QUEUED)
        n_sent = task_status.count(_TaskStatus.SENT)
        n_running = task_status.count(_TaskStatus.RUN)
        n_done = task_status.count(_TaskStatus.DONE)
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

    def get_task_status(self) -> dict[str, int]:
        """Get task status.

        Returns
        -------
        dict
            The overall status of the jobs as a dict.

        """
        task_status = [x.get_status() for x in self.meta.task_status]
        n_queued = task_status.count(_TaskStatus.QUEUED)
        n_sent = task_status.count(_TaskStatus.SENT)
        n_running = task_status.count(_TaskStatus.RUN)
        n_done = task_status.count(_TaskStatus.DONE)
        out = {
            "done": n_done,
            "running": n_running,
            "sent": n_sent,
            "queued": n_queued,
            "total": self.meta.n_tasks,
        }
        # logger.debug(f"Task status: {out}")
        return out

    def get_core_hours(self) -> float:
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
                )  # type: ignore
                core_hours += delta.total_seconds() / 3600 * task.request_cpus
        for c in self.children:
            core_hours += c.get_core_hours()
        return core_hours


def parse(root_fname) -> MetaTree:
    """Parse meta tree from file.

    Parameters
    ----------
    root_fname : pathlib.Path
        The path to the root JSON file.

    Returns
    -------
    MetaTree instance
        The parsed tree

    """
    logger.log(level=10, msg=f"Parsing {root_fname}")
    tree = MetaTree.from_json(root_fname)
    tree.update()
    return tree
