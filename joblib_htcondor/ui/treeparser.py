# %%
import json
from datetime import datetime
from pathlib import Path
from typing import List, Optional

from ..backend import _BackendMeta
from .uilogging import logger


TASK_STATUS_QUEUED = 0
TASK_STATUS_SENT = 1
TASK_STATUS_RUN = 2
TASK_STATUS_DONE = 3


class MetaTree:
    def __init__(self, meta, fname):
        self.meta: _BackendMeta = meta
        self.children: List[MetaTree] = []
        self.task_status = []
        self.fname = fname

    @classmethod
    def from_json(cls, fname):
        with open(fname, "r") as f:
            meta = _BackendMeta.from_json(json.load(f))
        return cls(meta, fname)

    def _update_from_list(self, all_meta):
        # Reload the json file
        self.meta = _BackendMeta.from_json(json.load(self.fname.open("r")))
        child_uuids = [c.meta.uuid for c in self.children]
        for tree in all_meta:
            if (
                tree.meta.parent == self.meta.uuid
                and tree.meta.uuid not in child_uuids
            ):
                self.children.append(tree)
        for c in self.children:
            c._update_from_list(all_meta)

    def update(self):
        all_meta = [
            MetaTree.from_json(f) for f in self.fname.parent.glob("*.json")
        ]
        self._update_from_list(all_meta)

    def __repr__(self) -> str:
        out = f"MetaTree({self.meta.uuid})"
        if len(self.children) > 0:
            out += f" [{len(self.children)}]-> \n"
            for c in self.children:
                out += f"\t{c}\n"
        return out

    def size(self) -> int:
        return 1 + sum([c.size() for c in self.children])

    def get_task_status(self):
        if not self.meta.shared_data_dir.exists():
            # all tasks are done
            return {
                "done": self.meta.n_tasks,
                "running": 0,
                "sent": 0,
                "queued": 0,
                "total": self.meta.n_tasks,
            }
        run_files_id = [
            int(f.stem.split("-")[1])
            for f in self.meta.shared_data_dir.glob("*.run")
        ]
        pickle_files_id = [
            int(f.stem.split("-")[1])
            for f in self.meta.shared_data_dir.glob("*.pickle")
            if "out" not in f.stem
        ]
        # If we have more tasks queued, extend the list with the missing tasks
        if len(self.task_status) < self.meta.n_tasks:
            self.task_status.extend(
                [TASK_STATUS_QUEUED]
                * (self.meta.n_tasks - len(self.task_status))
            )
        n_queued = 0
        n_sent = 0
        n_running = 0
        n_done = 0
        # logger.debug(f"Pre status: {self.task_status}")
        for i, task_id in enumerate(range(1, self.meta.n_tasks + 1)):
            cur_status = self.task_status[i]

            if cur_status == TASK_STATUS_DONE:
                # Done tasks are done
                n_done += 1
                continue
            elif cur_status == TASK_STATUS_RUN:
                if task_id not in run_files_id:
                    # If it was running but no run file present, it's done
                    self.task_status[i] = TASK_STATUS_DONE
                    n_done += 1
                else:
                    # If we have a run file, it's running
                    n_running += 1
            elif cur_status == TASK_STATUS_SENT:
                if task_id in run_files_id:
                    # If we have a run file, its ruinning
                    self.task_status[i] = TASK_STATUS_RUN
                    n_running += 1
                elif task_id not in pickle_files_id:
                    # If we don't have a pickle file, we missed the run
                    # but it's done.
                    self.task_status[i] = TASK_STATUS_DONE
                    n_done += 1
                else:
                    # If we have a pickle file, it's sent
                    n_sent += 1
            elif cur_status == TASK_STATUS_QUEUED:
                if task_id in run_files_id:
                    # If we have a run file, its ruinning
                    self.task_status[i] = TASK_STATUS_RUN
                    n_running += 1
                elif task_id in pickle_files_id:
                    # If we have a pickle file, it's sent
                    self.task_status[i] = TASK_STATUS_SENT
                    n_sent += 1
                else:
                    # If we have nothing, it's queued
                    n_queued += 1
        # Border case: task was done but we missed it because no files
        # are left so we set it as queued
        if max(self.task_status) > TASK_STATUS_QUEUED:
            for i, s in enumerate(self.task_status):
                if s == TASK_STATUS_QUEUED:
                    # We found a queue task before a sent task, this is done
                    self.task_status[i] = TASK_STATUS_DONE
                else:
                    # We found the first non-queued task, we are done
                    break

        # logger.debug(f"Post status: {self.task_status}")
        out = {
            "done": n_done,
            "running": n_running,
            "sent": n_sent,
            "queued": n_queued,
            "total": self.meta.n_tasks,
        }
        # logger.debug(f"Task status: {out}")
        return out


def parse(root_fname):
    tree = MetaTree.from_json(root_fname)
    tree.update()
    return tree


# %%
