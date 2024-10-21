"""The joblib htcondor UI tree monitor."""

# Authors: Federico Raimondo <f.raimondo@fz-juelich.de>
# License: AGPL

import threading
import time
import traceback
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

from .treeparser import MetaTree, parse
from .uilogging import logger


class TreeMonitor:
    """Class for tree monitor.

    Use this class to monitor a tree for changes, regularly updating the tree
    object.

    Parameters
    ----------
    curpath : Path
        The path to the json file to monitor as the root of the tree.
    refresh_interval : int, optional
        The minimum interval in seconds between updates.

    """

    def __init__(self, curpath: Path, refresh_interval: int = 5) -> None:
        self._curpath = curpath
        self._refresh_interval = refresh_interval
        self._last_update = time.time()
        self._curtree = None
        self._lock = threading.Lock()
        self._continue = True
        self._parse_tree()

    def set_path(self, curpath: Path) -> None:
        """Change the path of the tree to monitor.

        curpath : Path
            The path to the json file to monitor as the root of the tree.

        """
        with self._lock:
            self._curpath = curpath
            self._curtree = None
        self._parse_tree()

    def run(self) -> None:
        """Monitor the tree, udpating it regularly."""
        logger.info("Starting tree monitor")
        while self._continue:
            now = time.time()
            if now - self._last_update > self._refresh_interval:
                logger.debug("Checking for updates")
                if self._curpath.is_file():
                    self._parse_tree()
                self._last_update = now
            time.sleep(max(0, 0.1 - (time.time() - now)))

    def _parse_tree(self) -> None:
        """Parse tree for monitor."""
        # logger.debug("P(Lock) for parsing")
        with self._lock:
            parse_from = self._curpath
        # logger.debug("V(Lock) for parsing")

        # Now try to parse an update
        if parse_from.is_dir():
            return
        logger.debug(f"Parsing tree from: {parse_from}")
        try:
            newtree = parse(parse_from)

            # logger.debug("P(Lock) for updating")
            with self._lock:
                self._curtree = newtree
            # logger.debug("V(Lock) for updating")
        except Exception as e:  # noqa: BLE001
            logger.error(
                f"Error parsing tree from {parse_from}: "
                f"{e}\n{traceback.format_exc()}"
            )

    def get_tree(self) -> MetaTree:
        """Get the current tree.

        Returns
        -------
        MetaTree instance
            The current tree.

        """
        # logger.debug("P(Lock) for get_tree")
        with self._lock:
            treecopy = self._curtree
        # logger.debug("V(Lock) for get_tree")
        return treecopy  # type: ignore

    def start(self) -> None:
        """Start monitor."""
        self._monitor_executor = ThreadPoolExecutor(
            max_workers=1,
            thread_name_prefix="ui_treepoller",
        )
        self._monitor_executor.submit(self.run)

    def stop(self) -> None:
        """Stop monitor."""
        if self._monitor_executor is not None:
            self._continue = False
            self._monitor_executor.shutdown(wait=False, cancel_futures=True)
            self._monitor_executor = None
