"""The joblib htcondor UI configuration parameters."""

# Authors: Federico Raimondo <f.raimondo@fz-juelich.de>
# License: AGPL

MAX_ENTRIES = 200  # Maximum number of entries to display in the pad

# Colors for the different task states
COLOR_DONE = 56
COLOR_RUNNING = 9
COLOR_SENT = 4
COLOR_QUEUED = 239

# Color thresholds for the disk usage progress bar
COLOR_TRESHOLDS = [56, 4, 13, 9]
COLOR_NONE = 239

# Progress bar character
PBAR_CHAR = "■"
