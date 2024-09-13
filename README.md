# Joblib HTCondor Backend

This library provides HTCondor backend for joblib to queue jobs on a HTCondor.

## Installation

To install `joblib_htcondor`, run:

```bash
pip install -e .
```

## Usage

Registering the backend:

```python
from operator import neg

from joblib import Parallel, delayed, parallel_config
from joblib_htcondor import register_htcondor

register_htcondor()

with parallel_config(
    backend="htcondor",
    pool="head2.htc.inm7.de",
    n_jobs=5,
    verbose=1000,
):
  Parallel()(delayed(neg)(i + 1) for i in range(5))
```
