# Joblib HTCondor Backend

This library provides HTCondor backend for joblib to queue jobs on a HTCondor.

## Installation

To install `joblib_htcondor`, run:

```bash
pip install joblib-htcondor
```

## Usage

Before using the HTCondor Backend, you need to import and register it so joblib knows about it

```python
from joblib_htcondor import register_htcondor

register_htcondor()

```

Once this is done, you can now use the htcondor backend as part of the `parallel_config` context:

```python
from operator import neg
from joblib import Parallel, delayed, parallel_config

with parallel_config(
    backend="htcondor",
    n_jobs=-1,
    request_cpus=1,
    request_memory="1Gb"
):
  Parallel()(delayed(neg)(i + 1) for i in range(5))
```

## Overhead considerations

Unlike multiprocessing or multithreading backends, the HTCondor backend is meant to be used with a large number of tasks with a considerable amount of compute time. One of the main aspects to consider is the _overhead time_, which is the time added to each task sent to joblib.

Internally, the backend will first _save_ the task in a binary file (using `cloudpickle`), that must be located in a directory shared across nodes, thus using the network infrastructure. Then, the backend will _submit_ the task to the HTCondor scheduler, which will _queue_ the task until a node is available. Once the task is _executed_, the backend will _load_ the result from the binary file. This is a considerable amount of time, compared to other backends.

Important: the use-case of the HTCondor backend is to parallelize a large number of tasks that are compute-intensive, and not I/O-bound. If the tasks are I/O-bound, the overhead time will be much higher than the time spent on the task itself.


## Data-transfer and shared disk space

As explained above, the HTCondor backend requires a shared disk space to save the tasks and results. This is because the tasks are saved in a binary file, and the results are loaded from it. This shared disk space must be accessible from all the nodes in the HTCondor pool.

The shared disk space can be a network file system (NFS), a distributed file system (e.g. CephFS), or a shared directory in a cluster (e.g. Lustre). The shared disk space must be mounted in the same path in all the nodes.

## Throttling and `n_jobs`

Unlike other backends, there is no specific semantics to the `n_jobs` parameter. This value is usually used to limit the amount of used resources to the maximum available, thus avoiding overloading the system. This parameter controls the behaviour of the queue. In an HTCondor cluster, we don't need to manage the queue, as this is done by the HTCondor scheduler, we just need to specify the resources required for each task. This leaves the `n_jobs` parameter useless. The only effect of setting this value is to control the default `pre_dispatch` parameter (set to `2 * n_jobs`), which controls how many tasks joblib dispatchs to the backend. Ideally, one would like all of the jobs to be dispatched at once, so the `n_jobs` parameter should be set to `-1`, which
will have the effect of setting `pre_dispatch` to `'all'`.

There is one caveat though: shared disk space. Each task that arrives to the backend will ocuppy disk space, creating copies of the data and code. This can be a problem if the disk space is limited. The way to control how many tasks are sent to the HTCondor scheduler is through the `throttle` parameter. This value will control how many tasks are sent to the scheduler at once. In HTCondor terms, this will limit how many taks are in IDLE and RUNNING states. If not set, and `n_jobs` is set to `-1`, the throttle will be set to 1000.


## Nested parallel calls

The HTCondor backend supports nested parallel calls. This means that you can use the HTCondor backend inside a parallel call. This is useful when you have a large number of tasks that can be parallelized, and each of these tasks can be parallelized as well. This is a common pattern in machine learning, where you have several folds in a cross-validation, and the trianing of each fold can be parallelized (e.g. hyperparameter tuning).

However, if the nested parallel call is not compute-intensive, the overhead considerations must be taken into account. Importantly, the parent task will be waiting for the child task to complete. If not properly configured, the parents tasks could occupy several resources and the child tasks could be queued for a long time.

By default, nested parallel calls are disabled. To enable them, set the `max_recursion_level` parameter to `1` or more in the `parallel_config` context.

To control the ratio of parent-to-child tasks, one can set the `throttle` parameter to a value that limits considerably the number of parents tasks. This paremeter can also take a list of values, each one representing the corresponding throttling value for each recursion level. For example, if we need to dispatch 5000 tasks, and that each task dispatchs another 100 tasks, we can set `throttle=[10, 100]` to limit the number of parent tasks to 10, and the number of child tasks to 100. In this case, if we have 1000 slots available, we will limit the maximum number of parents tasks running to 10, leaving 990 for child tasks.


## Configuration

These are the current HTCondor Backend parameters that can be set in the `parallel_config` context:

### HTCondor-specific parameters:

- `request_cpus`: Number of CPUs required for each task. Equivalent to the `request_cpus` parameter in the submit file.
- `request_memory`: Amount of memory required for each task. Equivalent to the `request_memory` parameter in the submit file.
- `pool`: the pool to submit the jobs to. This is equivalent to the `-name` parameter on the `condor_submit` command.
- `schedd`: `the htcondor2.Schedd` to submit the jobs to.
- `universe`: the universe to submit the jobs to. Equivalent to the `universe` parameter in the submit file.
- `python_path`: the path to the python executable. If not specified, it will use the current python executable.
- `request_disk`: Space requested for the scratch disk. Equivalent to the `request_disk` parameter in the submit file.
- `initial_dir`: the initial directory where the job will be executed. Equivalent to the `initial_dir` parameter in the submit file. Defaults to the current directory.
- `extra_directives`: Extra directives to be added to the submit file. This is a dictionary where the key is the directive name and the value is the directive value. For example, to set the `Requirements` directive to `'Arch == "ppc64le"`, set `extra_directives={"Requirements": "Arch == "ppc64le""}`.

### Parameters that control the behaviour of the backend:
- `log_dir_prefix`: Prefix for each of the HTCondor log files. If not specified, it will create a `logs` directory in the `initial_dir`.
- `poll_interval`: Minimum time (in seconds) between polls to the HTCondor scheduler. Defaults to 5 seconds. A lower value will increase the load on the HTCondor collector as well as the filesystem, but will increase reactivity of the backend. A higher value will decrease the load, but the backend will take longer to react to changes in the queue. Important: there will be a poller for each nested parallel call, so the load on the system will be multiplied by the number of nested parallel calls.
- `shared_data_dir`: Directory where the tasks and results will be saved. This directory must be shared across all the nodes in the HTCondor pool. If not specified, it will use a `joblib_htcondor_shared_data` directory inside the current working directory.
- `worker_log_level`: Log level for the worker. Defaults to `INFO`.
- `throttle`: Throttle the number of jobs submitted at once. If list, the first element is the throttle for the current level and the rest are for the nested levels (default None).
- `batch_size`: Currently under development
- `max_recursion_level`: Maximum recursion level for nested parallel calls. Defaults to 0 (no nested parallel calls allowed).
- `export_metadata`: Export metadata to be used with the UI. Defaults to False.
