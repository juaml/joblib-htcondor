import logging
from operator import neg

from joblib import Parallel, delayed, parallel_config

from joblib_htcondor import register_htcondor



def slowneg(x):
    import time

    time.sleep(2)
    return -x


register_htcondor()
logging.basicConfig(
    level=logging.WARNING,
    format="%(asctime)s %(name)s %(levelname)-8s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logging.getLogger("joblib_htcondor.backend").setLevel(logging.INFO)

n_tasks = 50

with parallel_config(
    backend="htcondor",
    pool="head2.htc.inm7.de",
    n_jobs=-1,
    throttle=10,
    request_cpus=1,
    request_memory="8GB",
    request_disk="1GB",
    python_path="/home/fraimondo/miniconda3/envs/ppc64le_dev/bin/python",
    extra_directives={"Requirements": 'Arch == "ppc64le"'},
    poll_interval=1,
):
    result = Parallel(pre_dispatch="all")(
        delayed(slowneg)(i + 1) for i in range(n_tasks)
    )
    print(result)
