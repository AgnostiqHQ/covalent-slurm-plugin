import covalent as ct
from covalent.executor import SlurmExecutor

executor = SlurmExecutor(
    username="wjc",
    address="perlmutter-p1.nersc.gov",
    ssh_key_file="/home/will/.ssh/nersc/nersc",
    cert_file="/home/will/.ssh/nersc/nersc-cert.pub",
    remote_workdir="/pscratch/sd/w/wjc",
    conda_env="covalent",
    poll_freq=60,
    prerun_commands=[
        "export COVALENT_CONFIG_DIR=/pscratch/sd/w/wjc/covalent",
        "export COVALENT_CACHE_DIR=/pscratch/sd/w/wjc/covalent",
    ],
    options={
        "cpus-per-task": 1,
        "qos": "regular",
        "time": "00:08:00",
        "account": "m4135",
        "constraint": "cpu",
    },
    use_srun=False,
)


@ct.electron(deps_pip=ct.DepsPip("numpy"))
def get_rand_sum_length(lo, hi):
    import numpy as np

    np.random.seed(1984)
    return np.random.randint(lo, hi)


@ct.electron(deps_pip=ct.DepsPip("numpy"))
def get_rand_num_slurm(lo, hi):
    import numpy as np

    np.random.seed(1984)
    return np.random.randint(lo, hi)


# Slurm sublattice
@ct.electron(executor=executor)
@ct.lattice
def add_n_random_nums(n, lo, hi):
    import numpy as np

    np.random.seed(1984)
    sum = 0
    for i in range(n):
        sum += get_rand_num_slurm(lo, hi)
    return sum


@ct.lattice(workflow_executor=executor)
def random_num_workflow(lo, hi):
    n = get_rand_sum_length(lo, hi)
    sum = add_n_random_nums(n, lo, hi)  # sublattice
    return sum


dispatch_id = ct.dispatch(random_num_workflow)(1, 3)
result = ct.get_result(dispatch_id, wait=True)
print(result.result)
