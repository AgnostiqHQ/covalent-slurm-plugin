&nbsp;

<div align="center">

<img src="https://raw.githubusercontent.com/AgnostiqHQ/covalent-slurm-plugin/main/assets/slurm_readme_banner.jpg" width=150%>

[![covalent](https://img.shields.io/badge/covalent-0.177.0-purple)](https://github.com/AgnostiqHQ/covalent)
[![python](https://img.shields.io/pypi/pyversions/covalent-slurm-plugin)](https://github.com/AgnostiqHQ/covalent-slurm-plugin)
[![tests](https://github.com/AgnostiqHQ/covalent-slurm-plugin/actions/workflows/tests.yml/badge.svg)](https://github.com/AgnostiqHQ/covalent-slurm-plugin/actions/workflows/tests.yml)
[![codecov](https://codecov.io/gh/AgnostiqHQ/covalent-slurm-plugin/branch/main/graph/badge.svg?token=QNTR18SR5H)](https://codecov.io/gh/AgnostiqHQ/covalent-slurm-plugin)
[![apache](https://img.shields.io/badge/License-Apache_License_2.0-blue)](https://www.apache.org/licenses/LICENSE-2.0)

</div>

## Covalent Slurm Plugin

Covalent is a Pythonic workflow tool used to execute tasks on advanced computing hardware. This executor plugin interfaces Covalent with HPC systems managed by [Slurm](https://slurm.schedmd.com/documentation.html). For workflows to be deployable, users must have SSH access to the Slurm login node, writable storage space on the remote filesystem, and permissions to submit jobs to Slurm.

## Installation

To use this plugin with Covalent, simply install it using `pip`:

```
pip install covalent-slurm-plugin
```

On the remote system, the Python version in the environment you plan to use must match that used when dispatching the calculations. Additionally, the remote system's Python environment must have the base [covalent package](https://github.com/AgnostiqHQ/covalent) installed (e.g. `pip install covalent`).


For development use the following to build it.

- Create a python environment, and in that environment install `pip install build`
- Go to the source folder, and run

```bash
python -m build
```


## Usage

The following shows an example of a Covalent [configuration](https://covalent.readthedocs.io/en/latest/how_to/config/customization.html) that is modified to support Slurm:

```console
[executors.slurm]
username = "user"
address = "login.cluster.org"
ssh_key_file = "/home/user/.ssh/id_rsa"
remote_workdir = "/scratch/user"
cache_dir = "/tmp/covalent"

[executors.slurm.options]
nodes = 1
ntasks = 4
cpus-per-task = 8
constraint = "gpu"
gpus = 4
qos = "regular"

[executors.slurm.srun_options]
cpu_bind = "cores"
gpus = 4
gpu-bind = "single:1"
```

The first stanza describes default connection parameters for a user who can connect to the Slurm login node using, for example:

```console
ssh -i /home/user/.ssh/id_rsa user@login.cluster.org
```

The second and third stanzas describe default parameters for `#SBATCH` directives and default parameters passed directly to `srun`, respectively.

This example generates a script containing the following preamble:

```console
   #!/bin/bash
   #SBATCH --nodes=1
   #SBATCH --ntasks=4
   #SBATCH --cpus-per-task=8
   #SBATCH --constraint=gpu
   #SBATCH --gpus=4
   #SBATCH --qos=regular
```

and subsequent workflow submission with:

```console
   srun --cpu_bind=cores --gpus=4 --gpu-bind=single:1
```

To use the configuration settings, an electron’s executor must be specified with a string argument, in this case:

```python
   import covalent as ct

   @ct.electron(executor="slurm")
   def my_task(x, y):
       return x + y
```

Alternatively, passing a `SlurmExecutor` instance enables custom behavior scoped to specific tasks. Here, the executor's `prerun_commands` and `postrun_commands` parameters can be used to list shell commands to be executed before and after submitting the workflow. These may include any additional `srun` commands apart from workflow submission. Commands can also be nested inside the submission call to `srun` by using the `srun_append` parameter.

More complex jobs can be crafted by using these optional parameters. For example, the instance below runs a job that accesses CPU and GPU resources on a single node, while profiling GPU usage via `nsys` and issuing complementary commands that pause/resume the central hardware counter.

```python
   executor = ct.executor.SlurmExecutor(
       remote_workdir="/scratch/user/experiment1",
       options={
           "qos": "regular",
           "time": "01:30:00",
           "nodes": 1,
           "constraint": "gpu",
       },
       prerun_commands=[
           "module load package/1.2.3",
           "srun --ntasks-per-node 1 dcgmi profile --pause"
       ],
       srun_options={
           "n": 4,
           "c": 8,
           "cpu-bind": "cores",
           "G": 4,
           "gpu-bind": "single:1"
       },
       srun_append="nsys profile --stats=true -t cuda --gpu-metrics-device=all",
       postrun_commands=[
           "srun --ntasks-per-node 1 dcgmi profile --resume",
       ]
   )

   @ct.electron(executor=executor)
   def my_custom_task(x, y):
       return x + y
```

Here the corresponding submit script contains the following commands:

```console
   module load package/1.2.3
   srun --ntasks-per-node 1 dcgmi profile --pause

   srun -n 4 -c 8 --cpu-bind=cores -G 4 --gpu-bind=single:1 \
   nsys profile --stats=true -t cuda --gpu-metrics-device=all \
   python /scratch/user/experiment1/workflow_script.py

   srun --ntasks-per-node 1 dcgmi profile --resume
```

## Release Notes

Release notes are available in the [Changelog](https://github.com/AgnostiqHQ/covalent-slurm-plugin/blob/main/CHANGELOG.md).

## Citation

Please use the following citation in any publications:

> W. J. Cunningham, S. K. Radha, F. Hasan, J. Kanem, S. W. Neagle, and S. Sanand.
> _Covalent._ Zenodo, 2022. https://doi.org/10.5281/zenodo.5903364

## License

Covalent is licensed under the Apache License 2.0. See the [LICENSE](https://github.com/AgnostiqHQ/covalent-slurm-plugin/blob/main/LICENSE) file or contact the [support team](mailto:support@agnostiq.ai) for more details.
