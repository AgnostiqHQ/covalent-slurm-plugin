&nbsp;

<div align="center">

<img src="https://raw.githubusercontent.com/AgnostiqHQ/covalent/master/doc/source/_static/covalent_readme_banner.svg" width=150%>

&nbsp;

</div>

## Covalent Slurm Plugin

Covalent is a Pythonic workflow tool used to execute tasks on advanced computing hardware. This executor plugin interfaces Covalent with HPC systems managed by [Slurm](https://slurm.schedmd.com/documentation.html). In order for workflows to be deployable, users must have SSH access to the Slurm login node, writable storage space on the remote filesystem, and permissions to submit jobs to Slurm.

To use this plugin with Covalent, simply install it using `pip`:

```
pip install covalent-slurm-plugin
```

The following shows an example of how a user might modify their Covalent [configuration](https://covalent.readthedocs.io/en/latest/how_to/config/customization.html) to support Slurm:

```console
[executors.slurm]
username = "user"
address = "login.cluster.org"
ssh_key_file = "/home/user/.ssh/id_rsa"
remote_workdir = "/scratch/user"
cache_dir = "/tmp/covalent"
conda_env = ""

[executors.slurm.options]
partition = "general"
cpus-per-task = 4
gres = "gpu:v100:4"
exclusive = ""
parsable = ""
```

The first stanza describes default connection parameters for a user who is able to successfully connect to the Slurm login node using `ssh -i /home/user/.ssh/id_rsa user@login.cluster.org`. The second stanza describes default parameters which are used to construct a Slurm submit script. In this example, the submit script would contain the following preamble:

```console
#!/bin/bash
#SBATCH --partition=general
#SBATCH --cpus-per-task=4
#SBATCH --gres=gpu:v100:4
#SBATCH --exclusive
#SBATCH --parsable
```

Within a workflow, users can then decorate electrons using these default settings:

```python
import covalent as ct

@ct.electron(executor="slurm")
def my_task(x, y):
    return x + y
```

or use a class object to customize behavior scoped to specific tasks:

```python
executor = ct.executor.SlurmExecutor(
    remote_workdir="/scratch/user/experiment1",
    conda_env="covalent",
    options={
        "partition": "compute",
	"cpus-per-task": 8
    }
)

@ct.electron(executor=executor)
def my_custom_task(x, y):
    return x + y
```

For more information about how to get started with Covalent, check out the project [homepage](https://github.com/AgnostiqHQ/covalent) and the official [documentation](https://covalent.readthedocs.io/en/latest/).

## Release Notes

Release notes are available in the [Changelog](https://github.com/AgnostiqHQ/covalent-slurm-plugin/blob/main/CHANGELOG.md).

## Citation

Please use the following citation in any publications:

> W. J. Cunningham, S. K. Radha, F. Hasan, J. Kanem, S. W. Neagle, and S. Sanand.
> *Covalent.* Zenodo, 2022. https://doi.org/10.5281/zenodo.5903364

## License

Covalent is licensed under the GNU Affero GPL 3.0 License. Covalent may be distributed under other licenses upon request. See the [LICENSE](https://github.com/AgnostiqHQ/covalent-slurm-plugin/blob/main/LICENSE) file or contact the [support team](mailto:support@agnostiq.ai) for more details.
