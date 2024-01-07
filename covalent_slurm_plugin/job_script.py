# Copyright 2021 Agnostiq Inc.
#
# This file is part of Covalent.
#
# Licensed under the Apache License 2.0 (the "License"). A copy of the
# License may be obtained with this software package or at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Use of this file is prohibited except in compliance with the License.
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


import re
from typing import Dict, List, Optional


SLURM_JOB_SCRIPT_TEMPLATE = """\
#!/bin/bash
{sbatch_directives}

{shell_env_setup}

{conda_env_setup}

retval=$?
if [ $retval -ne 0 ] ; then
  >&2 echo "Failed to activate conda env '$_conda_env_name' on compute node."
  exit 99
fi

remote_py_version=$(python -c "print('.'.join(map(str, __import__('sys').version_info[:2])))")
if [[ "{python_version}" != $remote_py_version ]] ; then
  >&2 echo "Python version mismatch. Please install Python {python_version} in the compute environment."
  exit 199
fi

covalent_version=$(python -c "import covalent; print(covalent.__version__, end='')")
if [[ $covalent_version != "{covalent_version}" ]] ; then
  >&2 echo "Covalent version mismatch."
  >&2 echo "Compute environment has 'covalent==$covalent_version', user has 'covalent=={covalent_version}'"
  exit 299
fi

cloudpickle_version=$(python -c "import cloudpickle; print(cloudpickle.__version__)")
if [[ $cloudpickle_version != "{cloudpickle_version}" ]] ; then
  >&2 echo "Cloudpickle version mismatch."
  >&2 echo "Compute environment has 'cloudpickle==$cloudpickle_version', but user has 'cloudpickle=={cloudpickle_version}'"
  exit 399
fi

{run_commands}

wait
"""


class JobScript:
    """Create a job script for the Slurm cluster."""

    def __init__(
        self,
        sbatch_options: Optional[Dict[str, str]] = None,
        srun_options: Optional[Dict[str, str]] = None,
        variables: Optional[Dict[str, str]] = None,
        bashrc_path: Optional[str] = None,
        conda_env: Optional[str] = None,
        prerun_commands: Optional[List[str]] = None,
        srun_append: Optional[str] = None,
        postrun_commands: Optional[List[str]] = None,
        use_srun: bool = True,
    ):
        # TODO: docstring

        self._sbatch_options = sbatch_options or {}
        self._srun_options = srun_options or {}
        self._variables = variables or {}
        self._bashrc_path = bashrc_path
        self._conda_env = conda_env
        self._prerun_commands = prerun_commands or []
        self._srun_append = srun_append
        self._postrun_commands = postrun_commands or []
        self._use_srun = use_srun

    @property
    def sbatch_directives(self) -> str:
        """Get the sbatch directives."""
        directives = []
        for key, value in self._sbatch_options.items():
            if len(key) == 1:
                directives.append(f"#SBATCH -{key}" + (f" {value}" if value else ""))
            else:
                directives.append(f"#SBATCH --{key}" + (f"={value}" if value else ""))

        return "\n".join(directives)

    @property
    def shell_env_setup(self) -> str:
        """Get the shell environment setup."""
        setup_lines = [
            f"source {self._bashrc_path}" if self._bashrc_path else "",
        ]
        for key, value in self._variables.items():
            setup_lines.append(f'export {key}="{value}"')

        return "\n".join(setup_lines)

    @property
    def conda_env_setup(self) -> str:
        """Get the conda environment setup."""
        setup_lines = []
        if not self._conda_env or self._conda_env == "base":
            conda_env_name = "base"
            setup_lines.append("conda activate")
        else:
            conda_env_name = self._conda_env
            setup_lines.append(f"conda activate {self._conda_env}")

        setup_lines.append(f'_conda_env_name="{conda_env_name}"')

        return "\n".join(setup_lines)

    @property
    def covalent_version(self) -> str:
        """Get the version of Covalent installed in the compute environment."""
        import covalent

        return covalent.__version__

    @property
    def cloudpickle_version(self) -> str:
        """Get the version of cloudpickle installed in the compute environment."""
        import cloudpickle

        return cloudpickle.__version__

    @property
    def prerun_commands(self) -> str:
        """Get the prerun commands."""

        return "\n".join(self._prerun_commands)

    def get_run_commands(
        self,
        remote_py_filename: str,
        func_filename: str,
        result_filename: str,
    ) -> str:
        """Get the run commands."""

        # Commands executed before the user's @electron function.
        if not self._prerun_commands:
            prerun_cmds = ""
        else:
            prerun_cmds = "\n".join(self._prerun_commands)

        # Command that executes the user's @electron function.
        if self._use_srun:
            srun_options = []
            for key, value in self._srun_options.items():
                if len(key) == 1:
                    srun_options.append(f"-{key}" + (f" {value}" if value else ""))
                else:
                    srun_options.append(f"--{key}" + (f"={value}" if value else ""))

            run_cmds = [
                f"srun {' '.join(srun_options)} \\" if srun_options else "srun \\",
                "  python {remote_py_filename} {func_filename} {result_filename}",
            ]
            if self._srun_append:
                run_cmds.insert(1, f"  {self._srun_append} \\")

            run_cmd = "\n".join(run_cmds)
        else:
            run_cmd = "python {remote_py_filename} {func_filename} {result_filename}"

        run_cmd = run_cmd.format(
            remote_py_filename=remote_py_filename,
            func_filename=func_filename,
            result_filename=result_filename,
        )

        # Commands executed after the user's @electron function.
        if not self._postrun_commands:
            postrun_cmds = ""
        else:
            postrun_cmds = "\n".join(self._postrun_commands)

        run_commands = [prerun_cmds, run_cmd, postrun_cmds]
        run_commands = [cmd for cmd in run_commands if cmd]

        return "\n\n".join(run_commands)

    @property
    def postrun_commands(self) -> str:
        """Get the postrun commands."""

        return "\n".join(self._postrun_commands)

    def format(
        self,
        python_version: str,
        remote_py_filename: str,
        func_filename: str,
        result_filename: str,
    ) -> str:
        """Render the job script."""
        template_kwargs = {
            "sbatch_directives": self.sbatch_directives,
            "shell_env_setup": self.shell_env_setup,
            "conda_env_setup": self.conda_env_setup,
            "covalent_version": self.covalent_version,
            "cloudpickle_version": self.cloudpickle_version,
            "python_version": python_version,
            "run_commands": self.get_run_commands(
                remote_py_filename=remote_py_filename,
                func_filename=func_filename,
                result_filename=result_filename,
            ),
        }
        existing_keys = set(template_kwargs.keys())
        required_keys = set(re.findall(r"\{(\w+)\}", SLURM_JOB_SCRIPT_TEMPLATE))

        if (missing_keys := required_keys - existing_keys):
            raise ValueError(f"Missing required keys: {', '.join(missing_keys)}")

        if (extra_keys := existing_keys - required_keys):
            raise ValueError(f"Unexpected keys: {', '.join(extra_keys)}")

        return SLURM_JOB_SCRIPT_TEMPLATE.format(**template_kwargs)
