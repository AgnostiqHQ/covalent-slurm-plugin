# Copyright 2024 Agnostiq Inc.
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

"""Tools for formatting the Slurm job submission script."""

import re
from typing import Dict, List, Optional

SLURM_JOB_SCRIPT_TEMPLATE = """\
#!/bin/bash
{sbatch_directives}

{shell_env_setup}

{conda_env_setup}

if [ $? -ne 0 ] ; then
  >&2 echo "Failed to activate conda env '$__env_name' on compute node."
  >&2 echo "In case you have the conda env installed, please make sure your .bashrc file doesn't ignore non-interactive shells."
  exit 99
fi

remote_py_version=$(python -c "print('.'.join(map(str, __import__('sys').version_info[:2])))")
if [[ $remote_py_version != "{python_version}" && {ignore_versions} != 1 ]] ; then
  >&2 echo "Python version mismatch."
  >&2 echo "Environment '$__env_name' (python=$remote_py_version) does not match task (python={python_version})."
  >&2 echo "The task might still be runnable but if failed the error might not be as informative."
  >&2 echo "You can do that by passing 'ignore_versions=True' in the SlurmExecutor constructor."
  exit 199
fi

covalent_version=$(python -c "import covalent; print(covalent.__version__)")
if [ $? -ne 0 ] ; then
  >&2 echo "Covalent may not be installed in the compute environment."
  >&2 echo "Please install covalent=={covalent_version} in the '$__env_name' conda env."
  exit 299
elif [[ $covalent_version != "{covalent_version}" && {ignore_versions} != 1 ]] ; then
  >&2 echo "Covalent version mismatch."
  >&2 echo "Environment '$__env_name' (covalent==$covalent_version) does not match task (covalent=={covalent_version})."
  >&2 echo "The task might still be runnable but if failed the error might not be as informative."
  >&2 echo "You can do that by passing 'ignore_versions=True' in the SlurmExecutor constructor."
  exit 299
fi

cloudpickle_version=$(python -c "import cloudpickle; print(cloudpickle.__version__)")
if [ $? -ne 0 ] ; then
  >&2 echo "Cloudpickle may not be installed in the compute environment."
  >&2 echo "Please install cloudpickle=={cloudpickle_version} in the '$__env_name' conda env."
  exit 399
elif [[ $cloudpickle_version != "{cloudpickle_version}" && {ignore_versions} != 1 ]] ; then
  >&2 echo "Cloudpickle version mismatch."
  >&2 echo "Environment '$__env_name' (cloudpickle==$cloudpickle_version) does not match task (cloudpickle=={cloudpickle_version})."
  >&2 echo "The task might still be runnable but if failed the error might not be as informative."
  >&2 echo "You can do that by passing 'ignore_versions=True' in the SlurmExecutor constructor."
  exit 399
fi

{run_commands}

wait
"""


class JobScript:
    """Formats an sbatch submit script for the Slurm cluster."""

    def __init__(
        self,
        sbatch_options: Optional[Dict[str, str]] = None,
        srun_options: Optional[Dict[str, str]] = None,
        variables: Optional[Dict[str, str]] = None,
        bashrc_path: Optional[str] = "",
        conda_env: Optional[str] = "",
        prerun_commands: Optional[List[str]] = None,
        srun_append: Optional[str] = "",
        postrun_commands: Optional[List[str]] = None,
        use_srun: bool = True,
        ignore_versions: bool = False,
    ):
        """Create a job script formatter.

        Args:
            See `covalent_slurm_plugin.slurm.SlurmExecutor` for details.
        """

        self._sbatch_options = sbatch_options or {}
        self._srun_options = srun_options or {}
        self._variables = variables or {}
        self._bashrc_path = bashrc_path
        self._conda_env = conda_env
        self._prerun_commands = prerun_commands or []
        self._srun_append = srun_append
        self._postrun_commands = postrun_commands or []
        self._use_srun = use_srun

        # Convert it to an int for easier comparison in bash
        self._ignore_versions = int(ignore_versions)

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
        setup_lines.extend(f'export {key}="{value}"' for key, value in self._variables.items())
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

        setup_lines.insert(0, f'__env_name="{conda_env_name}"')

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

    def get_run_commands(
        self,
        remote_py_filename: str,
        func_filename: str,
        result_filename: str,
    ) -> str:
        """Get the run commands."""

        # Commands executed before the user's @electron function.
        prerun_cmds = "\n".join(self._prerun_commands)

        # Command that executes the user's @electron function.
        python_cmd = "python {remote_py_filename} {func_filename} {result_filename}"

        if not self._use_srun:
            # Invoke python directly.
            run_cmd = python_cmd
        else:
            # Invoke python via srun.
            srun_options = []
            for key, value in self._srun_options.items():
                if len(key) == 1:
                    srun_options.append(f"-{key}" + (f" {value}" if value else ""))
                else:
                    srun_options.append(f"--{key}" + (f"={value}" if value else ""))

            run_cmds = [
                f"srun {' '.join(srun_options)} \\" if srun_options else "srun \\",
                f"  {self._srun_append} \\",
                f"  {python_cmd}",
            ]
            if not self._srun_append:
                # Remove (empty) commands appended to `srun` call.
                run_cmds.pop(1)

            run_cmd = "\n".join(run_cmds)

        run_cmd = run_cmd.format(
            remote_py_filename=remote_py_filename,
            func_filename=func_filename,
            result_filename=result_filename,
        )

        # Commands executed after the user's @electron function.
        postrun_cmds = "\n".join(self._postrun_commands)

        # Combine all commands.
        run_commands = [prerun_cmds, run_cmd, postrun_cmds]
        run_commands = [cmd for cmd in run_commands if cmd]

        return "\n\n".join(run_commands)

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
            "ignore_versions": self._ignore_versions,
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

        if missing_keys := required_keys - existing_keys:
            raise ValueError(f"Missing required keys: {', '.join(missing_keys)}")

        if extra_keys := existing_keys - required_keys:
            raise ValueError(f"Unexpected keys: {', '.join(extra_keys)}")

        return SLURM_JOB_SCRIPT_TEMPLATE.format(**template_kwargs)
