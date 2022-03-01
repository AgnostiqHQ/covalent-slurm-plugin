# Copyright 2021 Agnostiq Inc.
#
# This file is part of Covalent.
#
# Licensed under the GNU Affero General Public License 3.0 (the "License").
# A copy of the License may be obtained with this software package or at
#
#      https://www.gnu.org/licenses/agpl-3.0.en.html
#
# Use of this file is prohibited except in compliance with the License. Any
# modifications or derivative works of this file must retain this copyright
# notice, and modified files must contain a notice indicating that they have
# been altered from the originals.
#
# Covalent is distributed in the hope that it will be useful, but WITHOUT
# ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
# FITNESS FOR A PARTICULAR PURPOSE. See the License for more details.
#
# Relief from the License may be granted by purchasing a commercial license.

"""Slurm executor plugin for the Covalent dispatcher."""

import os
import shutil
import subprocess
import tempfile
import time
from copy import deepcopy
from typing import Any, Dict, List, Tuple

import cloudpickle as pickle
from covalent._shared_files.util_classes import DispatchInfo
from covalent._workflow.transport import TransportableObject
from covalent.executor import BaseExecutor

_EXECUTOR_PLUGIN_DEFAULTS = {
    "username": "",
    "address": "",
    "ssh_key_file": "",
    "remote_workdir": "",
    "poll_freq": 30,
    "cache_dir": "/tmp/covalent",
    "options": {
        "parsable": "",
    },
}

executor_plugin_name = "SlurmExecutor"


class SlurmExecutor(BaseExecutor):
    """Slurm executor plugin class.

    Args:
        username: Username used to authenticate over SSH.
        address: Remote address or hostname of the Slurm login node.
        ssh_key_file: Private RSA key used to authenticate over SSH.
        remote_workdir: Working directory on the remote cluster.
        poll_freq: Frequency with which to poll a submitted job.
        cache_dir: Cache directory used by this executor for temporary files.
        options: Dictionary of parameters used to build a Slurm submit script.
    """

    def __init__(
        self,
        username: str,
        address: str,
        ssh_key_file: str,
        remote_workdir: str,
        poll_freq: int,
        options: Dict,
        **kwargs,
    ):
        super().__init__(**kwargs)

        self.username = username
        self.address = address
        self.ssh_key_file = ssh_key_file
        self.remote_workdir = remote_workdir
        self.poll_freq = poll_freq
        self.options = deepcopy(options)

    def execute(
        self,
        function: TransportableObject,
        args: List,
        kwargs: Dict,
        dispatch_id: str,
        results_dir: str,
        node_id: int = -1,
    ) -> Tuple[Any, str, str]:

        dispatch_info = DispatchInfo(dispatch_id)
        result_filename = f"result-{dispatch_id}-{node_id}.pkl"
        slurm_filename = f"slurm-{dispatch_id}-{node_id}.sh"
        task_results_dir = os.path.join(results_dir, dispatch_id)

        if "output" not in self.options:
            self.options["output"] = os.path.join(
                self.remote_workdir, f"stdout-{dispatch_id}-{node_id}.log"
            )
        if "error" not in self.options:
            self.options["error"] = os.path.join(
                self.remote_workdir, f"stderr-{dispatch_id}-{node_id}.log"
            )

        with self.get_dispatch_context(dispatch_info), tempfile.NamedTemporaryFile(
            dir=self.cache_dir, delete=False
        ) as f, tempfile.NamedTemporaryFile(dir=self.cache_dir, mode="w", delete=False) as g:

            # Write the serialized function to file
            pickle.dump(function, f)
            f.flush()

            # Create the remote directory
            subprocess.run(
                [
                    "ssh",
                    "-i",
                    self.ssh_key_file,
                    "-o",
                    "StrictHostKeyChecking=no",
                    "-o",
                    "UserKnownHostsFile=/dev/null",
                    "-o",
                    "LogLevel=ERROR",
                    f"{self.username}@{self.address}",
                    "mkdir",
                    "-p",
                    self.remote_workdir,
                ],
                check=True,
                capture_output=True,
            )

            # Copy the function to the remote filesystem
            func_filename = f"func-{dispatch_id}-{node_id}.pkl"
            remote_func_filename = os.path.join(self.remote_workdir, func_filename)
            subprocess.run(
                [
                    "rsync",
                    "-e",
                    f"ssh -i {self.ssh_key_file} -o StrictHostKeyChecking=no "
                    "-o UserKnownHostsFile=/dev/null -o LogLevel=ERROR",
                    f.name,
                    f"{self.username}@{self.address}:{remote_func_filename}",
                ],
                check=True,
                capture_output=True,
            )

            # Format the SLURM submit script
            slurm_submit_script = self._format_submit_script(
                func_filename,
                result_filename,
                function.python_version,
                args,
                kwargs,
            )
            g.write(slurm_submit_script)
            g.flush()

            # Copy the script to the local results directory
            local_slurm_filename = os.path.join(task_results_dir, "slurm.sh")
            shutil.copyfile(g.name, local_slurm_filename)

            # Copy the script to the remote filesystem
            remote_slurm_filename = os.path.join(self.remote_workdir, slurm_filename)
            subprocess.run(
                [
                    "rsync",
                    "-e",
                    f"ssh -i {self.ssh_key_file} -o StrictHostKeyChecking=no "
                    "-o UserKnownHostsFile=/dev/null -o LogLevel=ERROR",
                    g.name,
                    f"{self.username}@{self.address}:{remote_slurm_filename}",
                ],
                check=True,
                capture_output=True,
            )

            # Execute the script
            remote_slurm_filename = os.path.join(self.remote_workdir, slurm_filename)
            proc = subprocess.run(
                [
                    "ssh",
                    "-i",
                    self.ssh_key_file,
                    "-o",
                    "StrictHostKeyChecking=no",
                    "-o",
                    "UserKnownHostsFile=/dev/null",
                    "-o",
                    "LogLevel=ERROR",
                    f"{self.username}@{self.address}",
                    "/bin/bash",
                    "-l",
                    "-c",
                    f'"sbatch {remote_slurm_filename}"',
                ],
                capture_output=True,
            )
            if proc.returncode == 0:
                slurm_job_id = int(proc.stdout.decode("utf-8").strip().split(";")[0])
            else:
                raise Exception(proc.stderr)

            self._poll_slurm(slurm_job_id)

            return self._query_result(result_filename, task_results_dir)

    def _format_submit_script(
        self,
        func_filename: str,
        result_filename: str,
        python_version: str,
        args: List,
        kwargs: Dict,
    ) -> str:
        """Create a SLURM script which wraps a pickled Python function.

        Args:
            func_filename: Name of the pickled function.
            result_filename: Name of the pickled result.
            python_version: Python version required by the pickled function.
            args: Positional arguments consumed by the task.
            kwargs: Keyword arguments consumed by the task.

        Returns:
            script: String object containing a script parsable by sbatch.
        """

        slurm_preamble = "#!/bin/bash\n"
        for key, value in self.options.items():
            slurm_preamble += "#SBATCH "
            slurm_preamble += f"-{key}" if len(key) == 1 else f"--{key}"
            if value:
                slurm_preamble += f" {value}"
            slurm_preamble += "\n"
        slurm_preamble += "\n"

        slurm_conda = (
            """
source $HOME/.bashrc
conda activate {conda_env}
retval=$?
if [ $retval -ne 0 ] ; then
  >&2 echo "Conda environment {conda_env} is not present on the compute node. "\
  "Please create the environment and try again."
  exit 99
fi

""".format(
                conda_env=self.conda_env
            )
            if self.conda_env
            else ""
        )

        slurm_python_version = """
if [[ "{python_version}" != `python -V | awk '{{print $2}}'` ]] ; then
  >&2 echo "Python version mismatch. Please install Python {python_version} in the compute "\
  "environment."
  exit 199
fi
""".format(
            python_version=python_version
        )

        slurm_body = """
python - <<EOF
import cloudpickle as pickle

with open("{func_filename}", "rb") as f:
    function = pickle.load(f).get_deserialized()

result = function(*{args}, **{kwargs})

with open("{result_filename}", "wb") as f:
    pickle.dump(result, f)
EOF

wait
""".format(
            func_filename=os.path.join(self.remote_workdir, func_filename),
            args=args,
            kwargs=kwargs,
            result_filename=os.path.join(self.remote_workdir, result_filename),
        )

        slurm_submit_script = slurm_preamble + slurm_conda + slurm_python_version + slurm_body

        return slurm_submit_script

    def get_status(self, job_id: str) -> str:
        """Query the status of a job previously submitted to Slurm.

        Args:
            job_id: Slurm job ID.

        Returns:
            status: String describing the job status.
        """

        proc = subprocess.run(
            [
                "ssh",
                "-i",
                self.ssh_key_file,
                "-o",
                "StrictHostKeyChecking=no",
                "-o",
                "UserKnownHostsFile=/dev/null",
                "-o",
                "LogLevel=ERROR",
                f"{self.username}@{self.address}",
                "/bin/bash",
                "-l",
                "-c",
                f'"scontrol show job {job_id}"',
            ],
            check=True,
            capture_output=True,
        )
        return proc.stdout.decode("utf-8").strip()

    def _poll_slurm(self, job_id: int) -> None:
        """Poll a Slurm job until completion.

        Args:
            job_id: Slurm job ID.

        Returns:
            None
        """

        # Poll status every `poll_freq` seconds
        status = self.get_status(str(job_id))
        while (
            "PENDING" in status
            or "RUNNING" in status
            or "COMPLETING" in status
            or "CONFIGURING" in status
        ):
            time.sleep(self.poll_freq)
            status = self.get_status(str(job_id))

        if "COMPLETED" not in status:
            raise Exception("Job failed with status:\n", status)

    def _query_result(self, result_filename: str, task_results_dir: str) -> Any:
        """Query and retrieve the task result including stdout and stderr logs.

        Args:
            result_filename: Name of the pickled result file.
            task_results_dir: Directory on the Covalent server where the result will be copied.

        Returns:
            result: Task result.
        """
        # Check the result file exists on the remote backend
        remote_result_filename = os.path.join(self.remote_workdir, result_filename)
        proc = subprocess.run(
            [
                "ssh",
                "-i",
                self.ssh_key_file,
                "-o",
                "StrictHostKeyChecking=no",
                "-o",
                "UserKnownHostsFile=/dev/null",
                "-o",
                "LogLevel=ERROR",
                f"{self.username}@{self.address}",
                "test",
                "-e",
                remote_result_filename,
            ],
            capture_output=True,
        )
        if proc.returncode != 0:
            raise FileNotFoundError(proc.returncode, proc.stderr, remote_result_filename)

        # Copy result file from backend to Covalent server
        subprocess.run(
            [
                "rsync",
                "-e",
                f"ssh -i {self.ssh_key_file} -o StrictHostKeyChecking=no "
                "-o UserKnownHostsFile=/dev/null -o LogLevel=ERROR",
                f"{self.username}@{self.address}:{remote_result_filename}",
                task_results_dir,
            ],
            check=True,
            capture_output=True,
        )

        # Copy stdout, stderr from backend to Covalent server
        subprocess.run(
            [
                "rsync",
                "-e",
                f"ssh -i {self.ssh_key_file} -o StrictHostKeyChecking=no "
                "-o UserKnownHostsFile=/dev/null -o LogLevel=ERROR",
                f"{self.username}@{self.address}:{self.options['output']}",
                task_results_dir,
            ],
            check=True,
            capture_output=True,
        )
        subprocess.run(
            [
                "rsync",
                "-e",
                f"ssh -i {self.ssh_key_file} -o StrictHostKeyChecking=no "
                "-o UserKnownHostsFile=/dev/null -o LogLevel=ERROR",
                f"{self.username}@{self.address}:{self.options['error']}",
                task_results_dir,
            ],
            check=True,
            capture_output=True,
        )

        local_result_filename = os.path.join(task_results_dir, result_filename)
        with open(local_result_filename, "rb") as f:
            result = pickle.load(f)
        os.remove(local_result_filename)

        stdout_file = os.path.join(task_results_dir, os.path.basename(self.options["output"]))
        with open(stdout_file, "r") as f:
            stdout = f.read()
        os.remove(stdout_file)

        stderr_file = os.path.join(task_results_dir, os.path.basename(self.options["error"]))
        with open(stderr_file, "r") as f:
            stderr = f.read()
        os.remove(stderr_file)

        return result, stdout, stderr

    def cancel(self, job_id: int) -> None:
        """Cancel a Slurm job.

        Args:
            job_id: Slurm job ID.

        Returns:
            None
        """
