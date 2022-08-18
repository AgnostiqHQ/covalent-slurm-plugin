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

import asyncio
import os
import re
import sys
import tempfile
from copy import deepcopy
from typing import Any, Callable, Dict, List, Union

import aiofiles
import cloudpickle as pickle
from aiofiles import os as async_os
from covalent._results_manager.result import Result
from covalent._shared_files import logger
from covalent._shared_files.util_classes import DispatchInfo
from covalent.executor.base import BaseAsyncExecutor

app_log = logger.app_log
log_stack_info = logger.log_stack_info

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


class SlurmExecutor(BaseAsyncExecutor):
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
        cache_dir: str,
        options: Dict,
        **kwargs,
    ):
        super().__init__(**kwargs)

        self.username = username
        self.address = address
        self.ssh_key_file = ssh_key_file
        self.remote_workdir = remote_workdir
        self.poll_freq = poll_freq
        self.cache_dir = cache_dir
        self.options = deepcopy(options)

    async def run_async_subprocess(self, cmd: List[str]):

        command = " ".join(cmd)
        proc = await asyncio.create_subprocess_shell(
            command, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
        )

        stdout, stderr = await asyncio.wait_for(proc.communicate(), timeout=10)

        print(f"[{command!r} exited with {proc.returncode}]")
        if stdout:
            print(f"\n{stdout.decode()}")
        if stderr:
            print(f"\n{stderr.decode()}", file=sys.stderr)

        return proc.returncode, stdout, stderr

    def _format_submit_script(
        self,
        func_filename: str,
        result_filename: str,
        python_version: str,
    ) -> str:
        """Create a SLURM script which wraps a pickled Python function.

        Args:
            func_filename: Name of the pickled function.
            result_filename: Name of the pickled result.
            python_version: Python version required by the pickled function.

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
            if hasattr(self, "conda_env") and self.conda_env
            else ""
        )

        slurm_python_version = """
remote_py_version=$(python -c "print(__import__('sys').version_info[0])").$(python -c "print(__import__('sys').version_info[1])")
if [[ "{python_version}" != $remote_py_version ]] ; then
  >&2 echo "Python version mismatch. Please install Python {python_version} in the compute environment."
  exit 199
fi
""".format(
            python_version=python_version
        )

        slurm_body = """
python - <<EOF
import cloudpickle as pickle

with open("{func_filename}", "rb") as f:
    function, args, kwargs = pickle.load(f)

result = None
exception = None

try:
    result = function(*args, **kwargs)
except Exception as e:
    exception = e

with open("{result_filename}", "wb") as f:
    pickle.dump((result, exception), f)
EOF

wait
""".format(
            func_filename=os.path.join(self.remote_workdir, func_filename),
            result_filename=os.path.join(self.remote_workdir, result_filename),
        )

        return slurm_preamble + slurm_conda + slurm_python_version + slurm_body

    async def get_status(self, info_dict: dict) -> Union[Result, str]:
        """Query the status of a job previously submitted to Slurm.

        Args:
            info_dict: a dictionary containing all neccessary parameters needed to query the
                status of the execution. Required keys in the dictionary are:
                    A string mapping "job_id" to Slurm job ID.

        Returns:
            status: String describing the job status.
        """

        job_id = info_dict.get("job_id")
        if job_id is None:
            return Result.NEW_OBJ

        _, proc_stdout, _ = await self.run_async_subprocess(
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
        )
        return proc_stdout.decode("utf-8").strip()

    async def _poll_slurm(self, job_id: int) -> None:
        """Poll a Slurm job until completion.

        Args:
            job_id: Slurm job ID.

        Returns:
            None
        """

        # Poll status every `poll_freq` seconds
        status = await self.get_status({"job_id": str(job_id)})

        while (
            "PENDING" in status
            or "RUNNING" in status
            or "COMPLETING" in status
            or "CONFIGURING" in status
        ):
            await asyncio.sleep(self.poll_freq)
            status = await self.get_status({"job_id": str(job_id)})

        if "COMPLETED" not in status:
            raise RuntimeError("Job failed with status:\n", status)

    async def _query_result(self, result_filename: str, task_results_dir: str) -> Any:
        """Query and retrieve the task result including stdout and stderr logs.

        Args:
            result_filename: Name of the pickled result file.
            task_results_dir: Directory on the Covalent server where the result will be copied.

        Returns:
            result: Task result.
        """
        # Check the result file exists on the remote backend
        remote_result_filename = os.path.join(self.remote_workdir, result_filename)
        return_code, _, proc_stderr = await self.run_async_subprocess(
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
        )
        if return_code != 0:
            raise FileNotFoundError(return_code, proc_stderr, remote_result_filename)

        # Copy result file from backend to Covalent server
        await self.run_async_subprocess(
            [
                "rsync",
                "-e",
                f"ssh -i {self.ssh_key_file} -o StrictHostKeyChecking=no "
                "-o UserKnownHostsFile=/dev/null -o LogLevel=ERROR",
                f"{self.username}@{self.address}:{remote_result_filename}",
                f"{task_results_dir}/",
            ],
        )

        # Copy stdout, stderr from backend to Covalent server
        await self.run_async_subprocess(
            [
                "rsync",
                "-e",
                f"ssh -i {self.ssh_key_file} -o StrictHostKeyChecking=no "
                "-o UserKnownHostsFile=/dev/null -o LogLevel=ERROR",
                f"{self.username}@{self.address}:{self.options['output']}",
                f"{task_results_dir}/",
            ],
        )

        await self.run_async_subprocess(
            [
                "rsync",
                "-e",
                f"ssh -i {self.ssh_key_file} -o StrictHostKeyChecking=no "
                "-o UserKnownHostsFile=/dev/null -o LogLevel=ERROR",
                f"{self.username}@{self.address}:{self.options['error']}",
                f"{task_results_dir}/",
            ],
        )

        local_result_filename = os.path.join(task_results_dir, result_filename)
        async with aiofiles.open(local_result_filename, "rb") as f:
            contents = await f.read()
            result, exception = pickle.loads(contents)
        await async_os.remove(local_result_filename)

        stdout_file = os.path.join(task_results_dir, os.path.basename(self.options["output"]))
        async with aiofiles.open(stdout_file, "r") as f:
            stdout = await f.read()
        await async_os.remove(stdout_file)

        stderr_file = os.path.join(task_results_dir, os.path.basename(self.options["error"]))
        async with aiofiles.open(stderr_file, "r") as f:
            stderr = await f.read()
        await async_os.remove(stderr_file)

        return result, stdout, stderr, exception

    async def run(self, function, args, kwargs, task_metadata):

        dispatch_id = task_metadata["dispatch_id"]
        node_id = task_metadata["node_id"]
        results_dir = task_metadata["results_dir"]

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

        result = None
        exception = None

        async with aiofiles.tempfile.NamedTemporaryFile(
            dir=self.cache_dir
        ) as temp_f, aiofiles.tempfile.NamedTemporaryFile(dir=self.cache_dir, mode="w") as temp_g:

            # Write the deserialized function to file
            await temp_f.write(pickle.dumps((function, args, kwargs)))
            await temp_f.flush()

            # Create the remote directory
            await self.run_async_subprocess(
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
            )

            # Copy the function to the remote filesystem
            func_filename = f"func-{dispatch_id}-{node_id}.pkl"
            remote_func_filename = os.path.join(self.remote_workdir, func_filename)

            await self.run_async_subprocess(
                [
                    "rsync",
                    "-e",
                    f"ssh -i {self.ssh_key_file} -o StrictHostKeyChecking=no "
                    "-o UserKnownHostsFile=/dev/null -o LogLevel=ERROR",
                    temp_f.name,
                    f"{self.username}@{self.address}:{remote_func_filename}",
                ],
            )

            func_py_version = ".".join(function.args[0].python_version.split(".")[:2])

            # Format the SLURM submit script
            slurm_submit_script = self._format_submit_script(
                func_filename,
                result_filename,
                func_py_version,
            )
            await temp_g.write(slurm_submit_script)
            await temp_g.flush()

            # Copy the script to the remote filesystem
            remote_slurm_filename = os.path.join(self.remote_workdir, slurm_filename)
            await self.run_async_subprocess(
                [
                    "rsync",
                    "-e",
                    f"ssh -i {self.ssh_key_file} -o StrictHostKeyChecking=no "
                    "-o UserKnownHostsFile=/dev/null -o LogLevel=ERROR",
                    temp_g.name,
                    f"{self.username}@{self.address}:{remote_slurm_filename}",
                ],
            )

            # Execute the script
            remote_slurm_filename = os.path.join(self.remote_workdir, slurm_filename)
            return_code, proc_stdout, proc_stderr = await self.run_async_subprocess(
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
            )
            if return_code == 0:
                slurm_job_id = int(re.findall("[0-9]+", proc_stdout.decode("utf-8"))[0])
            else:
                raise RuntimeError(proc_stderr)

            await self._poll_slurm(slurm_job_id)

            result, stdout, stderr, exception = await self._query_result(
                result_filename, task_results_dir
            )

            if exception:
                raise exception

            # FIX: covalent-mono's _run_task should parse exceptions from executor.execute()
            # return result, stdout, stderr, exception
            return result, stdout, stderr
