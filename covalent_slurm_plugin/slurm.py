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
from copy import deepcopy
from pathlib import Path
from typing import Any, Callable, Dict, List, Tuple, Union

import aiofiles
import asyncssh
import cloudpickle as pickle
from aiofiles import os as async_os
from covalent._results_manager.result import Result
from covalent._shared_files import logger
from covalent._shared_files.config import get_config
from covalent.executor.base import AsyncBaseExecutor

app_log = logger.app_log
log_stack_info = logger.log_stack_info

_EXECUTOR_PLUGIN_DEFAULTS = {
    "username": "",
    "address": "",
    "ssh_key_file": "",
    "remote_workdir": "covalent-workdir",
    "slurm_path": None,
    "conda_env": None,
    "cache_dir": str(Path(get_config("dispatcher.cache_dir")).expanduser().resolve()),
    "options": {
        "parsable": "",
    },
    "poll_freq": 30,
    "cleanup": True,
}

executor_plugin_name = "SlurmExecutor"


class SlurmExecutor(AsyncBaseExecutor):
    """Slurm executor plugin class.

    Args:
        username: Username used to authenticate over SSH.
        address: Remote address or hostname of the Slurm login node.
        ssh_key_file: Private RSA key used to authenticate over SSH.
        remote_workdir: Working directory on the remote cluster.
        slurm_path: Path to the slurm commands if they are not found automatically.
        conda_env: Name of conda environment on which to run the function.
        cache_dir: Cache directory used by this executor for temporary files.
        options: Dictionary of parameters used to build a Slurm submit script.
        poll_freq: Frequency with which to poll a submitted job.
        cleanup: Whether to perform cleanup or not on remote machine.
    """

    def __init__(
        self,
        username: str,
        address: str,
        ssh_key_file: str,
        remote_workdir: str = "covalent-workdir",
        slurm_path: str = None,
        conda_env: str = None,
        cache_dir: str = None,
        options: Dict = None,
        poll_freq: int = 30,
        cleanup: bool = True,
        **kwargs,
    ):
        super().__init__(**kwargs)

        self.username = username
        self.address = address

        ssh_key_file = ssh_key_file or get_config("executors.slurm.ssh_key_file")
        self.ssh_key_file = str(Path(ssh_key_file).expanduser().resolve())

        self.remote_workdir = remote_workdir
        self.slurm_path = slurm_path
        self.conda_env = conda_env

        cache_dir = cache_dir or get_config("dispatcher.cache_dir")
        self.cache_dir = str(Path(cache_dir).expanduser().resolve())

        # To allow passing empty dictionary
        if options is None:
            options = get_config("executors.slurm.options")
        self.options = deepcopy(options)

        self.poll_freq = poll_freq
        self.cleanup = cleanup

        self.LOAD_SLURM_PREFIX = "source /etc/profile\n module whatis slurm &> /dev/null\n if [ $? -eq 0 ] ; then\n module load slurm\n fi\n"

    async def _client_connect(self) -> Tuple[bool, asyncssh.SSHClientConnection]:
        """
        Helper function for connecting to the remote host through asyncssh module.

        Args:
            None

        Returns:
            True if connection to the remote host was successful, False otherwise.
        """

        ssh_success = False
        conn = None
        if os.path.exists(self.ssh_key_file):
            conn = await asyncssh.connect(
                self.address,
                username=self.username,
                client_keys=[self.ssh_key_file],
                known_hosts=None,
            )

            ssh_success = True

        else:
            message = f"No SSH key file found at {self.ssh_key_file}. Cannot connect to host."
            raise RuntimeError(message)

        return ssh_success, conn

    async def perform_cleanup(
        self,
        conn: asyncssh.SSHClientConnection,
        remote_func_filename: str,
        remote_slurm_filename: str,
        remote_result_filename: str,
        remote_stdout_filename: str,
        remote_stderr_filename: str,
    ) -> None:
        """
        Function to perform cleanup on remote machine

        Args:
            remote_func_filename: Function file on remote machine
            remote_slurm_filename: Slurm script file on remote machine
            remote_result_filename: Result file on remote machine
            remote_stdout_filename: Standard out file on remote machine
            remote_stderr_filename: Standard error file on remote machine

        Returns:
            None
        """

        await conn.run(f"rm {remote_func_filename}")
        await conn.run(f"rm {remote_slurm_filename}")
        await conn.run(f"rm {remote_result_filename}")
        await conn.run(f"rm {remote_stdout_filename}")
        await conn.run(f"rm {remote_stderr_filename}")

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

    async def get_status(
        self, info_dict: dict, conn: asyncssh.SSHClientConnection
    ) -> Union[Result, str]:
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

        cmd_scontrol = f"scontrol show job {job_id}"

        if self.slurm_path:
            app_log.debug("Exporting slurm path for scontrol...")
            cmd_scontrol = f"export PATH=$PATH:{self.slurm_path} && {cmd_scontrol}"

        else:
            app_log.debug("Verifying slurm installation for scontrol...")
            proc_verify_scontrol = await conn.run(self.LOAD_SLURM_PREFIX + "which scontrol")

            if proc_verify_scontrol.returncode != 0:
                raise RuntimeError("Please provide `slurm_path` to run scontrol command")

            cmd_scontrol = self.LOAD_SLURM_PREFIX + cmd_scontrol

        proc = await conn.run(cmd_scontrol)
        return proc.stdout.strip()

    async def _poll_slurm(self, job_id: int, conn: asyncssh.SSHClientConnection) -> None:
        """Poll a Slurm job until completion.

        Args:
            job_id: Slurm job ID.

        Returns:
            None
        """

        # Poll status every `poll_freq` seconds
        status = await self.get_status({"job_id": str(job_id)}, conn)

        while (
            "PENDING" in status
            or "RUNNING" in status
            or "COMPLETING" in status
            or "CONFIGURING" in status
        ):
            await asyncio.sleep(self.poll_freq)
            status = await self.get_status({"job_id": str(job_id)}, conn)

        if "COMPLETED" not in status:
            raise RuntimeError("Job failed with status:\n", status)

    async def _query_result(
        self, result_filename: str, task_results_dir: str, conn: asyncssh.SSHClientConnection
    ) -> Any:
        """Query and retrieve the task result including stdout and stderr logs.

        Args:
            result_filename: Name of the pickled result file.
            task_results_dir: Directory on the Covalent server where the result will be copied.

        Returns:
            result: Task result.
        """

        # Check the result file exists on the remote backend
        remote_result_filename = os.path.join(self.remote_workdir, result_filename)

        proc = await conn.run(f"test -e {remote_result_filename}")
        if proc.returncode != 0:
            raise FileNotFoundError(proc.returncode, proc.stderr.strip(), remote_result_filename)

        # Copy result file from remote machine to Covalent server
        local_result_filename = os.path.join(task_results_dir, result_filename)
        await asyncssh.scp((conn, remote_result_filename), local_result_filename)

        # Copy stdout, stderr from remote machine to Covalent server
        stdout_file = os.path.join(task_results_dir, os.path.basename(self.options["output"]))
        stderr_file = os.path.join(task_results_dir, os.path.basename(self.options["error"]))

        await asyncssh.scp((conn, self.options["output"]), stdout_file)
        await asyncssh.scp((conn, self.options["error"]), stderr_file)

        async with aiofiles.open(local_result_filename, "rb") as f:
            contents = await f.read()
            result, exception = pickle.loads(contents)
        await async_os.remove(local_result_filename)

        async with aiofiles.open(stdout_file, "r") as f:
            stdout = await f.read()
        await async_os.remove(stdout_file)

        async with aiofiles.open(stderr_file, "r") as f:
            stderr = await f.read()
        await async_os.remove(stderr_file)

        return result, stdout, stderr, exception

    async def run(self, function: Callable, args: List, kwargs: Dict, task_metadata: Dict):

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

        ssh_success, conn = await self._client_connect()

        if not ssh_success:
            raise RuntimeError(
                f"Could not connect to host: '{self.address}' as user: '{self.username}'"
            )

        async with aiofiles.tempfile.NamedTemporaryFile(
            dir=self.cache_dir
        ) as temp_f, aiofiles.tempfile.NamedTemporaryFile(dir=self.cache_dir, mode="w") as temp_g:

            # Write the function to file
            app_log.debug("Writing function, args, kwargs to file...")
            await temp_f.write(pickle.dumps((function, args, kwargs)))
            await temp_f.flush()

            # Create the remote directory
            app_log.debug(f"Creating remote work directory {self.remote_workdir} ...")
            cmd_mkdir_remote = f"mkdir -p {self.remote_workdir}"

            proc_mkdir_cache = await conn.run(cmd_mkdir_remote)
            if client_err := proc_mkdir_cache.stderr.strip():
                raise RuntimeError(client_err)

            # Copy the function to the remote filesystem
            func_filename = f"func-{dispatch_id}-{node_id}.pkl"
            remote_func_filename = os.path.join(self.remote_workdir, func_filename)

            app_log.debug(f"Copying function to remote fs: {remote_func_filename} ...")
            await asyncssh.scp(temp_f.name, (conn, remote_func_filename))

            func_py_version = ".".join(function.args[0].python_version.split(".")[:2])
            app_log.debug(f"Python version: {func_py_version}")

            # Format the SLURM submit script
            slurm_submit_script = self._format_submit_script(
                func_filename,
                result_filename,
                func_py_version,
            )

            app_log.debug("Writing slurm submit script to file...")
            await temp_g.write(slurm_submit_script)
            await temp_g.flush()

            # Copy the script to the remote filesystem
            remote_slurm_filename = os.path.join(self.remote_workdir, slurm_filename)

            app_log.debug(f"Copying slurm submit script to remote: {remote_slurm_filename} ...")
            await asyncssh.scp(temp_g.name, (conn, remote_slurm_filename))

            # Execute the script
            remote_slurm_filename = os.path.join(self.remote_workdir, slurm_filename)

            app_log.debug(f"Running the script: {remote_slurm_filename} ...")
            cmd_sbatch = f"sbatch {remote_slurm_filename}"

            if self.slurm_path:
                app_log.debug("Exporting slurm path for sbatch...")
                cmd_sbatch = f"export PATH=$PATH:{self.slurm_path} && {cmd_sbatch}"

            else:
                app_log.debug("Verifying slurm installation for sbatch...")
                proc_verify_sbatch = await conn.run(self.LOAD_SLURM_PREFIX + "which sbatch")

                if proc_verify_sbatch.returncode != 0:
                    raise RuntimeError("Please provide `slurm_path` to run sbatch command")

                cmd_sbatch = self.LOAD_SLURM_PREFIX + cmd_sbatch

            proc = await conn.run(cmd_sbatch)

            if proc.returncode != 0:
                raise RuntimeError(proc.stderr.strip())

            app_log.debug(f"Job submitted with stdout: {proc.stdout.strip()}")
            slurm_job_id = int(re.findall("[0-9]+", proc.stdout.strip())[0])

            app_log.debug(f"Polling slurm with job_id: {slurm_job_id} ...")
            await self._poll_slurm(slurm_job_id, conn)

            app_log.debug(f"Querying result with job_id: {slurm_job_id} ...")
            result, stdout, stderr, exception = await self._query_result(
                result_filename, task_results_dir, conn
            )

            print(stdout)
            print(stderr, file=sys.stderr)

            if exception:
                raise RuntimeError(exception)

            app_log.debug("Preparing for teardown...")
            self._remote_func_filename = remote_func_filename
            self._remote_slurm_filename = remote_slurm_filename
            self._result_filename = result_filename

            app_log.debug("Closing SSH connection...")
            conn.close()
            await conn.wait_closed()
            app_log.debug("SSH connection closed, returning result")

            return result

    async def teardown(self, task_metadata: Dict):

        if self.cleanup:
            app_log.debug("Performing cleanup on remote...")
            _, conn = await self._client_connect()
            await self.perform_cleanup(
                conn=conn,
                remote_func_filename=self._remote_func_filename,
                remote_slurm_filename=self._remote_slurm_filename,
                remote_result_filename=os.path.join(self.remote_workdir, self._result_filename),
                remote_stdout_filename=self.options["output"],
                remote_stderr_filename=self.options["error"],
            )

        app_log.debug("Closing SSH connection...")
        conn.close()
        await conn.wait_closed()
        app_log.debug("SSH connection closed, teardown complete")
