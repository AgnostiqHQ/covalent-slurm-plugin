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

"""Slurm executor plugin for the Covalent dispatcher."""

import asyncio
import re
import sys
from copy import deepcopy
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Tuple, Union

import aiofiles
import asyncssh
import cloudpickle as pickle
from aiofiles import os as async_os
from covalent._results_manager.result import Result
from covalent._shared_files import logger
from covalent._shared_files.config import get_config
from covalent.executor.base import AsyncBaseExecutor
from pydantic import BaseModel, Field

from covalent_slurm_plugin.job_script import JobScript

__all__ = ["SlurmExecutor"]

EXECUTOR_PLUGIN_NAME = "SlurmExecutor"


class ExecutorPluginDefaults(BaseModel):
    """Defaults for the SlurmExecutor plugin."""

    username: Optional[str] = ""
    address: Optional[str] = ""
    ssh_key_file: Optional[str] = ""
    cert_file: Optional[str] = None
    ssh_port: Optional[int] = 22
    remote_workdir: Optional[str] = "covalent-workdir"
    create_unique_workdir: bool = False
    variables: Optional[Dict[str, str]] = Field(default_factory=dict)
    conda_env: Optional[str] = ""
    options: Optional[Dict] = Field(default_factory=lambda: {"parsable": ""})
    prerun_commands: Optional[List[str]] = Field(default_factory=list)
    postrun_commands: Optional[List[str]] = Field(default_factory=list)
    use_srun: bool = True
    srun_options: Optional[Dict] = Field(default_factory=dict)
    srun_append: Optional[str] = ""
    bashrc_path: Optional[str] = "$HOME/.bashrc"
    slurm_path: Optional[str] = "/usr/bin"
    poll_freq: int = 5
    cleanup: bool = True
    cache_dir: Optional[str] = str(
        Path.home() / ".config/covalent/executor_plugins/covalent-slurm-cache"
    )
    ignore_versions: bool = False


_EXECUTOR_PLUGIN_DEFAULTS = ExecutorPluginDefaults().model_dump()

app_log = logger.app_log
log_stack_info = logger.log_stack_info

_LOAD_SLURM_PREFIX = """\
source /etc/profile
module whatis slurm &> /dev/null
if [ $? -eq 0 ] ; then
  module load slurm
fi
"""

# TODO: consider enumerating job statuses
# TODO: capture poll_freq errors and inform user


class SlurmExecutor(AsyncBaseExecutor):
    """Slurm executor plugin class.

    Args:
        username: Username used to authenticate over SSH.
        address: Remote address or hostname of the Slurm login node.
        ssh_key_file: Private RSA key used to authenticate over SSH (usually at ~/.ssh/id_rsa).
        cert_file: Certificate file used to authenticate over SSH, if required (usually has extension .pub).
        remote_workdir: Working directory on the remote cluster.
        create_unique_workdir: Whether to create a unique working (sub)directory for each task.
        options: Dictionary of parameters used to build a Slurm submit script.
        variables: A dictionary of environment variables to declare before job execution.
        conda_env: Name of conda environment on which to run the function. Use "base" for the base environment or "" for no conda.
        prerun_commands: List of shell commands to run before running the pickled function.
        postrun_commands: List of shell commands to run after running the pickled function.
        use_srun: Whether or not to run the pickled Python function with srun. If your function itself makes srun or mpirun calls, set this to False.
        srun_options: Dictionary of parameters passed to srun inside submit script.
        srun_append: Command nested into srun call.
        bashrc_path: Path to the bashrc file to source before running the function.
        slurm_path: Path to the slurm commands if they are not found automatically.
        poll_freq: Frequency with which to poll a submitted job. Always is >= 5.
        cleanup: Whether to perform cleanup or not on remote machine.
        cache_dir: Local cache directory used by this executor for temporary files.
        log_stdout: The path to the file to be used for redirecting stdout.
        log_stderr: The path to the file to be used for redirecting stderr.
        time_limit: time limit for the task
        retries: Number of times to retry execution upon failure
        ignore_versions: Whether to ignore the Python, Covalent, and Cloudpickle version mismatch on the remote machine and try running the task anyway. Default is False.
    """

    def __init__(
        self,
        username: Optional[str] = None,
        address: Optional[str] = None,
        ssh_key_file: Optional[str] = None,
        cert_file: Optional[str] = None,
        # added ssh port option: Rajarshi
        ssh_port: Optional[int] = 22,
        remote_workdir: Optional[str] = None,
        create_unique_workdir: bool = False,
        options: Optional[Dict] = None,
        variables: Optional[Dict[str, str]] = None,
        conda_env: Optional[str] = None,
        prerun_commands: Optional[List[str]] = None,
        postrun_commands: Optional[List[str]] = None,
        use_srun: Optional[bool] = None,
        srun_options: Optional[Dict] = None,
        srun_append: Optional[str] = None,
        bashrc_path: Optional[str] = None,
        slurm_path: Optional[str] = None,
        poll_freq: Optional[int] = None,
        cleanup: Optional[bool] = None,
        cache_dir: Optional[str] = None,
        *,
        log_stdout: str = "",
        log_stderr: str = "",
        time_limit: int = -1,
        retries: int = 0,
        ignore_versions: bool = None,
    ):
        super().__init__(
            log_stdout=log_stdout, log_stderr=log_stderr, time_limit=time_limit, retries=retries
        )

        self.username = username or get_config("executors.slurm.username")
        self.address = address or get_config("executors.slurm.address")
        self.ssh_key_file = ssh_key_file or get_config("executors.slurm.ssh_key_file")
        self.cert_file = cert_file or get_config("executors.slurm").get("cert_file", None)
        # add ssh port: Rajarshi
        self.ssh_port = ssh_port or get_config("executors.slurm").get("ssh_port", 22)
        self.remote_workdir = remote_workdir or get_config("executors.slurm.remote_workdir")
        self.variables = variables or get_config("executors.slurm.variables")
        self.conda_env = conda_env or get_config("executors.slurm.conda_env")
        self.prerun_commands = prerun_commands or get_config("executors.slurm.prerun_commands")
        self.postrun_commands = postrun_commands or get_config("executors.slurm.postrun_commands")
        self.srun_append = srun_append or get_config("executors.slurm.srun_append")
        self.slurm_path = slurm_path or get_config("executors.slurm.slurm_path")
        self.poll_freq = poll_freq or get_config("executors.slurm.poll_freq")
        self.cache_dir = Path(cache_dir or get_config("executors.slurm.cache_dir"))
        self.ignore_versions = (
            ignore_versions
            if ignore_versions is not None
            else get_config("executors.slurm.ignore_versions")
        )

        # Resolve ssh_key_file and cert_file to absolute paths.
        if self.ssh_key_file:
            self.ssh_key_file = str(Path(self.ssh_key_file).expanduser().resolve())
        if self.cert_file:
            self.cert_file = str(Path(self.cert_file).expanduser().resolve())

        # Allow user to override bashrc_path with empty string.
        self.bashrc_path = (
            "" if bashrc_path == "" else (bashrc_path or get_config("executors.slurm.bashrc_path"))
        )

        self.srun_options = deepcopy(srun_options or get_config("executors.slurm.srun_options"))
        self.options = deepcopy(options or get_config("executors.slurm.options"))
        self.options.update(parsable="")

        self.create_unique_workdir = (
            get_config("executors.slurm.create_unique_workdir")
            if create_unique_workdir is None
            else create_unique_workdir
        )
        self.use_srun = get_config("executors.slurm.use_srun") if use_srun is None else use_srun
        self.cleanup = get_config("executors.slurm.cleanup") if cleanup is None else cleanup
        # Force minimum value on `poll_freq`.
        if self.poll_freq < 5:
            app_log.info("Increasing poll_freq to the minimum allowed: 5 seconds.")
            self.poll_freq = 5

        # Create cache dir if it doesn't exist.
        if not self.cache_dir.exists():
            self.cache_dir.mkdir(parents=True)

    async def _client_connect(self) -> asyncssh.SSHClientConnection:
        """
        Helper function for connecting to the remote host through asyncssh module.

        Args:
            None

        Returns:
            The connection object
        """

        if not self.username:
            raise ValueError("username is a required parameter in the Slurm plugin.")

        if not self.address:
            raise ValueError("address is a required parameter in the Slurm plugin.")

        if not self.ssh_key_file:
            raise ValueError("ssh_key_file is a required parameter in the Slurm plugin.")

        if self.cert_file and not Path(self.cert_file).exists():
            raise FileNotFoundError(f"Certificate file not found: {self.cert_file}")

        if not Path(self.ssh_key_file).exists():
            raise FileNotFoundError(f"SSH key file not found: {self.ssh_key_file}")

        if self.cert_file:
            client_keys = [
                (
                    asyncssh.read_private_key(self.ssh_key_file),
                    asyncssh.read_certificate(self.cert_file),
                )
            ]
        else:
            client_keys = [asyncssh.read_private_key(self.ssh_key_file)]

        try:
            conn = await asyncssh.connect(
                self.address,
                port=self.ssh_port,
                username=self.username,
                client_keys=client_keys,
                known_hosts=None,
            )

        except Exception as e:
            raise RuntimeError(
                f"Could not connect to host: '{self.address}' "
                f"as user: '{self.username}' "
                f"with key file: '{self.ssh_key_file}'"
            ) from e

        return conn

    async def perform_cleanup(
        self,
        conn: asyncssh.SSHClientConnection,
        remote_func_filename: str,
        remote_slurm_filename: str,
        remote_py_filename: str,
        remote_result_filename: str,
        remote_stdout_filename: str,
        remote_stderr_filename: str,
    ) -> None:
        """
        Function to perform cleanup on remote machine

        Args:
            conn: SSH connection object
            remote_func_filename: Function file on remote machine
            remote_slurm_filename: Slurm script file on remote machine
            remote_py_filename: Python script file on remote machine
            remote_result_filename: Result file on remote machine
            remote_stdout_filename: Standard out file on remote machine
            remote_stderr_filename: Standard error file on remote machine

        Returns:
            None
        """

        await conn.run(f"rm {remote_func_filename}")
        await conn.run(f"rm {remote_slurm_filename}")
        await conn.run(f"rm {remote_py_filename}")
        await conn.run(f"rm {remote_result_filename}")
        await conn.run(f"rm {remote_stdout_filename}")
        await conn.run(f"rm {remote_stderr_filename}")

    def _format_submit_script(
        self,
        python_version: str,
        py_filename: str,
        func_filename: str,
        result_filename: str,
        current_remote_workdir: str,
    ) -> str:
        """Create the SLURM that defines the job, uses srun to run the python script.

        Args:
            python_version: Python version required by the pickled function.
            py_filename: Name of the python script.
            func_filename: Name of the pickled function file.
            result_filename: Name of the pickled result file.
            current_remote_workdir: Current working directory on the remote machine.

        Returns:
            script: String object containing a script parsable by sbatch.
        """

        self.options["chdir"] = current_remote_workdir

        job_script = JobScript(
            sbatch_options=self.options,
            srun_options=self.srun_options,
            variables=self.variables,
            bashrc_path=self.bashrc_path,
            conda_env=self.conda_env,
            prerun_commands=self.prerun_commands,
            srun_append=self.srun_append,
            postrun_commands=self.postrun_commands,
            use_srun=self.use_srun,
            ignore_versions=self.ignore_versions,
        )

        return job_script.format(
            python_version=python_version,
            remote_py_filename=py_filename,
            func_filename=func_filename,
            result_filename=result_filename,
        )

    async def get_status(
        self,
        info_dict: dict,
        conn: asyncssh.SSHClientConnection,
    ) -> Union[Result, str]:
        """Query the status of a job previously submitted to Slurm.

        Args:
            info_dict: a dictionary containing all necessary parameters needed to query the
                status of the execution. Required keys in the dictionary are:
                    A string mapping "job_id" to Slurm job ID.
            conn: SSH connection object.

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
            proc_verify_scontrol = await conn.run(_LOAD_SLURM_PREFIX + "which scontrol")

            if proc_verify_scontrol.returncode != 0:
                raise RuntimeError("Please provide `slurm_path` to run scontrol command")

            cmd_scontrol = _LOAD_SLURM_PREFIX + cmd_scontrol

        proc = await conn.run(cmd_scontrol)
        return proc.stdout.strip()

    async def _poll_slurm(
        self,
        job_id: int,
        conn: asyncssh.SSHClientConnection,
    ) -> str:
        """Poll a Slurm job until completion.

        Args:
            job_id: Slurm job ID.
            conn: SSH connection object.

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

        return status

    async def _query_logs(
        self,
        task_results_dir: Path,
        current_remote_workdir: Path,
        conn: asyncssh.SSHClientConnection,
    ) -> Tuple[str, str]:
        """Query and retrieve the job logs.

        Args:
            task_metadata: Dictionary of metadata associated with the task.
            conn: SSH connection object.
            current_remote_workdir: Current working directory on the remote filesystem.

        Returns:
            (stdout, stderr): Contents of the stdout and stderr log files.
        """

        stdout_file = task_results_dir / self.options["output"]
        stderr_file = task_results_dir / self.options["error"]

        await asyncssh.scp((conn, current_remote_workdir / self.options["output"]), stdout_file)
        await asyncssh.scp((conn, current_remote_workdir / self.options["error"]), stderr_file)

        async with aiofiles.open(stdout_file, "r") as f:
            stdout = await f.read()
        await async_os.remove(stdout_file)

        async with aiofiles.open(stderr_file, "r") as f:
            stderr = await f.read()
        await async_os.remove(stderr_file)

        return stdout, stderr

    async def _query_result(
        self,
        remote_result_filename: Path,
        task_results_dir: Path,
        conn: asyncssh.SSHClientConnection,
    ) -> Tuple[Any, str, str, Optional[Exception]]:
        """Query and retrieve the task result including stdout and stderr logs.

        Args:
            result_filename: Name of the pickled result file.
            task_results_dir: Directory on the Covalent server where the result will be copied.
            conn: SSH connection object.
            current_remote_workdir: Current working directory on the remote machine.

        Returns:
            result: Task result.
        """

        # Check the result file exists on the remote backend
        proc = await conn.run(f"test -e {remote_result_filename!s}")
        if proc.returncode != 0:
            raise FileNotFoundError(proc.returncode, proc.stderr.strip(), remote_result_filename)

        # Copy result file from remote machine to Covalent server
        local_result_filename = task_results_dir / remote_result_filename.name
        await asyncssh.scp((conn, remote_result_filename), local_result_filename)

        async with aiofiles.open(local_result_filename, "rb") as f:
            contents = await f.read()
            result, exception = pickle.loads(contents)
        await async_os.remove(local_result_filename)

        stdout, stderr = await self._query_logs(
            task_results_dir=task_results_dir,
            current_remote_workdir=remote_result_filename.parent,
            conn=conn,
        )

        return result, stdout, stderr, exception

    async def _copy_files(
        self,
        dispatch_id: str,
        node_id: str,
        func_tuple: Tuple[Callable, List, Dict],
        conn: asyncssh.SSHClientConnection,
    ) -> Dict[str, Path]:
        """Copy files to remote machine.

        Args:
            dispatch_id: Workflow dispatch ID.
            node_id: Workflow node ID.
            func_tuple: The wrapped electron function, its args, and its kwargs.
            conn: SSH connection object.

        Returns:
            remote_paths: Dictionary of paths on the remote filesystem.
        """

        result_filename = f"result-{dispatch_id}-{node_id}.pkl"
        slurm_filename = f"slurm-{dispatch_id}-{node_id}.sh"
        py_script_filename = f"script-{dispatch_id}-{node_id}.py"
        func_filename = f"func-{dispatch_id}-{node_id}.pkl"

        if "output" not in self.options:
            self.options["output"] = f"stdout-{dispatch_id}-{node_id}.log"
        if "error" not in self.options:
            self.options["error"] = f"stderr-{dispatch_id}-{node_id}.log"

        remote_workdir = Path(self.remote_workdir)
        if self.create_unique_workdir:
            current_remote_workdir = remote_workdir / f"{dispatch_id}/node_{node_id}"
        else:
            current_remote_workdir = remote_workdir

        # Create the remote directory
        app_log.debug("Creating remote work directory %s ...", current_remote_workdir)
        cmd_mkdir_remote = f"mkdir -p {current_remote_workdir!s}"
        proc_mkdir_remote = await conn.run(cmd_mkdir_remote)

        if client_err := proc_mkdir_remote.stderr.strip():
            raise RuntimeError(client_err)

        function = func_tuple[0]
        py_version_func = ".".join(function.args[0].python_version.split(".")[:2])
        app_log.debug("Python version: %s", py_version_func)

        # Pickle the function, write to file, and copy to remote filesystem
        async with aiofiles.tempfile.NamedTemporaryFile(dir=self.cache_dir) as temp_f:
            app_log.debug("Writing pickled function, args, kwargs to file...")
            await temp_f.write(pickle.dumps(func_tuple))
            await temp_f.flush()

            remote_func_filename = current_remote_workdir / func_filename
            app_log.debug("Copying pickled function to remote fs: %s ...", remote_func_filename)
            await asyncssh.scp(temp_f.name, (conn, remote_func_filename))

        # Format the function execution script, write to file, and copy to remote filesystem
        async with aiofiles.tempfile.NamedTemporaryFile(dir=self.cache_dir, mode="w") as temp_g:
            python_exec_script = (Path(__file__).parent / "exec.py").read_text("utf-8")
            app_log.debug("Writing python run-function script to tempfile...")
            await temp_g.write(python_exec_script)
            await temp_g.flush()

            remote_py_script_filename = current_remote_workdir / py_script_filename
            app_log.debug(
                "Copying python run-function to remote fs: %s", remote_py_script_filename
            )
            await asyncssh.scp(temp_g.name, (conn, remote_py_script_filename))

        # Format the SLURM submit script, write to file, and copy to remote filesystem
        async with aiofiles.tempfile.NamedTemporaryFile(dir=self.cache_dir, mode="w") as temp_h:
            self.options["job-name"] = (
                self.options.get("job-name", None) or f"covalent-{node_id}-{dispatch_id}"
            )
            slurm_submit_script = self._format_submit_script(
                python_version=py_version_func,
                py_filename=py_script_filename,
                func_filename=func_filename,
                result_filename=result_filename,
                current_remote_workdir=current_remote_workdir,
            )
            app_log.debug("Writing slurm submit script to tempfile...")
            await temp_h.write(slurm_submit_script)
            await temp_h.flush()

            remote_slurm_filename = current_remote_workdir / slurm_filename
            app_log.debug("Copying slurm submit script to remote fs: %s", remote_slurm_filename)
            await asyncssh.scp(temp_h.name, (conn, remote_slurm_filename))

        return {
            "slurm": remote_slurm_filename,
            "py": remote_py_script_filename,
            "func": remote_func_filename,
            "result": current_remote_workdir / result_filename,
        }

    async def run(
        self, function: Callable, args: List, kwargs: Dict, task_metadata: Dict
    ) -> Optional[Any]:
        """Run a function on a remote machine using Slurm.

        Args:
            function: Function to be executed.
            args: List of positional arguments to be passed to the function.
            kwargs: Dictionary of keyword arguments to be passed to the function.
            task_metadata: Dictionary of metadata associated with the task.

        Returns:
            result: Result object containing the result of the function execution.
        """

        dispatch_id = task_metadata["dispatch_id"]
        node_id = task_metadata["node_id"]
        result = None
        exception = None
        stdout = ""
        stderr = ""
        task_results_dir = Path(task_metadata["results_dir"]) / dispatch_id

        conn = await self._client_connect()

        # Copy files to remote
        remote_paths = await self._copy_files(
            dispatch_id=dispatch_id,
            node_id=node_id,
            func_tuple=(function, args, kwargs),
            conn=conn,
        )

        # Submit the job script with `sbatch`.
        app_log.debug("Running the script: %s", remote_paths["slurm"])
        cmd_sbatch = f"sbatch {remote_paths['slurm']}"
        if self.slurm_path:
            app_log.debug("Exporting slurm path for sbatch...")
            cmd_sbatch = f"export PATH=$PATH:{self.slurm_path} && {cmd_sbatch}"
        else:
            app_log.debug("Verifying slurm installation for sbatch...")
            proc_verify_sbatch = await conn.run(_LOAD_SLURM_PREFIX + "which sbatch")
            if proc_verify_sbatch.returncode != 0:
                raise RuntimeError("Please provide `slurm_path` to run sbatch command")
            cmd_sbatch = _LOAD_SLURM_PREFIX + cmd_sbatch

        proc = await conn.run(cmd_sbatch)
        if proc.returncode != 0:
            raise RuntimeError(proc.stderr.strip())

        # Poll for result.
        app_log.debug("Job submitted with stdout: %s", proc.stdout.strip())
        slurm_job_id = int(re.findall("[0-9]+", proc.stdout.strip())[0])

        app_log.debug("Polling slurm with job_id: %s ...", slurm_job_id)
        status = await self._poll_slurm(slurm_job_id, conn)

        if "COMPLETED" in status:
            app_log.debug("Querying result with job_id: %s ...", slurm_job_id)
            result, stdout, stderr, exception = await self._query_result(
                remote_result_filename=remote_paths["result"],
                task_results_dir=task_results_dir,
                conn=conn,
            )
        else:
            # Result will be None. Recover errors from Slurm job log files.
            app_log.debug("Job submission FAILED with status:\n%s\n", status)
            _, stderr = await self._query_logs(
                task_results_dir=task_results_dir,
                current_remote_workdir=remote_paths["result"].parent,
                conn=conn,
            )
            exception = RuntimeError(stderr)

        print(stdout)
        print(stderr, file=sys.stderr)

        if exception:
            raise RuntimeError(exception)

        app_log.debug("Preparing for teardown...")
        self._remote_func_filename = remote_paths["func"]
        self._remote_slurm_filename = remote_paths["slurm"]
        self._remote_py_script_filename = remote_paths["py"]
        self._result_filename = remote_paths["result"]

        app_log.debug("Closing SSH connection...")
        conn.close()
        await conn.wait_closed()
        app_log.debug("SSH connection closed, returning result")

        return result

    async def teardown(self, task_metadata: Dict) -> None:
        """Perform cleanup on remote machine.

        Args:
            task_metadata: Dictionary of metadata associated with the task.

        Returns:
            None
        """
        if self.cleanup:
            try:
                app_log.debug("Performing cleanup on remote...")
                conn = await self._client_connect()
                current_remote_workdir = self._result_filename.parent
                await self.perform_cleanup(
                    conn=conn,
                    remote_func_filename=self._remote_func_filename,
                    remote_slurm_filename=self._remote_slurm_filename,
                    remote_py_filename=self._remote_py_script_filename,
                    remote_result_filename=self._result_filename,
                    remote_stdout_filename=current_remote_workdir / self.options["output"],
                    remote_stderr_filename=current_remote_workdir / self.options["error"],
                )

                app_log.debug("Closing SSH connection...")
                conn.close()
                await conn.wait_closed()
                app_log.debug("SSH connection closed, teardown complete")
            except Exception:
                app_log.warning("Slurm cleanup could not successfully complete. Nonfatal error.")
