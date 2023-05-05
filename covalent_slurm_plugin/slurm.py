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
from datetime import datetime
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
    "cert_file": None,
    "remote_workdir": "covalent-workdir",
    "slurm_path": None,
    "conda_env": None,
    "cache_dir": str(Path(get_config("dispatcher.cache_dir")).expanduser().resolve()),
    "options": {
        "parsable": "",
    },
    "poll_freq": 60,
    "srun_options": {},
    "srun_append": None,
    "prerun_commands": None,
    "postrun_commands": None,
    "cleanup": True,
}

executor_plugin_name = "SlurmExecutor"


class SlurmExecutor(AsyncBaseExecutor):
    """Slurm executor plugin class.

    Args:
        username: Username used to authenticate over SSH.
        address: Remote address or hostname of the Slurm login node.
        ssh_key_file: Private RSA key used to authenticate over SSH.
        cert_file: Certificate file used to authenticate over SSH, if required.
        remote_workdir: Working directory on the remote cluster.
        slurm_path: Path to the slurm commands if they are not found automatically.
        conda_env: Name of conda environment on which to run the function.
        cache_dir: Cache directory used by this executor for temporary files.
        options: Dictionary of parameters used to build a Slurm submit script.
        srun_options: Dictionary of parameters passed to srun inside submit script.
        srun_append: Command nested into srun call.
        prerun_commands: List of shell commands to run before submitting with srun.
        postrun_commands: List of shell commands to run after submitting with srun.
        poll_freq: Frequency with which to poll a submitted job.
        cleanup: Whether to perform cleanup or not on remote machine.
    """

    def __init__(
        self,
        username: str = None,
        address: str = None,
        ssh_key_file: str = None,
        cert_file: str = None,
        remote_workdir: str = None,
        slurm_path: str = None,
        conda_env: str = None,
        cache_dir: str = None,
        options: Dict = None,
        sshproxy: Dict = None,
        srun_options: Dict = None,
        srun_append: str = None,
        prerun_commands: List[str] = None,
        postrun_commands: List[str] = None,
        poll_freq: int = None,
        cleanup: bool = None,
        **kwargs,
    ):
        super().__init__(**kwargs)

        self.username = username or get_config("executors.slurm.username")
        if not self.username:
            raise ValueError("username is a required parameter in the Slurm plugin.")

        self.address = address or get_config("executors.slurm.address")
        if not self.address:
            raise ValueError("address is a required parameter in the Slurm plugin.")

        self.ssh_key_file = ssh_key_file or get_config("executors.slurm.ssh_key_file")
        if not self.ssh_key_file:
            raise ValueError("ssh_key_file is a required parameter in the Slurm plugin.")
        self.ssh_key_file = str(Path(ssh_key_file).expanduser().resolve())

        try:
            self.cert_file = cert_file or get_config("executors.slurm.cert_file")
            self.cert_file = str(Path(self.cert_file).expanduser().resolve())
        except KeyError:
            self.cert_file = None

        self.remote_workdir = remote_workdir or get_config("executors.slurm.remote_workdir")

        try:
            self.slurm_path = slurm_path or get_config("executors.slurm.slurm_path")
        except KeyError:
            self.slurm_path = None

        try:
            self.conda_env = conda_env or get_config("executors.slurm.conda_env")
        except KeyError:
            self.conda_env = None

        cache_dir = cache_dir or get_config("executors.slurm.cache_dir")
        self.cache_dir = str(Path(cache_dir).expanduser().resolve())

        # To allow passing empty dictionary
        if options is None:
            options = get_config("executors.slurm.options")
        self.options = deepcopy(options)

        if sshproxy is None:
            try:
                sshproxy = get_config("executors.slurm.sshproxy")
            except KeyError:
                sshproxy = {}
        self.sshproxy = deepcopy(sshproxy)

        if srun_options is None:
            srun_options = get_config("executors.slurm.srun_options")
        self.srun_options = deepcopy(srun_options)

        try:
            self.srun_append = srun_append or get_config("executors.slurm.srun_append")
        except KeyError:
            self.srun_append = None

        self.prerun_commands = list(prerun_commands) if prerun_commands else []
        self.postrun_commands = list(postrun_commands) if postrun_commands else []

        self.poll_freq = poll_freq or get_config("executors.slurm.poll_freq")
        if self.poll_freq < 60:
            print("Polling frequency will be increased to the minimum for Slurm: 60 seconds.")
            self.poll_freq = 60

        self.cleanup = cleanup or get_config("executors.slurm.cleanup")

        # Ensure that the slurm data is parsable
        if "parsable" not in self.options:
            self.options["parsable"] = ""

        self.LOAD_SLURM_PREFIX = "source /etc/profile\n module whatis slurm &> /dev/null\n if [ $? -eq 0 ] ; then\n module load slurm\n fi\n"

    async def _client_connect(self) -> asyncssh.SSHClientConnection:
        """
        Helper function for connecting to the remote host through asyncssh module.

        Args:
            None

        Returns:
            The connection object
        """

        if self.sshproxy and self.address in self.sshproxy["hosts"]:
            try:
                import oathtool
            except ImportError:
                raise RuntimeError(
                    "To use 'sshproxy' options, reinstall the Slurm plugin as 'pip install covalent-slurm-plugin[sshproxy]'"
                )

            # Validate the certificate is not expired
            valid_cert = False
            if self.cert_file and Path(self.cert_file).exists():
                proc = await asyncio.create_subprocess_shell(
                    f"ssh-keygen -L -f {self.cert_file} | awk '/Valid/ " + "{print $5}'",
                    stdout=asyncio.subprocess.PIPE,
                    stderr=asyncio.subprocess.PIPE,
                )
                stdout, stderr = await proc.communicate()

                if proc.returncode != 0:
                    raise RuntimeError(
                        "Failed to identify the expiration of the SSH key. Is this key compatible with sshproxy?"
                    )

                expiration = datetime.strptime(stdout.decode().rstrip(), "%Y-%m-%dT%H:%M:%S")
                if expiration > datetime.now():
                    valid_cert = True

                app_log.debug(f"Certificate expiration: {stdout.decode()}")

            if not valid_cert:
                app_log.debug("Requesting new key and certificate")
                password = self.sshproxy["password"]
                otp = oathtool.generate_otp(self.sshproxy["secret"])

                proc = await asyncio.create_subprocess_shell(
                    f"sshproxy -u {self.username} -o {self.ssh_key_file}",
                    stdin=asyncio.subprocess.PIPE,
                    stdout=asyncio.subprocess.PIPE,
                    stderr=asyncio.subprocess.PIPE,
                )
                stdout, stderr = await proc.communicate(input=f"{password}{otp}".encode())

                if proc.returncode != 0:
                    raise RuntimeError(f"sshproxy failed to retrieve a key: {stderr.decode()}")

                if not self.cert_file:
                    self.cert_file = Path(self.ssh_key_file).parents[0] / "nersc-cert.pub"

                app_log.debug("sshproxy successful")

        if self.cert_file and not os.path.exists(self.cert_file):
            raise FileNotFoundError(f"Certificate file not found: {self.cert_file}")

        if not os.path.exists(self.ssh_key_file):
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
                username=self.username,
                client_keys=client_keys,
                known_hosts=None,
            )

        except Exception as e:
            raise RuntimeError(
                f"Could not connect to host: '{self.address}' as user: '{self.username}'", e
            )

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
    ) -> str:
        """Create the SLURM that defines the job, uses srun to run the python script.

        Args:
            python_version: Python version required by the pickled function.

        Returns:
            script: String object containing a script parsable by sbatch.
        """

        # preamble
        slurm_preamble = "#!/bin/bash\n"
        for key, value in self.options.items():
            slurm_preamble += "#SBATCH "
            if len(key) == 1:
                slurm_preamble += f"-{key}" + (f" {value}" if value else "")
            else:
                slurm_preamble += f"--{key}" + (f"={value}" if value else "")
            slurm_preamble += "\n"
        slurm_preamble += "\n"

        if hasattr(self, "conda_env") and self.conda_env:
            conda_env_str = self.conda_env
        else:
            conda_env_str = ""

        # sets up conda environment
        slurm_conda = f"""
source $HOME/.bashrc
conda activate {conda_env_str}
retval=$?
if [ $retval -ne 0 ] ; then
  >&2 echo "Conda environment {conda_env_str} is not present on the compute node. "\
  "Please create the environment and try again."
  exit 99
fi

"""
        # checks remote python version
        slurm_python_version = f"""
remote_py_version=$(python -c "print('.'.join(map(str, __import__('sys').version_info[:2])))")
if [[ "{python_version}" != $remote_py_version ]] ; then
  >&2 echo "Python version mismatch. Please install Python {python_version} in the compute environment."
  exit 199
fi
"""
        # runs pre-run commands
        if self.prerun_commands:
            slurm_prerun_commands = "\n".join([""] + self.prerun_commands + [""])
        else:
            slurm_prerun_commands = ""

        # uses srun to run script calling pickled function
        srun_options_str = ""
        for key, value in self.srun_options.items():
            srun_options_str += " "
            if len(key) == 1:
                srun_options_str += f"-{key}" + (f" {value}" if value else "")
            else:
                srun_options_str += f"--{key}" + (f"={value}" if value else "")

        remote_py_filename = os.path.join(self.remote_workdir, py_filename)
        slurm_srun = f"srun{srun_options_str} \\"

        if self.srun_append:
            # insert any appended commands
            slurm_srun += f"""
{self.srun_append} \\
"""
        else:
            slurm_srun += """
"""

        slurm_srun += f"python {remote_py_filename}"

        # runs post-run commands
        if self.postrun_commands:
            slurm_postrun_commands = "\n".join([""] + self.postrun_commands + [""])
        else:
            slurm_postrun_commands = ""

        # assemble commands into slurm body
        slurm_body = "\n".join([slurm_prerun_commands, slurm_srun, slurm_postrun_commands, "wait"])

        # assemble script
        return "".join([slurm_preamble, slurm_conda, slurm_python_version, slurm_body])

    def _format_py_script(
        self,
        func_filename: str,
        result_filename: str,
    ) -> str:
        """Create the Python script that executes the pickled python function.

        Args:
            func_filename: Name of the pickled function.
            result_filename: Name of the pickled result.

        Returns:
            script: String object containing a script parsable by sbatch.
        """
        func_filename = os.path.join(self.remote_workdir, func_filename)
        result_filename = os.path.join(self.remote_workdir, result_filename)
        return f"""
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
"""

    async def get_status(
        self, info_dict: dict, conn: asyncssh.SSHClientConnection
    ) -> Union[Result, str]:
        """Query the status of a job previously submitted to Slurm.

        Args:
            info_dict: a dictionary containing all necessary parameters needed to query the
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
        task_results_dir = os.path.join(results_dir, dispatch_id)

        result_filename = f"result-{dispatch_id}-{node_id}.pkl"
        slurm_filename = f"slurm-{dispatch_id}-{node_id}.sh"
        py_script_filename = f"script-{dispatch_id}-{node_id}.py"
        func_filename = f"func-{dispatch_id}-{node_id}.pkl"

        if "output" not in self.options:
            self.options["output"] = os.path.join(
                self.remote_workdir, f"stdout-{dispatch_id}-{node_id}.log"
            )
        if "error" not in self.options:
            self.options["error"] = os.path.join(
                self.remote_workdir, f"stderr-{dispatch_id}-{node_id}.log"
            )

        result = None

        conn = await self._client_connect()

        py_version_func = ".".join(function.args[0].python_version.split(".")[:2])
        app_log.debug(f"Python version: {py_version_func}")

        # Create the remote directory
        app_log.debug(f"Creating remote work directory {self.remote_workdir} ...")
        cmd_mkdir_remote = f"mkdir -p {self.remote_workdir}"
        proc_mkdir_cache = await conn.run(cmd_mkdir_remote)

        if client_err := proc_mkdir_cache.stderr.strip():
            raise RuntimeError(client_err)

        async with aiofiles.tempfile.NamedTemporaryFile(dir=self.cache_dir) as temp_f:
            # Pickle the function, write to file, and copy to remote filesystem
            app_log.debug("Writing pickled function, args, kwargs to file...")
            await temp_f.write(pickle.dumps((function, args, kwargs)))
            await temp_f.flush()

            remote_func_filename = os.path.join(self.remote_workdir, func_filename)
            app_log.debug(f"Copying pickled function to remote fs: {remote_func_filename} ...")
            await asyncssh.scp(temp_f.name, (conn, remote_func_filename))

        async with aiofiles.tempfile.NamedTemporaryFile(dir=self.cache_dir, mode="w") as temp_g:
            # Format the function execution script, write to file, and copy to remote filesystem
            python_exec_script = self._format_py_script(func_filename, result_filename)
            app_log.debug("Writing python run-function script to tempfile...")
            await temp_g.write(python_exec_script)
            await temp_g.flush()

            remote_py_script_filename = os.path.join(self.remote_workdir, py_script_filename)
            app_log.debug(f"Copying python run-function to remote fs: {remote_py_script_filename}")
            await asyncssh.scp(temp_g.name, (conn, remote_py_script_filename))

        async with aiofiles.tempfile.NamedTemporaryFile(dir=self.cache_dir, mode="w") as temp_h:
            # Format the SLURM submit script, write to file, and copy to remote filesystem
            slurm_submit_script = self._format_submit_script(py_version_func, py_script_filename)
            app_log.debug("Writing slurm submit script to tempfile...")
            await temp_h.write(slurm_submit_script)
            await temp_h.flush()

            remote_slurm_filename = os.path.join(self.remote_workdir, slurm_filename)
            app_log.debug(f"Copying slurm submit script to remote fs: {remote_slurm_filename} ...")
            await asyncssh.scp(temp_h.name, (conn, remote_slurm_filename))

        # Execute the script
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
        self._remote_py_script_filename = remote_py_script_filename

        app_log.debug("Closing SSH connection...")
        conn.close()
        await conn.wait_closed()
        app_log.debug("SSH connection closed, returning result")

        return result

    async def teardown(self, task_metadata: Dict):
        if self.cleanup:
            try:
                app_log.debug("Performing cleanup on remote...")
                conn = await self._client_connect()
                await self.perform_cleanup(
                    conn=conn,
                    remote_func_filename=self._remote_func_filename,
                    remote_slurm_filename=self._remote_slurm_filename,
                    remote_py_filename=self._remote_py_script_filename,
                    remote_result_filename=os.path.join(
                        self.remote_workdir, self._result_filename
                    ),
                    remote_stdout_filename=self.options["output"],
                    remote_stderr_filename=self.options["error"],
                )

                app_log.debug("Closing SSH connection...")
                conn.close()
                await conn.wait_closed()
                app_log.debug("SSH connection closed, teardown complete")
            except Exception:
                app_log.warning("Slurm cleanup could not successfully complete. Nonfatal error.")
