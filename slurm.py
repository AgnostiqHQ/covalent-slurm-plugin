# Copyright 2021 Agnostiq Inc.
#
# This file is a part of Covalent Cloud.

"""Slurm executor plugin for the Covalent dispatcher."""

import os
import shutil
import subprocess
import sys
import tempfile
import time
from pathlib import Path
from typing import Any, Dict, List, Optional

import cloudpickle as pickle
from covalent._shared_files.logger import app_log
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
        self.options = options

    def _format_submit_script(
        self,
        func_filename: str,
        result_filename: str,
        python_version: str,
        dispatch_id: str,
        node_id: int,
        args: List,
        kwargs: Dict,
    ) -> str:
        """Create a SLURM script which wraps a pickled Python function.

        Args:
            func_filename: Name of the pickled function.
            result_filename: Name of the pickled result.
            python_version: Python version required by the pickled function.
            dispatch_id: Workflow UUID.
            node_id: ID of the task within the workflow.
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
  >&2 echo "Conda environment {conda_env} is not present on the compute node. Please create the environment and try again."
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

    def execute(
        self,
        function: TransportableObject,
        args: List,
        kwargs: Dict,
        dispatch_id: str,
        results_dir: str,
        node_id: int = -1,
    ) -> Any:

        dispatch_info = DispatchInfo(dispatch_id)
        result_filename = f"result-{dispatch_id}-{node_id}.pkl"
        slurm_filename = f"slurm-{dispatch_id}-{node_id}.sh"
        task_results_dir = os.path.join(results_dir, dispatch_id)

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
                    f"ssh -i {self.ssh_key_file} -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o LogLevel=ERROR",
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
                dispatch_id,
                node_id,
                args,
                kwargs,
            )
            g.write(slurm_submit_script)
            g.flush()

            # Copy the script to the local results directory
            slurm_filename = os.path.join(task_results_dir, slurm_filename)
            shutil.copyfile(g.name, slurm_filename)

            # Copy the script to the remote filesystem
            subprocess.run(
                [
                    "rsync",
                    "-e",
                    f"ssh -i {self.ssh_key_file} -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o LogLevel=ERROR",
                    slurm_filename,
                    f"{self.username}@{self.address}:{self.remote_workdir}",
                ],
                check=True,
                capture_output=True,
            )

            # Execute the script
            remote_slurm_filename = os.path.join(self.remote_workdir, slurm_submit_script)
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
            app_log.warning(f"RETCODE: {proc.returncode}")
            if proc.returncode == 0:
                slurm_job_id = int(proc.stdout.decode("utf-8").strip().split(";")[0])
            else:
                print(proc.stderr, file=sys.stdout)
                return

            # TODO: Need to return stdout and stderr as well
            return self._poll_slurm(slurm_job_id, result_filename, task_results_dir)

    def _get_job_status(self, job_id: int) -> str:
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
                f'"scontrol show job {str(job_id)}"',
            ],
            check=True,
            capture_output=True,
        )
        return proc.stdout.decode("utf-8").strip()

    def _poll_slurm(self, job_id: int, result_filename: str, task_results_dir: str) -> Any:
        """Poll a Slurm job until completion and retrieve the result.

        Args:
            job_id: Slurm job ID.
            result_filename: Name of the pickled result file.
            task_results_dir: Directory on the Covalent server where the result will be copied.

        Returns:
            result: Task result.
        """

        # Poll status every `poll_freq` seconds
        status = self.get_job_status(job_id)
        while (
            "PENDING" in status
            or "RUNNING" in status
            or "COMPLETING" in status
            or "CONFIGURING" in status
        ):
            time.sleep(self.poll_freq)
            status = self.get_job_status(job_id)

        if "COMPLETED" not in status:
            raise Exception("Job failed with status:\n", status)

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
                f"ssh -i {self.ssh_key_file} -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o LogLevel=ERROR",
                f"{self.username}@{self.address}:{remote_result_filename}",
                task_results_dir,
            ],
            check=True,
            capture_output=True,
        )

        with open(result_filename, "rb") as f:
            result = pickle.load(f)

        return result
