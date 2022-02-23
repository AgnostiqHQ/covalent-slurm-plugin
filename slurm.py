# Copyright 2021 Agnostiq Inc.
#
# This file is a part of Covalent Cloud.

"""Slurm executor plugin for the Covalent dispatcher."""

import sys
import tempfile
import cloudpickle as pickle
import subprocess
from pathlib import Path
import shutil

from typing import Any, Dict, Optional

from covalent._shared_files.util_classes import DispatchInfo
from covalent._workflow.transport import TransportableObject
from covalent.executor import BaseExecutor

_EXECUTOR_PLUGIN_DEFAULTS = {
    "username": "",
    "address": "",
    "ssh_key_file": "",
    "remote_workdir": "",
    "cache_dir": "/tmp/covalent",
    "options": {
        "parsable": "",
    },
}

executor_plugin_name = "SlurmExecutor"
# TODO: Use os.path.join
# TODO: Abstract re-used strings and other code

class SlurmExecutor(BaseExecutor):
    def __init__(self, username: str, address: str, ssh_key_file: str, remote_workdir: str, options: Dict, **kwargs):
        super().__init__(**kwargs)
       
        self.username = username
        self.address = address
        self.ssh_key_file = ssh_key_file
        self.remote_workdir = remote_workdir
        self.options = options

    def execute(
        self,
        function: TransportableObject,
        kwargs: Any,
        execution_args: dict,
        dispatch_id: str,
        node_id: int = -1,
    ) -> Any:

        dispatch_info = DispatchInfo(dispatch_id)

        with self.get_dispatch_context(dispatch_info), tempfile.NamedTemporaryFile(dir=self.cache_dir) as f, tempfile.NamedTemporaryFile(dir=self.cache_dir, mode="w") as g:
            print(f"Execution args: {execution_args}")

            results_dir = execution_args["results_dir"]

            # Write the serialized function to file
            pickle.dump(function, f)
            f.flush()

            # Create the remote directory
            subprocess.run(["ssh", "-i", self.ssh_key_file, "-o", "StrictHostKeyChecking=no", "-o", "UserKnownHostsFile=/dev/null", "-o", "LogLevel=ERROR", f"{self.username}@{self.address}", "mkdir", "-p", self.remote_workdir], check=True, capture_output=True)

            # Copy the function to the remote filesystem
            func_filename = f"func-{dispatch_id}-{node_id}.pkl"
            subprocess.run(["rsync", "-e", f"ssh -i {self.ssh_key_file} -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o LogLevel=ERROR", f.name, f"{self.username}@{self.address}:{self.remote_workdir}/{func_filename}"], check=True, capture_output=True)

            # Format the SLURM submit script
            slurm_preamble = "#!/bin/bash\n"
            for key, value in self.options.items():
                slurm_preamble += "#SBATCH "
                slurm_preamble += f"-{key}" if len(key) == 1 else f"--{key}"
                if value:
                    slurm_preamble += f" {value}"
                slurm_preamble += "\n"
            slurm_preamble += "\n"

            slurm_conda = ("""
source $HOME/.bashrc
conda activate {conda_env}
retval=$?
if [ $retval -ne 0 ] ; then
  >&2 echo "Conda environment {conda_env} is not present on the compute node. Please create the environment and try again."
  exit 99
fi

""".format(conda_env=self.conda_env) if self.conda_env else "")

            slurm_python_version = """
if [[ "{python_version}" != `python -V | awk '{{print $2}}'` ]] ; then
  >&2 echo "Python version mismatch. Please install Python {python_version} in the compute environment."
  exit 199
fi
""".format(python_version=function.python_version)

            slurm_body = """
python - <<EOF
import cloudpickle as pickle

with open("{func_filename}", "rb") as f:
    function = pickle.load(f).get_deserialized()

result = function(**{kwargs})

with open("{result_filename}", "wb") as f:
    pickle.dump(result, f)
EOF

wait
""".format(
                func_filename=f"{self.remote_workdir}/{func_filename}",
                kwargs=kwargs,
                result_filename=f"{self.remote_workdir}/result-{dispatch_id}-{node_id}.pkl"
            )

            slurm_submit_script = slurm_preamble + slurm_conda + slurm_python_version + slurm_body
            g.write(slurm_submit_script)
            g.flush()

            # Copy the script to the local results directory
            slurm_filename = f"{results_dir}/{dispatch_id}/slurm-{dispatch_id}-{node_id}.sh"
            shutil.copyfile(g.name, slurm_filename)

            # Copy the script to the remote filesystem
            subprocess.run(["rsync", "-e", f"ssh -i {self.ssh_key_file} -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o LogLevel=ERROR", slurm_filename, f"{self.username}@{self.address}:{self.remote_workdir}"], check=True, capture_output=True)

            # Execute the script
            proc = subprocess.run(["ssh", "-i", self.ssh_key_file, "-o", "StrictHostKeyChecking=no", "-o", "UserKnownHostsFile=/dev/null", "-o", "LogLevel=ERROR", f"{self.username}@{self.address}", "bash", "-l", "-c", f"\"sbatch {self.remote_workdir}/slurm-{dispatch_id}-{node_id}.sh\""], check=True, capture_output=True)
            if proc.returncode == 0:
                slurm_job_id = proc.stdout.decode("utf-8").strip().split(";")[0]
            else:
                print(proc.stderr, file=sys.stdout)
                slurm_job_id = -1

        # TODO: Poll slurm status, return result instead of slurm job ID
        return slurm_job_id
