# Copyright 2021 Agnostiq Inc.
#
# This file is a part of Covalent Cloud.

"""Slurm executor for the Covalent dispatcher."""

import tempfile
import cloudpickle as pickle
import subprocess

from typing import Any, Dict, Optional

from covalent._shared_files.util_classes import DispatchInfo
from covalent._workflow.transport import TransportableObject
from covalent.executor import BaseExecutor

executor_plugin_name = "SlurmExecutor"

class SlurmExecutor(BaseExecutor):
    def __init__(self, address: str, ssh_key_file: str, remote_workdir: str, *args, **kwargs):
        super().__init__(*args, **kwargs)
        
        self.address = address
        self.ssh_key_file = ssh_key_file

    def execute(
        self,
        function: TransportableObject,
        kwargs: Any,
        execution_args: dict,
        dispatch_id: str,
        node_id: int = -1,
    ) -> Any:

        dispatch_info = DispatchInfo(dispatch_id)

        with self.get_dispatch_context(dispatch_info), tempfile.NamedTemporaryFile(dir=self.cache_dir) as f, tempfile.NamedTemporaryFile(dir=self.cache_dir) as g:
            #print(f"Execution args: {execution_args}")

            # Strip off Covalent metadata
            results_dir = execution_args["results_dir"]
            del execution_args["results_dir"]

            time_limit = execution_args["time_limit"]
            del execution_args["time_limit"]

            # Write the serialized function to file
            pickle.dump(function, f)

            # Copy the function to the remote filesystem
            subprocess.run(["rsync", "-az", f.name, f"{address}:{remote_workdir}"])

            # Format the SLURM submit script
            slurm_preamble = "#!/bin/bash\n"
            slurm_preamble += "#SBATCH --parsable\n"
            for k, v in execution_args.items():
                slurm_preamble += f"#SBATCH --{key}={value}\n"
            slurm_preamble += "\n"

            slurm_conda = ("""
source $HOME/.bashrc
conda activate {conda_env}
retval=$?
if [ $retval -ne 0 ] ; then
  >&2 echo "Conda environment {conda_env} is not present on the compute node. Please create the environment and try again."
  exit 99
fi

""".format(conda_env=self.conda_env) if conda_env else "")

            slurm_body = """
python - <<EOF
import cloudpickle as pickle

with open("{func_filename}", "rb") as f:
    function = pickle.load(f).get_deserialized()

result = fn(**{kwargs})

with open("{result_filename}", "wb") as f:
    pickle.dump(result, f)
EOF

wait""".format(
                func_filename=f.name,
                kwargs=kwargs,
                result_filename=f"{remote_workdir}/{result_filename}"
            )

        return None
