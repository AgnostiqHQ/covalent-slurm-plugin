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


import covalent as ct

from covalent_slurm_plugin import SlurmExecutor

slurm_executor = SlurmExecutor(
    username="slurmuser",
    address="localhost",
    ssh_key_file="./slurm_test",
    conda_env="covalent",
    ignore_versions=True,
)


@ct.lattice
@ct.electron(executor=slurm_executor)
def task():
    print("Hello World!")
    return 42


did = ct.dispatch(task)()
print(did)

res = ct.get_result(did, wait=True)
print(res)

if __name__ == "__main__":
    assert res.result == 42
