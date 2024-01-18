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

"""Tests for the @electron task execution script that runs on the SLURM cluster"""

from unittest import mock

import filelock
import pytest


def test_setup_import_error():
    """Test error message correctly reported in case of ImportError"""
    patched_import_covalent = mock.patch(
        "covalent_slurm_plugin.exec._import_covalent", side_effect=ImportError()
    )

    with patched_import_covalent:
        with pytest.raises(
            RuntimeError, match="The covalent SDK is not installed in the Slurm job environment."
        ):
            from covalent_slurm_plugin import exec

            exec._check_setup()


def test_setup_filelock_timeout_error():
    """Test error message correctly reported in case of filelock Timeout error"""
    patched_import_covalent = mock.patch(
        "covalent_slurm_plugin.exec._import_covalent",
        side_effect=filelock._error.Timeout("nonexistent_lock_file"),
    )

    with patched_import_covalent:
        with pytest.raises(RuntimeError, match="Failed to acquire file lock"):
            from covalent_slurm_plugin import exec

            exec._check_setup()
