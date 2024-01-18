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

import sys
import tempfile
from unittest import mock

import cloudpickle as pickle
import filelock
import pytest

from covalent_slurm_plugin import exec as exec_script


def test_setup_import_error():
    """Test error message correctly reported in case of ImportError"""

    patched_import_covalent = mock.patch(
        "covalent_slurm_plugin.exec._import_covalent", side_effect=ImportError()
    )

    with patched_import_covalent:
        with pytest.raises(
            RuntimeError, match="The covalent SDK is not installed in the Slurm job environment."
        ):
            exec_script._check_setup()


def test_setup_filelock_timeout_error():
    """Test error message correctly reported in case of filelock Timeout error"""

    patched_import_covalent = mock.patch(
        "covalent_slurm_plugin.exec._import_covalent",
        side_effect=filelock._error.Timeout("nonexistent_lock_file"),
    )

    with patched_import_covalent:
        with pytest.raises(RuntimeError, match="Failed to acquire file lock"):
            exec_script._check_setup()


def test_execute():
    """Test _execute correctly reports the result"""

    # Disable this - don't need `covalent` with fake electron functions.
    patched_import_covalent = mock.patch(
        "covalent_slurm_plugin.exec._import_covalent",
    )

    def _fake_electron_function(x, y):
        return x + y

    args = (1, 2)
    kwargs = {}
    with tempfile.NamedTemporaryFile("wb") as f:
        pickle.dump((_fake_electron_function, args, kwargs), f)
        f.flush()
        f.seek(0)

        patched_sys_argv = mock.patch.object(sys, "argv", ["", f.name])

        with patched_import_covalent, patched_sys_argv:
            result_dict = exec_script._execute()

    assert result_dict["result"] == 3
    assert result_dict["exception"] is None


def test_execute_with_error():
    """Test _execute correctly reports an error"""

    _fake_exception = ValueError("bad bad not good")

    # Disable this - don't need `covalent` with fake electron functions.
    patched_import_covalent = mock.patch(
        "covalent_slurm_plugin.exec._import_covalent",
    )

    def _fake_electron_function(x, y):
        raise _fake_exception

    args = (1, 2)
    kwargs = {}
    with tempfile.NamedTemporaryFile("wb") as f:
        pickle.dump((_fake_electron_function, args, kwargs), f)
        f.flush()
        f.seek(0)

        patched_sys_argv = mock.patch.object(sys, "argv", ["", f.name])

        with patched_import_covalent, patched_sys_argv:
            result_dict = exec_script._execute()

    assert result_dict["result"] is None
    assert isinstance(result_dict["exception"], type(_fake_exception))
    assert "bad bad not good" in str(result_dict["exception"])


def test_main():
    """Test the main function correctly reports the result"""

    _fake_result = 123456789

    # Disable this - don't need `covalent` with fake electron functions.
    patched_import_covalent = mock.patch(
        "covalent_slurm_plugin.exec._import_covalent",
    )
    patched_execute = mock.patch(
        "covalent_slurm_plugin.exec._execute",
        return_value={"result": _fake_result, "exception": None},
    )

    with tempfile.NamedTemporaryFile("wb") as f:
        patched_sys_argv = mock.patch.object(sys, "argv", ["", "fake_func_filename", f.name])

        with patched_import_covalent, patched_sys_argv, patched_execute:
            # Write the result to the temporary file.
            exec_script.main()

        f.flush()
        f.seek(0)

        with open(f.name, "rb") as f:
            result, exception = pickle.load(f)

    assert result == _fake_result
    assert exception is None


def test_main_with_error():
    """Test the main function correctly reports an error"""

    _fake_exception = SyntaxError("bad bad not good")

    # Disable this - don't need `covalent` with fake electron functions.
    patched_import_covalent = mock.patch(
        "covalent_slurm_plugin.exec._import_covalent",
    )
    patched_execute = mock.patch(
        "covalent_slurm_plugin.exec._execute",
        return_value={"result": None, "exception": _fake_exception},
    )

    with tempfile.NamedTemporaryFile("wb") as f:
        patched_sys_argv = mock.patch.object(sys, "argv", ["", "fake_func_filename", f.name])

        with patched_import_covalent, patched_sys_argv, patched_execute:
            # Write the result to the temporary file.
            exec_script.main()

        f.flush()
        f.seek(0)

        with open(f.name, "rb") as f:
            result, exception = pickle.load(f)

    assert result is None
    assert isinstance(exception, type(_fake_exception))
    assert "bad bad not good" in str(exception)
