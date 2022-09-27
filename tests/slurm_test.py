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

"""Tests for the SSH executor plugin."""

import os
from functools import partial
from pathlib import Path
from unittest import mock

import aiofiles
import pytest
from covalent._results_manager.result import Result
from covalent._workflow.transport import TransportableObject
from covalent.executor.base import wrapper_fn

from covalent_slurm_plugin import SlurmExecutor

aiofiles.threadpool.wrap.register(mock.MagicMock)(
    lambda *args, **kwargs: aiofiles.threadpool.AsyncBufferedIOBase(*args, **kwargs)
)


@pytest.fixture
def proc_mock():
    return mock.Mock()


@pytest.fixture
def conn_mock():
    return mock.Mock()


def test_init():
    """Test that initialization properly sets member variables."""

    username = "username"
    host = "host"
    key_file = "key_file"
    remote_workdir = "/test/remote/workdir"
    slurm_path = "/opt/test/slurm/path"
    cache_dir = "/test/cache/dir"

    executor = SlurmExecutor(
        username=username,
        address=host,
        ssh_key_file=key_file,
        remote_workdir=remote_workdir,
        slurm_path=slurm_path,
        poll_freq=30,
        cache_dir=cache_dir,
        options={},
    )

    assert executor.username == username
    assert executor.address == host
    assert executor.ssh_key_file == str(Path(key_file).expanduser().resolve())
    assert executor.remote_workdir == remote_workdir
    assert executor.slurm_path == slurm_path
    assert executor.poll_freq == 30
    assert executor.cache_dir == cache_dir
    assert executor.options == {}


def test_format_submit_script():
    """Test that the script (in string form) which is to be run on the remote server is
    created with no errors."""

    executor = SlurmExecutor(
        username="test_user",
        address="test_address",
        ssh_key_file="~/.ssh/id_rsa",
        remote_workdir="/federation/test_user/.cache/covalent",
        poll_freq=30,
        cache_dir="~/.cache/covalent",
        options={},
    )

    def simple_task(x):
        return x

    transport_function = partial(
        wrapper_fn, TransportableObject(simple_task), [], [], TransportableObject(5)
    )
    python_version = ".".join(transport_function.args[0].python_version.split(".")[:2])

    dispatch_id = "259efebf-2c69-4981-a19e-ec90cdffd026"
    task_id = 3
    func_filename = f"func-{dispatch_id}-{task_id}.pkl"
    result_filename = f"result-{dispatch_id}-{task_id}.pkl"

    try:
        executor._format_submit_script(
            func_filename,
            result_filename,
            python_version,
        )
    except Exception as exc:
        assert False, f"Exception while running _format_submit_script: {exc}"


@pytest.mark.asyncio
async def test_get_status(proc_mock, conn_mock):
    """Test the get_status method."""

    executor = SlurmExecutor(
        username="test_user",
        address="test_address",
        ssh_key_file="~/.ssh/id_rsa",
        remote_workdir="/federation/test_user/.cache/covalent",
        poll_freq=30,
        cache_dir="~/.cache/covalent",
        options={},
    )

    proc_mock.returncode = 0
    proc_mock.stdout = "Fake Status"
    proc_mock.stderr = "stderr"

    conn_mock.run = mock.AsyncMock(return_value=proc_mock)

    status = await executor.get_status({}, conn_mock)
    assert status == Result.NEW_OBJ

    status = await executor.get_status({"job_id": 0}, conn_mock)
    assert status == "Fake Status"
    assert conn_mock.run.call_count == 2


@pytest.mark.asyncio
async def test_poll_slurm(proc_mock, conn_mock):
    """Test that polling the status works."""

    executor = SlurmExecutor(
        username="test_user",
        address="test_address",
        ssh_key_file="~/.ssh/id_rsa",
        remote_workdir="/federation/test_user/.cache/covalent",
        poll_freq=30,
        cache_dir="~/.cache/covalent",
        options={},
        slurm_path="sample_path",
    )

    proc_mock.returncode = 0
    proc_mock.stdout = "COMPLETED"
    proc_mock.stderr = "stderr"

    conn_mock.run = mock.AsyncMock(return_value=proc_mock)

    # Check completed status does not give any errors
    await executor._poll_slurm(0, conn_mock)
    conn_mock.run.assert_called_once()

    # Now give an "error" in the get_status method and check that the
    # correct exception is raised.
    proc_mock.returncode = 1
    proc_mock.stdout = "AN ERROR"
    conn_mock.run = mock.AsyncMock(return_value=proc_mock)

    try:
        await executor._poll_slurm(0, conn_mock)
    except RuntimeError as raised_exception:
        expected_exception = RuntimeError("Job failed with status:\n", "AN ERROR")
        assert type(raised_exception) == type(expected_exception)
        assert raised_exception.args == expected_exception.args

    conn_mock.run.assert_called_once()


@pytest.mark.asyncio
async def test_query_result(mocker, proc_mock, conn_mock):
    """Test querying results works as expected"""

    executor = SlurmExecutor(
        username="test_user",
        address="test_address",
        ssh_key_file="~/.ssh/id_rsa",
        remote_workdir="/federation/test_user/.cache/covalent",
        poll_freq=30,
        cache_dir="~/.cache/covalent",
        options={"output": "stdout_file", "error": "stderr_file"},
    )

    # First test when the remote result file is not found by mocking the return code
    # with a non-zero value.
    proc_mock.returncode = 1
    proc_mock.stdout = "stdout"
    proc_mock.stderr = "stderr"

    conn_mock.run = mock.AsyncMock(return_value=proc_mock)

    try:
        await executor._query_result(
            result_filename="mock_result", task_results_dir="", conn=conn_mock
        )
    except Exception as raised_exception:
        expected_exception = FileNotFoundError(1, "stderr")
        assert type(raised_exception) == type(expected_exception)
        assert raised_exception.args == expected_exception.args

    # Now mock result files.
    proc_mock.returncode = 0
    conn_mock.run = mock.AsyncMock(return_value=proc_mock)

    mocker.patch("asyncssh.scp", return_value=mock.AsyncMock())

    # Don't actually try to remove result files:
    async_os_remove_mock = mock.AsyncMock(return_value=None)
    mocker.patch("aiofiles.os.remove", side_effect=async_os_remove_mock)

    # Mock the opening of specific result files:
    expected_results = [1, 2, 3, 4, 5]
    expected_error = None
    expected_stdout = "output logs"
    expected_stderr = "output errors"
    pickle_mock = mocker.patch(
        "cloudpickle.loads", return_value=(expected_results, expected_error)
    )
    unpatched_open = open

    def mock_open(*args, **kwargs):
        if args[0] == "mock_result":
            return mock.mock_open(read_data=None)(*args, **kwargs)
        elif args[0] == executor.options["output"]:
            return mock.mock_open(read_data=expected_stdout)(*args, **kwargs)
        elif args[0] == executor.options["error"]:
            return mock.mock_open(read_data=expected_stderr)(*args, **kwargs)
        else:
            return unpatched_open(*args, **kwargs)

    with mock.patch("aiofiles.threadpool.sync_open", mock_open):

        result, stdout, stderr, exception = await executor._query_result(
            result_filename="mock_result", task_results_dir="", conn=conn_mock
        )

        assert result == expected_results
        assert exception == expected_error
        assert stdout == expected_stdout
        assert stderr == expected_stderr
        pickle_mock.assert_called_once()
