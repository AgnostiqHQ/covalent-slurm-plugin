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

import asyncio
import os
import subprocess
from functools import partial
from unittest import mock

import aiofiles
import cloudpickle as pickle
import covalent as ct
import pytest
from covalent._results_manager.result import Result
from covalent._workflow.transport import TransportableObject
from covalent.executor import SlurmExecutor
from covalent.executor.base import wrapper_fn

aiofiles.threadpool.wrap.register(mock.MagicMock)(
    lambda *args, **kwargs: aiofiles.threadpool.AsyncBufferedIOBase(*args, **kwargs)
)


class MockAsyncProcess:
    def __init__(self, returncode, stdout, stderr):
        self.returncode = returncode
        self.stdout = stdout
        self.stderr = stderr

    async def communicate(self):
        return (self.stdout, self.stderr)


def test_init():
    """Test that initialization properly sets member variables."""

    username = os.getenv("SLURM_USERNAME", "username")
    host = os.getenv("SLURM_CLUSTER_ADDR", "host")
    key_file = os.getenv(
        "SLURM_SSH_KEY_FILE", os.path.join(os.getenv("HOME", "~/"), ".ssh/id_rsa")
    )
    remote_username = os.getenv("SLURM_USERNAME", "remote_username")

    executor = ct.executor.SlurmExecutor(
        username=username,
        address=host,
        ssh_key_file=key_file,
        remote_workdir=f"/federation/{remote_username}/.cache/covalent",
        poll_freq=30,
        options={},
    )

    assert executor.username == username
    assert executor.address == host
    assert executor.ssh_key_file == key_file
    assert executor.remote_workdir == f"/federation/{remote_username}/.cache/covalent"
    assert executor.poll_freq == 30
    assert executor.options == {}


def test_format_submit_script():
    """Test that the script (in string form) which is to be run on the remote server is
    created with no errors."""

    executor = ct.executor.SlurmExecutor(
        username="test_user",
        address="test_address",
        ssh_key_file="~/.ssh/id_rsa",
        remote_workdir="/federation/test_user/.cache/covalent",
        poll_freq=30,
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
async def test_get_status(mocker):
    """Test the get_status method."""

    executor = ct.executor.SlurmExecutor(
        username="test_user",
        address="test_address",
        ssh_key_file="~/.ssh/id_rsa",
        remote_workdir="/federation/test_user/.cache/covalent",
        poll_freq=30,
        options={},
    )

    status = await executor.get_status({})
    assert status == Result.NEW_OBJ

    async_mock = mock.AsyncMock(
        return_value=MockAsyncProcess(0, "Fake Status".encode("utf-8"), None)
    )

    subproc_mock = mocker.patch(
        "asyncio.create_subprocess_shell",
        side_effect=async_mock,
    )

    status = await executor.get_status({"job_id": 0})
    assert status == "Fake Status"
    subproc_mock.assert_called_once()


@pytest.mark.asyncio
async def test_poll_slurm(mocker):
    """Test that polling the status works."""

    executor = ct.executor.SlurmExecutor(
        username="test_user",
        address="test_address",
        ssh_key_file="~/.ssh/id_rsa",
        remote_workdir="/federation/test_user/.cache/covalent",
        poll_freq=30,
        options={},
    )

    async_mock = mock.AsyncMock(
        return_value=MockAsyncProcess(0, "COMPLETED".encode("utf-8"), None)
    )

    subproc_mock = mocker.patch(
        "asyncio.create_subprocess_shell",
        side_effect=async_mock,
    )

    await executor._poll_slurm(0)
    subproc_mock.assert_called_once()

    # Now give an "error" in the get_status method and check that the
    # correct exception is raised.

    async_mock = mock.AsyncMock(return_value=MockAsyncProcess(0, "AN ERROR".encode("utf-8"), None))
    subproc_mock = mocker.patch(
        "asyncio.create_subprocess_shell",
        side_effect=async_mock,
    )

    try:
        await executor._poll_slurm(0)
    except RuntimeError as raised_exception:
        expected_exception = RuntimeError("Job failed with status:\n", "AN ERROR")
        assert type(raised_exception) == type(expected_exception)
        assert raised_exception.args == expected_exception.args
    subproc_mock.assert_called_once()


@pytest.mark.asyncio
async def test_query_result(mocker):
    """Test querying results works as expected"""

    executor = ct.executor.SlurmExecutor(
        username="test_user",
        address="test_address",
        ssh_key_file="~/.ssh/id_rsa",
        remote_workdir="/federation/test_user/.cache/covalent",
        poll_freq=30,
        options={"output": "stdout_file", "error": "stderr_file"},
    )

    # First test when the remote result file is not found by mocking the return code
    # with a non-zero value.
    async_mock = mock.AsyncMock(return_value=MockAsyncProcess(1, None, None))
    mocker.patch(
        "asyncio.create_subprocess_shell",
        side_effect=async_mock,
    )

    try:
        await executor._query_result(result_filename="mock_result", task_results_dir="")
    except Exception as raised_exception:
        expected_exception = FileNotFoundError(1, None)
        assert type(raised_exception) == type(expected_exception)
        assert raised_exception.args == expected_exception.args

    # Now mock result files.
    async_mock = mock.AsyncMock(return_value=MockAsyncProcess(0, None, None))
    mocker.patch(
        "asyncio.create_subprocess_shell",
        side_effect=async_mock,
    )

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
            result_filename="mock_result", task_results_dir=""
        )

        assert result == expected_results
        assert exception == expected_error
        assert stdout == expected_stdout
        assert stderr == expected_stderr
        pickle_mock.assert_called_once()
