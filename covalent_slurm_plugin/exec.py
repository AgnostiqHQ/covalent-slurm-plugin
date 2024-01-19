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

"""This script executes the electron function on the Slurm cluster."""

import os
import sys

import cloudpickle as pickle


def _import_covalent() -> None:
    # Wrapped import for convenience in testing.
    import covalent


def _check_setup() -> None:
    """Use these checks to create more informative error messages."""

    import filelock

    msg = ""
    exception = None

    try:
        # covalent is needed because the @electron function
        # executes inside `wrapper_fn` to apply deps
        _import_covalent()

    except ImportError as _exception:
        msg = "The covalent SDK is not installed in the Slurm job environment."
        exception = _exception

    except filelock._error.Timeout as _exception:
        config_location = os.getenv("COVALENT_CONFIG_DIR", "~/.config/covalent")
        config_location = os.path.expanduser(config_location)
        config_file = os.path.join(config_location, "covalent.conf")

        msg = "\n".join(
            [
                f"Failed to acquire file lock '{config_file}.lock' on Slurm cluster filesystem. "
                f"Consider overriding the current config location ('{config_location}'), e.g:",
                '    SlurmExecutor(..., variables={"COVALENT_CONFIG_DIR": "/tmp"})' "",
            ]
        )
        exception = _exception

    # Raise the exception if one was caught
    if exception:
        raise RuntimeError(msg) from exception


def _execute() -> dict:
    """Load and execute the @electron function"""

    func_filename = sys.argv[1]

    with open(func_filename, "rb") as f:
        function, args, kwargs = pickle.load(f)

    result = None
    exception = None

    try:
        result = function(*args, **kwargs)
    except Exception as ex:
        exception = ex

    return {
        "result": result,
        "exception": exception,
    }


def main():
    """Execute the @electron function on the Slurm cluster."""

    output_data = {
        "result": None,
        "exception": None,
        "result_filename": sys.argv[2],
    }
    try:
        _check_setup()
        output_data.update(**_execute())

    except Exception as ex:
        output_data.update(exception=ex)

    finally:
        _record_output(**output_data)


def _record_output(result, exception, result_filename) -> None:
    """Record the output of the @electron function"""

    with open(result_filename, "wb") as f:
        pickle.dump((result, exception), f)


if __name__ == "__main__":
    main()
