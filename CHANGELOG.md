# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [UNRELEASED]

## Changed

- Make `ssh_key_file` optional.
- Updates **init** signature kwargs replaced with parent for better documentation.

### Docs

- Add missing `,` to README.

## [0.16.0] - 2023-05-12

### Added

- A new config variable, `bashrc_path`, which is the path to the bashrc script to source.

### Changed

- Removed automatic sourcing of `$HOME/.bashrc` from the SLURM submit script.

### Fixed

- Does not put conda-related lines in SLURM script if `conda_env` is set to `False` or `""`.
- Changed default config value of `conda_env` from `None` to `""`.
- A proper `ValueError` will now be raised if `ssh_key_file` is not supplied.

## [0.15.0] - 2023-05-12

### Added

- A new kwarg, `use_srun`, that allows for the user to specify whether to use `srun` when running the pickled Python function.
- Added docstring for `sshproxy`

## [0.14.0] - 2023-05-12

### Added

- A new kwarg `create_unique_workdir` that will create unique subfolders of the type `<DISPATCH ID>/node_<NODE ID>` within `remote_workdir` if set to `True`

### Fixed

- Fixed a bug where `cleanup = False` would be ignored.
- Fixed a bug where if `cache_dir` was not present, Covalent would crash.

## [0.13.0] - 2023-05-11

### Changed

- Updated pre-commit hooks

## [0.12.1] - 2023-05-05

### Fixed

- Moved executor validations out of constructor

### Operations

- Fixed license CI workflow

## [0.12.0] - 2023-05-05

### Added

- Basic support for NERSC's sshproxy tool which uses MFA to generate SSH keys

## [0.11.0] - 2023-05-02

### Added

- Added instructions to the `README` for the remote machine's dependencies.

### Changed

- Automatically apply the `"parsable": ""` option by default if not set by the user.

## [0.10.0] - 2023-05-01

### Added

- Modified executor to use `srun` in slurm script, instead of injecting python code and calling python directly.
- Added new parameters to `SlurmExecutor` to allow finer control of jobs via options for `srun` and in-script commands (see README.md).
- Added `srun_append` parameter allowing insertion of intermediate command (see README.md).
- Added `prerun_commands` and `postrun_commands` parameters allowing execution of in-script shell commands before and after the workflow submission via `srun` (see README.md).

## [0.9.0] - 2023-04-30

### Added

- Added a new kwarg, `cert_file`, to `SlurmExecutor` that allows for a certificate file to be passed.

### Changed

- Changed the `_client_connect` function to output the connection object only since the first positional argument cannot get used.

### Operations

- Added Alejandro to paul blart group

## [0.8.0] - 2022-11-19

### Changed

- Changed BaseAsyncExecutor to AsyncBaseExecutor
- Updated covalent version to >=0.202.0,<1

### Operations

- Added license workflow

### Tests

- Enabled Codecov

## [0.7.0] - 2022-08-23

### Added

- `SlurmExecutor` can now be import directly from `covalent_slurm_plugin`
- Added several debug log statements to track progress when debugging
- `asyncssh` added as a requirement
- Added support for performing cleanup on remote machine (default is True) once execution completes
- Added `slurm_path` for users to provide a path for slurm commands if they aren't detected automatically

### Changed

- Default values set for some `SlurmExecutor` initialization parameters
- Since there were several ssh calls, thus now using `asyncssh` module for a uniform interface to run ssh commands on remote machine
- File transfer to and from is now done using `scp` instead of `rsync`

### Fixed

- Fixed returning only `result` from `run` method instead of returning `stdout` and `stderr` as well, which are now printed directly appropriately

### Tests

- Updated tests to reflect above changes

## [0.6.0] - 2022-08-18

### Changed

- Updated `covalent` version to `stable`

## [0.5.2] - 2022-08-18

### Fixed

- Restore `cache_dir` parameter to constructor

## [0.5.1] - 2022-08-14

### Fixed

- Banner file extension

## [0.5.0] - 2022-08-14

### Changed

- Updated readme banner

### Fixed

- Fixed test references to conda

## [0.4.0] - 2022-08-04

### Changed

- Slurm executor is now async aware. Internal subprocess calls are now awaited.
- Tests have been updated to reflect above changes.

## [0.3.0] - 2022-05-26

### Changed

- New logo to reflect revamp in UI.
- Reverted some changes in slurm.py.

### Fixed

- Handle exceptions correctly

## [0.2.5] - 2022-05-26

### Fixed

- Workflows are fixed

## [0.2.4] - 2022-04-28

### Added

- Unit tests written and added to the .github workflows.

## [0.2.3] - 2022-04-18

### Changed

- The function is deserialized before sending to the remote machine. This allows the remote machine to execute the fuction in a "vanilla" python, and not need Covalent to be installed.
- The args and kwargs inputs to the function to be executed are pickled into the same file as the function, for transport to the remote machine.

## [0.2.2] - 2022-04-14

### Fixed

- Fixed full local path to where result files were being copied back from remote
- Pass in dictionary to `self.get_status` instead of `str`

## [0.2.1] - 2022-04-14

### Changed

- The python version on the remote machine only has to be equal to the python version which created the function to be executed down to the minor version. Eg, matching 3.8, instead of matching 3.8.13.

## [0.2.0] - 2022-04-12

### Changed

- Modified slurm.py to be compatible with the refactored Covalent codebase.

## [0.1.2] - 2022-04-12

### Changed

- Add time module import back to `slurm.py`

## [0.1.1] - 2022-04-12

### Changed

- Updated how slurm job id is retrieved from `proc.stdout` using regex

## [0.1.0] - 2022-04-08

### Changed

- Changed global variable executor_plugin_name -> EXECUTOR_PLUGIN_NAME in executors to conform with PEP8.

## [0.0.2] - 2022-03-02

### Added

- Enabled PyPI upload

## [0.0.1] - 2022-03-02

### Added

- Core files for this repo.
- CHANGELOG.md to track changes (this file).
- Semantic versioning in VERSION.
- CI pipeline job to enforce versioning.
