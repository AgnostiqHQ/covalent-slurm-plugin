# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

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
