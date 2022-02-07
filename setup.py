# Copyright 2021 Agnostiq Inc.
#
# This file is part of Covalent Cloud.

import site
from setuptools import find_packages, setup

site.ENABLE_USER_SITE = "--user" in sys.argv[1:]

with open("VERSION") as f:
    version = f.read().strip()

with open("requirements.txt") as f:
    required = f.read().splitlines()

plugins_list = ["custom = custom"]

setup_info = {
    # Add this to covalent's extra_requires to install as `pip install cova[custom]`
    "name": "covalent-custom-plugin", 
    "packages": find_packages("."),
    "version": version,
    "maintainer": "Agnostiq",
    "url": "https://github.com/AgnostiqHQ/covalent-custom-plugin",
    "download_url": f"https://github.com/AgnostiqHQ/covalent-custom-plugin/archive/v{version}.tar.gz",
    "license": "Proprietary",
    "author": "Agnostiq",
    "author_email": "support@agnostiq.ai",
    "description": "Covalent Custom Executor Plugin",
    "long_description": open("README.md").read()
    "long_description_content_type": "text/markdown",
    "include_package_data": True,
    "install_requires": required,
    "classifiers": [
        "Development Status :: 2 - Pre-Alpha",
        "Environment :: Console",
        "Environment :: Plugins",
        "Intended Audience :: Developers",
        "Intended Audience :: Education",
        "Intended Audience :: Science/Research",
        "License :: Other/Proprietary License",
        "Natural Language :: English",
        "Operating System :: MacOS",
        "Operating System :: POSIX :: Linux",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3 :: Only",
        "Programming Language :: Python :: 3.8",
        "Topic :: Adaptive Technologies",
        "Topic :: Scientific/Engineering",
        "Topic :: Scientific/Engineering :: Interface Engine/Protocol Translator",
        "Topic :: Software Development",
        "Topic :: System :: Distributed Computing",
    ],
    entry_points: {
        "covalent.executor.executor_plugins": plugins_list,
    },
}

if __name__ == "__main__":
    setup(**setup_info)
