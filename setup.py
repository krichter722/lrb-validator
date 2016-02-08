#!/usr/bin/python
# -*- coding: utf-8 -*-

#
# Copyright (C) 2014 - 2015 Humboldt-Universität zu Berlin
# %
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

# This scripts generates a `validate.config` file from the
# `validate.config.tmpl` template using the Cheetah template engine.
# Customization parameters are read from command line (use
# `python generate_validate_config.py --help` for help) or can be passed as
# python function arguments if you intend to use this script in your own.

from setuptools import setup, find_packages
from pkg_resources import parse_version

setup(
    name = "lrb-validator",
    version_command = ("git describe --tags", "pep440-git"),
    packages = ["."],
    setup_requires = ["setuptools-version-command>=2.2"],
    install_requires = ["template-helper>=1.0a0", "python-essentials>=1.1.5.post9", "psycopg2", "pexpect", "plac", "Cheetah"]
)
