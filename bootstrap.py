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

# This script install the prerequisites to run the Linear Road Benchmark (LRB)
# validator script `validate.pl`. Then it starts the PostgreSQL database
# required by the script.

import plac
import subprocess as sp
import python_essentials.lib.check_os as check_os
import python_essentials.lib.pm_utils as pm_utils
import python_essentials.lib.os_utils as os_utils
import logging
import os
import validate_globals
import pexpect
import sys
import re

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
logger_stdout_handler = logging.StreamHandler()
logger_stdout_handler.setLevel(logging.DEBUG)
logger_formatter = logging.Formatter('%(asctime)s:%(message)s')
logger_stdout_handler.setFormatter(logger_formatter)
logger.addHandler(logger_stdout_handler)

# binaries
cpan_default = "cpan"

def bootstrap(cpan=cpan_default):
    if not os_utils.which(cpan):
        raise ValueError("cpan binary '%s' doesn't exist or isn't executable" % (cpan,))
    # there's seriously no smart way to avoid cpan questions without manipulating the local configurtion (which conflicts with the idea of making it possible to run the script locally and not only on CI services)
    cpan_packages = ["DBD::PgPP", "Log::Log4perl", "DBD::Pg"]
    logger.info("installing %s with cpan '%s'" % (str(cpan_packages), cpan))
    cpan_proc = pexpect.spawn(str.join(" ", [cpan]+cpan_packages))
    cpan_proc.logfile = sys.stdout
    cpan_proc.timeout = 10000000
    expect_result = cpan_proc.expect(['\\[yes\\]',
        pexpect.EOF # if already installed
        ])
    if expect_result == 0:
        cpan_proc.sendline("yes")
        cpan_proc.expect(['\\[local::lib\\]']) # need to add surrounding [] in order to
             # avoid double match
        cpan_proc.sendline("sudo")
        cpan_proc.expect(["\\[yes\\]"])
        cpan_proc.sendline("yes")
        cpan_proc.expect(pexpect.EOF) # wait for termination
    if check_os.check_debian() or check_os.check_ubuntu():
        pm_utils.install_packages(["postgresql"])
    else:
        raise ValueError("operating system not supported")
    logger.info("You're ready to run `python bootstrap_unprivileged.py`")

def main():
    plac.call(bootstrap)

if __name__ == "__main__":
    main()
