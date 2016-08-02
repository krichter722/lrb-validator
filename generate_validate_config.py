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

import os
import plac
from Cheetah.Template import Template
import logging
import template_helper
import validate_globals

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
ch = logging.StreamHandler()
ch.setLevel(logging.INFO)
logger.addHandler(ch)

remove_log_value_map = {True: "yes", False: "no"}
use_template_helper = False
    # a flag for the usage of the not too intuitive, but destruction-preventing
    # template-helper module (opens a GUI or command line difftool in case the
    # template output doesn't match with the already existing file which allows
    # keeping track of changes which have been made to the output file and would
    # otherwise be overwritten)

@plac.annotations(
    base_dir_path=("the base directory of the LRB validator input and output data", "positional", None, str),
    remove_log=("whether the logs ought to be deleted right after the validation", "flag"),
    log_file_name=("the name of the logfile to be produced in base_dir_path", "option"),
    database_name=("the database name", "option"),
    database_host=("the database host specification (TCP address or path to local socket)", "option"),
    database_user=("the database user", "option"),
    database_password=("the database password", "option"),
    validate_config_file_path=("The path of the output file", "option"),
)
def generate_validate_config(base_dir_path=validate_globals.base_dir_path_default,
    remove_log=False,
    log_file_name=validate_globals.log_file_name_default,
    database_name=validate_globals.database_name_default,
    database_host=validate_globals.database_host_default,
    database_user=validate_globals.database_user_default,
    database_password=validate_globals.database_password_default,
    validate_config_file_path=validate_globals.validate_config_file_path_default):
    if not os.path.exists(base_dir_path):
        logger.info("creating inexisting base directory '%s'" % (base_dir_path,))
        os.makedirs(base_dir_path)
    elif os.path.isfile(base_dir_path):
        raise ValueError("base directory '%s' is an existing file, but needs to be a directory" % (base_dir_path,))
    t = Template(file=os.path.realpath(os.path.join(__file__, "..", "validate.config.tmpl")))
    t_file_path = validate_config_file_path
    t.base_dir_path = base_dir_path
    t.keep_log = remove_log_value_map[not remove_log] #see internal implementation notes below
    t.log_file_name = log_file_name
    t.database_name = database_name
    t.database_host = database_host
    t.database_user = database_user
    t.database_password = database_password
    if use_template_helper is True:
        template_helper.write_template_file(str(t), t_file_path)
    else:
        t_file = open(t_file_path, "w")
        t_file.write(str(t))
        t_file.flush()
        t_file.close()
# internal implementation notes:
# - due to the fact that the keeplog variable is true by default (which isn't
# useful for a flag), we wrap it in its negative meaning to get a by default
# false flag and avoid touching the perl code

if __name__ == "__main__":
    plac.call(generate_validate_config)
