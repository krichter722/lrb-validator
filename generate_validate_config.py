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

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
ch = logging.StreamHandler()
ch.setLevel(logging.INFO)
logger.addHandler(ch)

base_dir_default = os.path.join(os.path.expanduser("~"), ".lrb-validator")
log_file_name_default = "lrb-validator.log"
database_name_default = "dbxway01"
database_user_default = "linear"
database_password_default = "linear"

remove_log_value_map = {True: "yes", False: "no"}

@plac.annotations(
    base_dir=("the base directory of the LRB validator input and output data", "positional", None, str),
    remove_log=("whether the logs ought to be deleted right after the validation", "flag"),
    log_file_name=("the name of the logfile to be produced in base_dir", "option"),
    database_name=("the database name", "option"),
    database_user=("the database user", "option"),
    database_password=("the database password", "option"),
)
def main(base_dir=base_dir_default, remove_log=False, log_file_name=log_file_name_default, database_name=database_name_default, database_user=database_user_default, database_password=database_password_default):
    if not os.path.exists(base_dir):
        logger.info("creating inexisting base_dir '%s'" % (base_dir,))
        os.makedirs(base_dir)
    elif os.path.isfile(base_dir):
        raise ValueError("base_dir '%s' is an existing file, but needs to be a directory" % (base_dir,))
    t = Template(file=os.path.realpath(os.path.join(__file__, "..", "validate.config.tmpl")))
    t_file = open(os.path.realpath(os.path.join(__file__, "..", "validate.config")), "w")
    t.base_dir = base_dir
    t.keep_log = remove_log_value_map[not remove_log] #see internal implementation notes below
    t.log_file_name = log_file_name
    t.database_name = database_name
    t.database_user = database_user
    t.database_password = database_password
    t_file.write(str(t))
    t_file.flush()
    t_file.close()
# internal implementation notes:
# - due to the fact that the keeplog variable is true by default (which isn't
# useful for a flag), we wrap it in its negative meaning to get a by default
# false flag and avoid touching the perl code

if __name__ == "__main__":
    plac.call(main)
