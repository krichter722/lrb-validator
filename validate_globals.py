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

import os

base_dir_path_default = os.path.join(os.path.expanduser("~"), ".lrb-validator")
log_file_name_default = "lrb-validator.log"
database_name_default = "dbxway01"
database_host_default = "/tmp/lrb-validator/postgres.socket"
database_user_default = "linear"
database_password_default = "linear"
