#!/bin/sh

#
# Copyright (C) 2014 - 2016 Humboldt-Universität zu Berlin
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

# Runs all necessary actions including all bootstrapping, installation of
# prerequisites

python setup.py build
sudo python setup.py install
git submodule update --init
cd template-helper && python setup.py build && sudo python setup.py install && cd ..
sudo apt-get update
# can't be installed via pip:
sudo apt-get install python-augeas
# install zero-conf `cpan` installer `cpanminus` doesn't skip configuration ->
# managed with `pexpect` in `bootstrap.py` which is more coherent anyway
sudo python bootstrap.py
python bootstrap_unprivileged.py &
echo "sleeping 10s to wait for the database server to be available"
sleep 10
# `validate.config` has been generated by `bootstrap_unprivileged.py`, but can
# be regenerated (e.g. after changes) with `generate_validate_config.py` (see
# `generate_validate_config.py --help` for usage info)
perl import.pl validate.config
perl validate.pl validate.config
validate_return_code=$?
exit validate_return_code
