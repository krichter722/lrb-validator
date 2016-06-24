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

# Runs all setup and bootstrap actions which need to be done once (except for
# the database initialization with `initdb`). After running this once, you can
# start the validator with `run.sh`.

python setup.py build
sudo python setup.py install
git submodule update --init
cd template-helper && python setup.py build && sudo python setup.py install && cd ..
sudo apt-get update
# can't be installed via pip:
if [ "$(lsb_release -c -s)" = "xenial" ]; then
    sudo apt-get install --assume-yes postgresql-9.5 postgresql-client-9.5
else
    sudo apt-get install --assume-yes postgresql-9.4 postgresql-client-9.4
fi
sudo apt-get install --assume-yes python-augeas postgresql-common python-pip python-setuptools
    # there's no postgresql-common package with version number in Ubuntu 16.04 @TODO: check others
# `import pexpect` fails on travis because `ptyprocess` can't be found; until it's clarified that this isn't a travis-only issue keep
# statement for manual install here
sudo pip install ptyprocess
# install zero-conf `cpan` installer `cpanminus` doesn't skip configuration ->
# managed with `pexpect` in `bootstrap.py` which is more coherent anyway
sudo python bootstrap.py
exit 0
