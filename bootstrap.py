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
import logging
import os
import psycopg2
import threading
import time
import validate_globals
import pexpect

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
logger_stdout_handler = logging.StreamHandler()
logger_stdout_handler.setLevel(logging.DEBUG)
logger_formatter = logging.Formatter('%(asctime)s:%(message)s')
logger_stdout_handler.setFormatter(logger_formatter)
logger.addHandler(logger_stdout_handler)

# binaries
cpan_default = "cpan"
postgres_version = (9,4)
initdb_default = "/usr/lib/postgresql/9.4/bin/initdb"
postgres_default = "/usr/lib/postgresql/9.4/bin/postgres"
psql_default = "/usr/lib/postgresql/9.4/bin/psql"
createdb_default = "/usr/lib/postgresql/9.4/bin/createdb"

def bootstrap(cpan=cpan_default, initdb=initdb_default, postgres=postgres_default, createdb=createdb_default, base_dir_path=validate_globals.base_dir_path_default, db_name=validate_globals.database_name_default, db_user=validate_globals.database_user_default, db_pass=validate_globals.database_password_default, shutdown_server=False):
    sp.check_call([cpan, "DBD::PgPP", "Log::Log4perl"])
    # request installation of postgresql and python-augeas outside the script
    # in README.md in order to avoid the need for privileges for the script
    #if check_os.check_debian() or check_os.check_ubuntu():
    #    pm_utils.install_packages(["postgresql", "python-augeas"])
    #else:
    #    raise ValueError("operating system not supported")
    db_dir_path = os.path.join(base_dir_path, "database")
    if not os.path.exists(db_dir_path):
        logger.debug("creating PostgreSQL database in '%s'" % (db_dir_path,))
        sp.check_call([initdb, db_dir_path])
    else:
        logger.debug("skipping creation of existing database '%s'" % (db_dir_path,))
    db_server_proc = None
    class DBThread(threading.Thread):
        def __init__(self):
            super(DBThread, self).__init__()
            self.running = True
        def run(self):
            logger.debug("starting PostgreSQL server")
            db_server_proc = sp.Popen([postgres, "-D", db_dir_path, "-k", "/tmp"])
            while self.running and db_server_proc.poll() == None:
                time.sleep(1)
            logger.debug("server shutdown requested")
            if db_server_proc.poll() == None:
                db_server_proc.terminate()
    db_thread = DBThread()
    db_thread.start()
    try_time = 0
    try_time_max = 10 # time in seconds in which we try to connect to the server
    try_interval = .5
    # wait until the server is up (don't connect as db_user, because initially
    # only the postgres user is available (and linear shouldn't be the
    # maintenance user)
    while try_time < try_time_max:
        try:
            conn = psycopg2.connect(dbname="postgres", host='/tmp', async=False)
            logger.debug("database connection established successfully")
            break
        except:
            logger.debug("waiting another %d s for the server to come up" % (try_time_max-try_time,))
            try_time += try_interval
            time.sleep(try_interval)
    cur = conn.cursor()
    logger.debug("creating user %s" % (db_user,))
    cur.execute("""create user %s with encrypted password '%s' login""" % (db_user, db_pass))
    conn.commit()
    logger.debug("creating database %s" % (db_name,))
    createdb_proc = pexpect.spawn(str.join(" ", [createdb, "--owner=%s" % (db_user,), "-W", "--host=/tmp/", "--port=5432", db_name]))
    createdb_proc.expect(['Password:', "Passwort:"])
    createdb_proc.sendline(db_pass)
    createdb_proc.expect(pexpect.EOF) # wait for termination
    #conn = psycopg2.connect(dbname="postgres", host='/tmp', async=False)
    #cur = conn.cursor()
    logger.debug("granting permissions")
    cur.execute("""grant all on database "%s" to %s""" % (db_name, db_user))
    #cur.close()
    #conn.close()
    conn.commit()
    success_message = "Setup successful :)"
    logger.info("""
%s
%s
%s""" % ("*"*(len(success_message)+4), "* %s *" % success_message, "*"*(len(success_message)+4)))
    logger.info("database is still running and can (and should) be used for LRB validator's validate.pl")
    if shutdown_server and db_thread != None:
        db_thread.running = False
        logger.debug("waiting for server to terminate")
        db_thread.join()

def main():
    plac.call(bootstrap)

if __name__ == "__main__":
    main()
