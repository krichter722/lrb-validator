# Building
## Debian and Ubuntu
These instructions refer to Debian 8.x and Ubuntu 15.10. They might work as
well with other (higher or lower) versions. Don't hesitate to request support
for your version or OS by
[opening an issue](https://github.com/krichter722/lrb-validator/issues/new).

Build and run instructions:

  * Make sure `git` and `python-setuptools` are
installed (PostgreSQL version 9.4 is expected to be installed which is the
default for above mentioned OS).
  * Check out the project with its submodule with
        git clone --recursive https://github.com/krichter722/lrb-validator.git
    and change into the source root with `cd lrb-validator`.
  * run
        ./run_once.sh
    in order to setup and install prerequisites and then
        ./run.sh
    which will perform necessary setup of databases and tables, start the
    database server necessary for running `lrb-validator`, perform the
    validation itself and shutdown the database cleanly when finished

After you've run `./run_once.sh` once, you don't need to run it anymore before
running `./run.sh`.
