# Building
## Debian and Ubuntu
These instructions refer to Debian 8.x and Ubuntu 15.10. They might work as
well with other (higher or lower) versions. Don't hesitate to request support
for your version or OS by
[opening an issue](https://github.com/krichter722/lrb-validator/issues/new).

Build instructions:

  * Make sure `git`, `python-setuptools`, `python-augeas` and `postgresql` are
installed (PostgreSQL version 9.4 is expected to be installed which is the
default for above mentioned OS). Since we're currently using non-configurable default ports, stop the started PostgreSQL server with `sudo systemctl stop postgresql.service`.
  * Check out the project with `git clone https://github.com/krichter722/lrb-validator.git && cd lrb-validator && git submodule update --init`
  * Build the template-helper submodule with `cd template-helper && python setup.py build && sudo python setup.py install` and change back to the source root with `cd ..`
  * Run `python setup.py build && sudo python setup.py install` to
fetch necessary dependencies in order to...
  * ...run `python bootstrap.py` which will setup a database and start a
database server (so keep it running!) which lets you...
  * ...generate a `validate.config` file using `python generate_validate_config.py`. Don't worry about the warning

        You don't have the C version of NameMapper installed! I'm disabling Cheetah's useStackFrames option as it is painfully slow with the Python version of NameMapper. You should get a copy of Cheetah with the compiled C version of NameMapper.
        "\nYou don't have the C version of NameMapper installed! "

  It can be ignored.
  * Run `perl validate.pl validate.config`.
  * The database server can be stopped with <kbd>Ctrl</kbd><kbd>C</kbd>.
