# Building
## Debian and Ubuntu
These instructions refer to Debian 8.x and Ubuntu 15.10. They might work as
well with other (higher or lower) versions. Don't hesitate to request support
for your version or OS by
[opening an issue](https://github.com/krichter722/lrb-validator/issues/new).

Build instructions:

  * Make sure `python-setuptools` is installed.
  * Run `python setup.py build && sudo python setup.py install` in order to
fetch necessary dependencies in order to...
  * ...run `python bootstrap.py` which will setup a database and start a
database server which lets you run `perl validate.pl`.
