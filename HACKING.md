Split in `run_once.sh` and `run.py` represents tasks which ought to be done
once with privileges and those which have to be done everytime before the
validator runs (e.g. starting the database).

There's no preference in script language as long as for contributions
as long as they don't represent a pure rewrite of working code in another
language.

Integration tests with a complete set of data on travis-ci.org are not
planned because they require too much time and possibly upload resources.

Translation are no planning because they seem unnecessary given the target
group of database researchers and have no priority.
