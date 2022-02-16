# ku-crm-bb
A simple babashka script used to manage students information.
It provides the following functionalities:

1. batch create students
2. batch update students

## Requirement
You need to install the [babashka](https://github.com/babashka/babashka)

## Setup
1. Config the the file `config.edn`, which is the config pointing to the data warehouse.
2. There should be an `ops_student` table and a `ops_student_serial` sequence in the data warehouse.

The typical SQL command to create the sequence is:
```
CREATE SEQUENCE ops_student_serial start XXX;
```

## Command

create new students by the src-table `pre_ops_student_insert`
```
$ ./crud.clj --create --src-table pre_ops_student_insert
```
Note:
We can also see the intermediate output by
```
./crud.clj --create --src-table pre_ops_student_insert -d 
```

The `-d` denotes debug.

## License

Copyright &copy; 2022 Laurence Chen

Licensed under the term of the Mozilla Public License 2.0, see LICENSE.
