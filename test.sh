#! /usr/bin/bash

cqlsh -f test/assets/setup.cql
python -m unittest test
