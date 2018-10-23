#!/usr/bin/env bash

dax_name=$(python daxgen.py -o dax_outputs -f $@)
./plan.sh ${dax_name}
