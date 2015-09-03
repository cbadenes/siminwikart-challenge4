#!/bin/bash
now=$(date +"%F-%T")
./createModel.sh > model-$now.log 2>&1 &
