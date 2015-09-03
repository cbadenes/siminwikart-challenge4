#!/bin/bash
now=$(date +"%F-%T")
./analyze.sh > analyze-$now.log 2>&1 &
