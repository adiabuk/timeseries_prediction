#!/bin/bash

rm output/*.csv
now=$(date +"%m_%d_%Y")
for i in `cat hostgroups`; do

./predict.py $i disk_used_percentage_root >> output/$i_${now}.csv
done

cat output/*.csv > reports/all_host_predictions_$now.csv
