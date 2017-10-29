$!/usr/bin/env bash

# generate report

cat output/* | grep -v ERROR | grep -v complete | awk -F, {'print $1'} | \
  cut -f1 -d"." | sed -e 's/[0-9]*//g' | sort | uniq -c | sort -nk1 | tac
