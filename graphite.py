#!/usr/bin/env python2

# grafana/graphite wrapper for timeseries_prediction
# process pickle file and generate 
# Usage: ./graphite.py <file.pickle>
#
# Author: Amro Diab
# Date: 07/10/15

from predict import process_data

import pickle
import re
import sys
import urllib

try:
    filename = sys.argv[1]
except IndexError:
    sys.exit('Please supply filename as argument')

try:
    pi = pickle.load(open(filename, 'rb'))
except IOError:
    sys.exit("Error: Unable to open file: {}".format(filename))
except KeyError, IndexError:
    sys.exit("Error: Not a pickle file: {}".format(filename))

for ix, i in enumerate(pi):
    name=pi[ix]['name']
    m = re.match(".*\w+\.(.*)\.df-var.*", name)
    name = m.group(1).replace('_', '.') if m else name
    final_data=[]
    for value in pi[ix]['values']:
        final_data.append({name:value})
    print process_data(final_data, number=3)
