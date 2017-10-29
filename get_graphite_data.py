#!/usr/bin/env python2

"""
get metrics from graphite API in pickle format to be processed by graphite
wrapper for timeseries prediction.  Need to supply hostname, metric, and
number of days back to go.  Can also specify https, otherwise default of http
will be used.
API will be queried and fetch the daily average for the metric given for the
timeperiod specified.
Example usage: get_graphite_data.py --host localhost:8080 --metric
collectd.*.df-var.df_complex-percent --days 30 --https

This will query the following endpoint:
    https://localhost:8080/render?format=pickle& target=summarize(collectd.*.df-var.df_complex-percent,%221day%22,%22avg%22)&from=-30days

and the output will be saved in output/localhost-<metric>-30-<time>.pickle

Author: Amro Diab
Date: 29/09/2015

"""

import optparse
import os
import re
import sys
import time
import urllib


def parse_options():

    parser = optparse.OptionParser()
    parser.add_option("-H", "--host", type="str", default=None,
                      help="grafana host")
    parser.add_option("-m", "--metric", type="str", default=None,
                      help="metric")
    parser.add_option("-s", "--https", action="store_true",
                      help="use https")
    parser.add_option("-d", "--days", type="int",
                      help="days back to collect data", default=30)

    (options, args) = parser.parse_args()
    return options

def strip_non_alphanumeric(str):
    return re.sub('[\W_]+', '', str)

def main():
    options = parse_options()
    host = options.host
    metric = options.metric
    if not host or not metric:
        sys.exit('Error: please specify host and metric args')
    protocol = "https" if options.https else "http"
    days = options.days

    url = ("{}://{}/render?format=pickle&target=summerize({},"
           "%221day%22,%22avg%22)&from=-{}".format(protocol, host,
                                                   metric, days))
    print url

    ts = int(time.time())
    REAL_PATH = os.path.dirname(os.path.realpath(__file__))
    response = urllib.urlopen(url)
    response_code = response.getcode()
    if not response_code == 200:
        sys.exit('Unable to retrieve data.  Received error code: {}'
                .format(response_code))
    data = response.read()
    filename = '/output/{}.{}.{}.pickle'.format(host, strip_non_alphanumeric(metric),
                                         ts)
    pi = open(REAL_PATH + filename, 'w')
    pi.write(data)
    pi.close()

if __name__ == "__main__":
    main()
