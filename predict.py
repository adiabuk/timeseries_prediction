#!/usr/bin/env python2
# pylint: disable=W1401
# PYTHON_ARGCOMPLETE_OK


"""
Collects metrics from timeseries database and attempts to predict when
value will go over acceptable limits by analysing trends over a period of time
Output is in csv format showing average rate change, amount left, days left,
and a link to the graph or timeseries data.

Example usage:
    predicting which servers will run out of disk space within the next 3 days

Author: Amro Diab
Date: 15/03/12

"""

import datetime
import optparse
import os
import sys

from subprocess import Popen, PIPE
import argcomplete

dcs_to_process = []
final_dcs = []
final_hosts = 0

def usage():
    """
    Print usage information
    """

    script = sys.argv[0]
    print """
    Usage: %s dc entity\n
    example: %s all.db.lon101 disk_used_perc_root
    """ % (script, script)

def error(msg, exit_code=0):
    """ Error Format """
    sys.stderr.write('ERROR: %s\n' % msg)

    if exit_code != 0:
        sys.exit(exit_code)

def parse_options():
    """
    Parse the argruments passed to the command line
    """
    class TROptionParser(optparse.OptionParser):
        def error(self, msg):
            self.print_help(sys.stderr)
            sys.stderr.write('\n')
            error(msg, 2)

    parser = TROptionParser()
    parser.add_option(
        "-c", "--dc", type="str", default=None,
        help='Name of dc to examine')
    parser.add_option(
        "-k", "--key", type="str", default=None,
        help='ODS Key')
    parser.add_option(
        "-d", "--days", action="store_true", default=None,
        help='Examine daily change')
    parser.add_option(
        "-w", "--weeks", action="store_true", default=None,
        help='Examine weekly change')
    parser.add_option(
        "-n", "--number", type="int", default=3,
        help="number of time periods to use")
    parser.add_option(
        "-l", "--limit", type="int", default=15,
        help="limit output to hosts alarming in {x} days (default=15)")
    parser.add_option(
        "--max_value", type="int", default=100,
        help="Maximum value of graph (default=100)")
    parser.add_option(
        "--min_value", type="int", default=0,
        help="Minimum value of graph (default=0)")
    parser.add_option(
        "-r", "--reverse", action="store_true",
        default=False, help='reverse trend direction')
    parser.add_option(
        "-a", "--aggregate", action="store_true",
        default=False, help='show dc aggregate, instead of host')
    parser.add_option(
        "-q", "--quiet", action="store_false",
        default=False, help='run silently')
    parser.add_option(
        "-o", "--dir", type="str", default="/tmp",
        help="Output directory (default=/tmp)")

    argcomplete.autocomplete(parser)
    (options, args) = parser.parse_args()

    if not options.dc:
        parser.error("DC not specified")
    elif not options.key:
        parser.error('Key not specified')
    elif not options.number >= 3:
        parser.error('Number must be at least 3')
    elif not bool(options.weeks) ^ bool(options.days):
        parser.error('Either --weeks or --days must be supplied, but not both')
    if not options.dir:
        print "Using default dir: /tmp"
    return options

def iterate_dc(dc):
    """
    Iterate through a dc and get list of hosts or child dcs
    """
    global final_hosts
    global dcs_to_process
    current_hosts = 0

    for current_dc in dcs_to_process:
        cmd = './get_hosts.sh %s' % current_dc
        process = Popen(cmd, shell=True, stdin=PIPE, stdout=PIPE,
                        stderr=PIPE, close_fds=True)
        output = process.stdout.read()

        if 'known to not exist' in output:
            return 'Bad dc'
        elif output == '':
            dcs_to_process.pop(dcs_to_process.index(current_dc))
            hosts = os.popen('./get_hosts.sh %s' % current_dc).read()
            if not hosts:
                error('unable to get hosts')
            for _ in hosts:
                current_hosts += 1
            if current_hosts > 0:
                final_hosts += current_hosts
                final_dcs.append(current_dc)
            return 'Empty dc'

        else:
            # found dc - process data
            for new_dc in iter(output.splitlines()):
                dcs_to_process.append(new_dc)
            dcs_to_process.pop(dcs_to_process.index(current_dc))
            return
"""
options = parse_options()

dc = options.dc
key_input = options.key
number = options.number
limit = options.limit
aggregate = options.aggregate
reverse = options.reverse
days = options.days
weeks = options.weeks
max_value = options.max_value
min_value = options.min_value
output_dir = options.dir
"""

def calc_hours_for_days(total_days):
    """
    Calculate the tstart and tend values for given number of days
    return in a dictionary
    """
    data = {}
    for day in range(1, total_days + 1):
        tstart = day * 24
        data.update({day: tstart})
    return data

def calc_hours_for_weeks(total_weeks):
    """
    Calculate the tstart and trend values for given number of weeks
    return in a dictionary
    """

    data = {}
    for week in range(1, total_weeks + 1):
        tstart = week * 24* 7
        data.update({week: tstart})
    return data

def get_data(command):
    """
    Execute system command and fetch data
    return in a dictionary
    """

    thisday = {}
    mylist = []

    for line in os.popen(command).readlines():
         # store only hostname and value
        if len(line.split()) == 3:
            name = line.split()[0]
            mylist.append(name)
            value = line.split()[2]
            thisday.update({name: value})
    return thisday

def predict_dc(my_dc, days=0, weeks=0, number=3):
    """
    Run predictor for given dc
    """

    global alldays
    alldays = []
    global aggregate
    aggregate = False

    if days:
        dict_of_tstart_values = calc_hours_for_days(number)
        one_unit = 24

    elif weeks:
        dict_of_tstart_values = calc_hours_for_weeks(number)
        one_unit = 24 * 7
    else:
        sys.exit('no days or weeks set')

    # calculate minutes back to use in ods link
    minutes_back = one_unit * (number+1) * 60

    for key, _ in dict_of_tstart_values.iteritems():
        # get data for all given time periods

        tstart = "-%d hour" % dict_of_tstart_values[key]
        if dict_of_tstart_values[key] == one_unit:
            tend = 'now'
        else:
            tend = "-%d hour" % (dict_of_tstart_values[key] - one_unit)

        script = './get_data.sh '
        if aggregate:
            entity_line = '--entity="%s recurse=true" ' % my_dc
        else:
            entity_line = '--entity="domain(%s,recurse=true)" ' % my_dc

        key_line = '--key=%s ' % key_input
        duration_line = ('--tstart="%s" --tend="%s" --transform="average" ' %
                         (tstart, tend))


        command = "%s%s%s%s" % (script, entity_line, key_line, duration_line)

        # store each day (dictionary) as a list item
        data = get_data(command)
        if not data:
            print "No Output %s" % data

        alldays.append(get_data(command))
        return process_data(alldays, number)

def process_data(data_input, number):
    for entity, value in data_input[number-1].iteritems():
        # Find items which have an upward trend across all days
        total  = 0
        diff = []

        try:
            # number of comparisions = data points -1
            for current_period in range(number - 1):
                diff.append(float(data_input[current_period][entity]) -
                            float(data_input[current_period + 1][entity]))

                print "diff: %d - %d" % (float(data_input[current_period][entity]),
                                         float(data_input[current_period + 1][entity]))
                my_value = (float(data_input[current_period][entity]) -
                            float(data_input[current_period + 1][entity]))
                print "result: %d" % my_value

        except TypeError as err:
            error("1st Missing data for %s: %s" % (entity, err))
            print "float!!!", data_input[current_period][entity]
            print "current_perod:", current_period
            print "data_input:", data_input
            print "entity:", entity
            continue
        reverse = False    #AMRO

        try:
            if reverse:   # downward trend
                print "getting here"
                percleft = float(data_input[0][entity]) - min_value
                print ("entity: %s current value:%s min:%s"
                       %(entity, float(data_input[0][entity]), min_value))

            else:  # upward trend
                print "getting to else clause"
                max_value = 100
                percleft = max_value - float(data_input[0][entity])
            print "out of else clause " + str(number)    # NUMBER IS number of differences ==total elements -1

            for current_period in range(number - 1):
                total += diff[current_period]
            print "diff-current: " + str(diff[current_period])
            print "total " + str(total)
            print "number-1 " + str(number-1)
            print " range " + str(range(number -1))
            avginc = total / (number -1)

            if reverse:
                avginc = -avginc
            print "percleft: "  +entity+ " " +  str(percleft)
            print "avginc:" + entity+str(avginc)
            try:
                daysleft = percleft / avginc
            except ZeroDivisionError:
                daysleft = 100

        except Exception as err:
            error("Disk Full on %s: %s" % (entity, err))
            print "percleft-error:"  +entity+ str(percleft)
            print "avginc-error:" + entity+str(avginc)

            continue
        limit = 5      #AMRO
        try:
            print "getting to try block"
            if daysleft < limit and daysleft > 0:
                link = ("https://localhost:8080/?entity=%s&key=%s&period="
                        "{'minutes_back'%%3A'%s'}&submitted=1") % (entity,
                                                                   key_input,
                                                                   minutes_back)

                data = "%s,%s,%s,%s,%s" %(entity, percleft, avginc, daysleft,
                                          link)
                print data
                print "getting to bottom return"
                return data

        except Exception as err:
            error("2nd No complete data for %s: %s" % (entity,err))
            continue

def main():
    """
    Main function - called if run directly, not imported
    """
    global key

    dcs_to_process.append(dc)
    print "Processing DCs..."

    while True:
        if len(dcs_to_process) == 0:
            break
        iterate_dc(dcs_to_process)

    print "Number of Hosts: %s" % final_hosts
    print "Number of DCs: %s" % len(final_dcs)
    print
    print "Predicting subdcs...."

    date_string = datetime.datetime.now().strftime("%Y-%m-%d-%H.%M")
    filename = "%s/%s_%s_%s.csv" % (output_dir, dc, key_input, date_string)

    output_file = open(filename, "w")
    output_file.write("Entity,Amount Remaining,Average Difference,"
                      "Days Left,Link\n")
    for my_dc in final_dcs:
        output_line = str(predict_dc(my_dc))
        output_file.write(output_line + '\n')
        output_file.flush()

    output_file.close()

if __name__ == "__main__":
    main()
