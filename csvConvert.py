#!/usr/bin/python
# Copyright (c) 2013 William B Phelps <wm@usa.net>
#
# Started with convert_csv.py from Tom Keffer
# Modified 2013/05/18 wbphelps - initial post to github
#  
#
""" Reads a CSV file produced by wdConvert.py and attempts to add new records to the
weewx database.
This program is an example only, you are cautioned to reveiw and test thoroughly before
actually adding any data to an existing database.  The author wrote this program for his
own use and is making it available to others in the hopes that it will be a useful example
but there are absolutely no guarantees that this is in any way a working program...
Use at your own risk!
"""

import argparse
import csv
import time

import weeutil
import weewx
import weewx.archive

#fields = ['date', 'time', 'outTemp', 'outHumidity', 'dewpoint', 'windSpeed', 'windGust10', 'windDir', 'rainRate', 'dayRain',
#          'barometer', 'totalRain', 'inTemp', 'inHumidity', 'windGust', 'windchill', 'heatindex',
#          'UV', 'radiation', 'ET', 'annualET', 'apparentTemp', 'maxradiation','hoursSunshine', 'windDir2']
# csv file fields
fields = ["dateTime","usUnits","interval","barometer","pressure","altimeter","inTemp","outTemp","inHumidity","outHumidity", \
	"windSpeed","windDir","windGust","windGustDir","rainRate","rain","dewpoint","windchill","heatindex","ET","radiation","UV", \
	"extraTemp1","extraTemp2","extraTemp3","soilTemp1","soilTemp2","soilTemp3","soilTemp4","leafTemp1","leafTemp2","extraHumid1","extraHumid2", \
	"soilMoist1","soilMoist2","soilMoist3","soilMoist4","leafWet1","leafWet2","rxCheckPercent","txBatteryStatus","consBatteryVoltage", \
	"hail","hailRate","heatingTemp","heatingVoltage","supplyVoltage","referenceVoltage","windBatteryStatus","rainBatteryStatus", \
	"outTempBatteryStatus","inTempBatteryStatus", \
	"cpuLoadFactor","cpuUsagePercent","cpuWaitPercent","cpuTemperature", \
	"logErrors","logIOErrors","logOPErrors","logFTPErrors","logRFErrors" ]

# Create a command line parser:
parser = argparse.ArgumentParser()
parser.add_argument("csv_path",
                    help="Path to the CSV file to be extracted (Required)")
parser.add_argument("sql_path",
                    help="Path to the SQLITE database (Required)")
args = parser.parse_args()

# Open up the sqlite database. An exception will be thrown if it does not exist
# or is not initialized:

config_fn, config_dict = weeutil.weeutil.read_config("weewx.conf", args)
print "Using configuration file %s." % config_fn

archive_db = config_dict['StdArchive']['archive_database']
print archive_db
archive_db_dict = config_dict['Databases'][archive_db]
archive_db_dict['database'] = 'archive/weewx_new.sdb'
print archive_db_dict        
archive = weewx.archive.Archive.open(archive_db_dict)
nrecs = 0

with open(args.csv_path, 'r') as csvfile:
    # Create a CSV reader for the file. Use a comma for the field separator
    csvreader = csv.reader(csvfile, delimiter=',', quoting=csv.QUOTE_NONE)
    row = csvreader.next() # skip header
    # Now go through each row in the file, converting it to a dictionary record
    for row in csvreader:
        # This will create a dictionary with keys given by the 'fields', and with
        # values given by what was read from the file
        record = dict(zip(fields, row))
        record['usUnits'] = 1
        record['dateTime'] = int(record['dateTime'])
        print record
        print "Unit system of incoming record (0x%x)" % record['usUnits']
        
        # Add the record to the database:
        archive.addRecord(record)
        nrecs += 1
        
archive.close()
print "%d records converted" % (nrecs,)
