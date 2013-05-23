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

import weedb
import weewx
#import weewx.archive
import weeutil.weeutil
import syslog

syslog.setlogmask(syslog.LOG_UPTO(syslog.LOG_NOTICE)) # prevent flooding syslog

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
parser.add_argument("csv_path", help="Path to the CSV file to be extracted (Required)")
parser.add_argument("sql_path", help="Path to the SQLITE database (Required)")
args = parser.parse_args()

# Open up the sqlite database. An exception will be thrown if it does not exist
# or is not initialized:

config_fn, config_dict = weeutil.weeutil.read_config("weewx.conf", args)
print "Using configuration file %s." % config_fn
   
def getTypes():
	global _connect, _table
	"""Returns the types appearing in an archive database.
	Raises exception of type weedb.OperationalError if the 
	database has not been initialized."""
	# Get the columns in the table
	column_list = _connect.columnsOf(_table)
	return column_list

def addRecord(record, log_level=syslog.LOG_NOTICE):
	global _connect, _std_unit_system, _sqlkeys
	"""Commit a single record or a collection of records to the archive.
	record_obj: Either a data record, or an iterable that can return data
	records. Each data record must look like a dictionary, where the keys
	are the SQL types and the values are the values to be stored in the
	database."""
     
	# Determine if record_obj is just a single dictionary instance (in which
	# case it will have method 'keys'). If so, wrap it in something iterable
	# (a list):
##	record_list = [record_obj] if hasattr(record_obj, 'keys') else record_obj
	with weedb.Transaction(_connect) as cursor:
##		for record in record_list:
			if record['dateTime'] is None:
				syslog.syslog(syslog.LOG_ERR, "Archive: archive record with null time encountered.")
				raise weewx.ViolatedPrecondition("Archive record with null time encountered.")
			
#			# Check to make sure the incoming record is in the same unit system as the
#			# records already in the database:
#			if _std_unit_system:
#				if record['usUnits'] != _std_unit_system:
#					raise ValueError("Unit system of incoming record (0x%x) "\
#						"differs from the archive database (0x%x)" % (record['usUnits'], self.std_unit_system))
#			else:
#				# This is the first record. Remember the unit system to check
#				# against subsequent records:
#				_std_unit_system = record['usUnits']
			
			# Only data types that appear in the database schema can be inserted.
			# To find them, form the intersection between the set of all record
			# keys and the set of all sql keys
			record_key_set = set(record.keys())
			insert_key_set = record_key_set.intersection(_sqlkeys)
			# Convert to an ordered list:
			key_list = list(insert_key_set)
			# Get the values in the same order:
			value_list = [record[k] for k in key_list]
			
			# This will create a string of sql types, separated by commas. Because some of the weewx
			# sql keys (notably 'interval') are reserved words in MySQL, put them in backquotes.
			k_str = ','.join(["`%s`" % k for k in key_list])
			# This will be a string with the correct number of placeholder question marks:
			q_str = ','.join('?' * len(key_list))
			# Form the SQL insert statement:
			sql_insert_stmt = "INSERT INTO %s (%s) VALUES (%s)" % (_table, k_str, q_str) 
##			print sql_insert_stmt
##			print value_list
			try:
				cursor.execute(sql_insert_stmt, value_list)
##				syslog.syslog(log_level, "Archive: added %s record %s" % (self.table, weeutil.weeutil.timestamp_to_string(record['dateTime'])))
			except Exception, e:
				syslog.syslog(syslog.LOG_ERR, "Archive: unable to add archive record %s" % weeutil.weeutil.timestamp_to_string(record['dateTime']))
				syslog.syslog(syslog.LOG_ERR, " ****    Reason: %s" % e)								# end try								# end try
			# end for
		# end for

archive_db = config_dict['StdArchive']['archive_database']
print archive_db
archive_db_dict = config_dict['Databases'][archive_db]
#archive_db_dict['database'] = 'archive/weewx_new.sdb'
archive_db_dict['database'] = args.sql_path
print archive_db_dict        
#archive = weewx.archive.Archive.open(archive_db_dict)
_connect = weedb.connect(archive_db_dict)
#_std_unit_system = None
_table = 'archive'
_sqlkeys = getTypes()
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
		print str(nrecs) + ": " +  str(record['dateTime'])
#		print record
#		print "Unit system of incoming record (0x%x)" % record['usUnits']
		
		# Add the record to the database:
##        archive.addRecord(record,log_level=syslog.LOG_INFO)
		addRecord(record,log_level=syslog.LOG_INFO)
		nrecs += 1
        
#archive.close()
_connect.close()
print "%d records converted" % (nrecs,)
