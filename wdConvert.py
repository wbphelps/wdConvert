# wdConvert.py

import os
import os.path
import argparse
from datetime import datetime
from datetime import timedelta
import csv
import time
import uwxutils

print "WD Convert utility"

#wd_dir = "C:\\wdisplay\\logfiles\\"
wd_dir = "logfiles\\"

parser = argparse.ArgumentParser(description='convert WD log files to csv/xls ?')
parser.add_argument('start_year', nargs='?', type=int, default='2010')
parser.add_argument('start_month', nargs='?', type=int, default='1')
parser.add_argument('end_year', nargs='?', type=int, default='2010')
parser.add_argument('end_month', nargs='?', type=int, default='13')
parser.add_argument('altitude', nargs='?', type=int, default='15')
parser.add_argument('interval', nargs='?', type=int, default='60')
parser.add_argument('count', nargs='?', type=int, default='9999999999')
args = parser.parse_args()

year = args.start_year
month = args.start_month
year_end = args.end_year
month_end = args.end_month
altitude = args.altitude
interval = args.interval

# Tuples for the fields in the 3 Davis log files
# WD nyyyylg.txt 
t_wdlg = 'day month year hour minute temperature humidity dewpoint barometer windspeed gustspeed direction rainlastmin dailyrain monthlyrain yearlyrain heatindex'.split()
#WD nyyyyvantagelg.txt
t_wdvalg = 'day month year hour minute radiation UV ET soilmoist soiltemp'.split()
# WD nyyyyindoorlg.txt
t_wdinlg = 'day month year hour minute temperature humidity'.split()

# Weewx database
t_weewx = ["dateTime","usUnits","interval","barometer","pressure","altimeter","inTemp","outTemp","inHumidity","outHumidity", \
	"windSpeed","windDir","windGust","windGustDir","rainRate","rain","dewpoint","windchill","heatindex","ET","radiation","UV", \
	"extraTemp1","extraTemp2","extraTemp3","soilTemp1","soilTemp2","soilTemp3","soilTemp4","leafTemp1","leafTemp2","extraHumid1","extraHumid2", \
	"soilMoist1","soilMoist2","soilMoist3","soilMoist4","leafWet1","leafWet2","rxCheckPercent","txBatteryStatus","consBatteryVoltage", \
	"hail","hailRate","heatingTemp","heatingVoltage","supplyVoltage","referenceVoltage","windBatteryStatus","rainBatteryStatus", \
	"outTempBatteryStatus","inTempBatteryStatus", \
	"cpuLoadFactor","cpuUsagePercent","cpuWaitPercent","cpuTemperature", \
	"logErrors","logIOErrors","logOPErrors","logFTPErrors","logRFErrors" ]
# CSV file
t_csv = ["dateTime","usUnits","interval", \
	"barometer","pressure","altimeter","inTemp","outTemp","inHumidity","outHumidity", \
	"windSpeed","windDir","windGust","windGustDir","rainRate","rain","dewpoint", \
	"windchill","heatindex","ET","radiation","UV", ]

###def get_pressures(self, time_ts, barometer, currentTempF, humidity):
def get_pressures(altitude, barometer, currentTempF, humidity):
	"""Calculate the missing pressures.
	Returns a tuple (station_pressure_inHg, altimeter_pressure_inHg)
	"""
	# Both the current SLP and temperature are needed.
	if barometer is not None and currentTempF is not None:
		# Get the temperature 12 hours ago, or if it is missing, use the current temperature
###		temp12HrsAgoF = self.get_temperature_12(time_ts)
###		if temp12HrsAgoF is None:
		temp12HrsAgoF = currentTempF
		# If humidity is missing, use 0.
		if humidity is None:
			humidity = 0
		pressureIn = uwxutils.uWxUtilsVP.SeaLevelToSensorPressure_12(barometer, altitude, currentTempF, temp12HrsAgoF, humidity)
		altimeterIn = uwxutils.TWxUtilsUS.StationToAltimeter(pressureIn, altitude)
		pressureIn = int(pressureIn*10000.0+0.5)/10000.0
		altimeterIn = int(altimeterIn*10000.0+0.5)/10000.0
		return pressureIn, altimeterIn
	else:
		return (None, None)

#windchillF from weewx wxformulas.py
def windchillF(T_F, V_mph) :
	"""Calculate wind chill. From http://www.nws.noaa.gov/om/windchill
	T_F: Temperature in Fahrenheit
	V_mph: Wind speed in mph
	Returns Wind Chill in Fahrenheit
	"""
	if T_F is None or V_mph is None:
		return None
	# Formula only valid for temperatures below 50F and wind speeds over 3.0 mph
	if T_F >= 50.0 or V_mph <= 3.0 :
		return T_F
	WcF = 35.74 + 0.6215 * T_F + (-35.75  + 0.4275 * T_F) * math.pow(V_mph, 0.16)
	return WcF


# example of myyyylg.txt
#day month year hour minute temperature humidity dewpoint barometer windspeed gustspeed direction rainlastmin dailyrain monthlyrain yearlyrain heatindex
# 1  3 2009  0  0 57.1  74 48.9 29.986   0   0 296  0.000 0.000 0.000 7.948 57.1

# example of myyyyvantagelog.txt
#day month year hour minute, Solar_radation, UV,  Daily_ET, soil_moist, soil_temp
# 1  3 2009  0  0 0.00 0.0 0.016 255.0 10.000

# example of myyyyindoorlog.txt
#day month year hour minute temperature humidity
# 1  3 2009  0  1  67.2  51

# Notes...
# the first data record in lg.txt is actually from midnight on the previous day; it will have minute=0
# at the start we will skip this record and start with minute=1. After that, the program will attempt to 
# keep the records of all 3 files in sync; it will at a minimum check all the timestamps and report an error
# if they do not match...

def dget(d, keys): # get multiple values from a dict (why isn't this built in???)
	return (d.get(k) for k in keys)

def open_fout(dtxt):
	global fout, fwriter
	#fout = open("out" + dtxt + ".csv", 'w')
	fout = open("out" + dtxt + ".csv", 'wb')
	fwriter = csv.writer(fout, delimiter=",", quotechar="'",quoting=csv.QUOTE_MINIMAL)
#	fwriter = csv.writer(fout)
	fwriter.writerow(t_csv)

def open_flg(dtxt):
	global flg, na_flg, i_flg
	na_flg = dtxt + "lg.txt"
	if os.path.isfile(wd_dir + na_flg):
		print "==> lg:   " + na_flg
		flg = open(wd_dir + na_flg, 'r')
		line = flg.readline()  # skip header
#		w_flg = flg.readline().split()  # read first data line
		i_flg = 2
	else:
		print "File not found: " + na_flg
		exit(1)

def open_fvalg(dtxt):
	global fvalg, na_fvalg, i_fvalg
	na_fvalg = dtxt + "vantagelog.txt"
	if os.path.isfile(wd_dir + na_fvalg):
		print "==> lgva: " + na_fvalg
		fvalg = open(wd_dir + na_fvalg, 'r')
		line = fvalg.readline()  # skip header
#		w_fvalg = fvalg.readline().split()  # read first data line
		i_fvalg = 2
	else:
		print "File not found: " + na_fvalg
		exit(1)

def open_finlg(dtxt):
	global finlg, na_finlg, i_finlg
	na_finlg = dtxt + "indoorlog.txt"
	if os.path.isfile(wd_dir + na_finlg):
		print "==> lgin: " + na_finlg
		finlg = open(wd_dir + na_finlg, 'r')
		line = finlg.readline()  # skip header
#		w_finlg = finlg.readline().split()  # read first data line
		i_finlg = 2
	else:
		print "File not found: " + na_finlg
		exit(1)

def get_flg():
	global flg, tell_flg, line_flg, i_flg, d_flg, day, month, year, dtxt, dt_flg, t_wdlg
	tell_flg = flg.tell()
	line_flg = flg.readline()
	i_flg += 1
	if (len(line_flg) == 0): # EOF
		print "#EOF fvalg"
		flg.close()
		day = 0
		month += 1
		if (month > 12):
			month = 1
			year = year + 1
			if (year > year_end):
				print "#DONE"
				return False
			if (year == year_end) and (month > month_end):
				print "#DONE"
				return False
		dtxt = str(month) + str(year)
		open_flg(dtxt)
		line_flg = flg.readline()
	d_flg = dict(zip(t_wdlg, line_flg.split()))
	dt_flg = datetime(*map(int,dget(d_flg,['year','month','day','hour','minute'])))
	return True

def get_fvalg():
	global fvalg, tell_fvalg, line_fvalg, i_fvalg, d_fvalg, dt_fvalg, t_wdvalg
	tell_fvalg = fvalg.tell()
	line_fvalg = fvalg.readline()
	i_fvalg += 1
	if (len(line_fvalg) == 0): # EOF
		print "#EOF fvalg"
		fvalg.close()
		open_fvalg(dtxt)
		line_fvalg = fvalg.readline()
	d_fvalg = dict(zip(t_wdvalg, line_fvalg.split()))
	dt_fvalg = datetime(*map(int,dget(d_fvalg,['year','month','day','hour','minute'])))

def get_finlg():
	global finlg, tell_finlg, line_finlg, i_finlg, d_finlg, dt_finlg, t_wdinlg
	tell_finlg = finlg.tell()
	line_finlg = finlg.readline()
	i_finlg += 1
	if (len(line_finlg) == 0): # EOF
		print "#EOF finlg"
		finlg.close()
		open_finlg(dtxt)
		line_finlg = finlg.readline()
	d_finlg = dict(zip(t_wdinlg, line_finlg.split()))
	dt_finlg = datetime(*map(int,dget(d_finlg,['year','month','day','hour','minute'])))

#day = 1
#hour = 0
#min = -1
dtxt = str(month) + str(year)
open_flg(dtxt)
open_fvalg(dtxt)
open_finlg(dtxt)
open_fout(dtxt)

i_flg = 2

pd_flg = None #save previous line
pd_fvalg = None
pd_finlg = None
pdt_flg = 0
pdt_fvalg = 0
pdt_finlg = 0

count = 0
max = args.count
dt_exp = datetime(year, month, 1, 0, 0, 0)  # initialize expected date

while (year <= year_end) and (month <= month_end) and (count < max):
	count += 1

	if (not get_flg()):
		break # exit loop
	get_fvalg()
	get_finlg()

#	d_exp = [str(day), str(month), str(year), str(hour), str(min)]

	if (dt_flg == pdt_flg): # check for duplicate (should be check for lower values?)
		print na_flg + "[" + str(i_flg) +"]: " + str(dt_flg)
		print "(da) duplicate, skipping..." 
		get_flg()
#		exit(2)
	while (dt_exp != dt_flg): # is log line what we expected?
		print na_flg + "[" + str(i_flg) +"]: " + str(dt_flg)
		print "(da) expected: " + str(dt_exp)
		d = dt_flg - dt_exp # timedelta
		ds = d.days*86400+d.seconds  # time diff in seconds (with correct sign)
#		if (ds>86400): # more than 1 day?
		if (ds>86400*3): # more than 3 days?
			print "(da) error date unexpected"
			exit(3)
		elif (ds>3600): # later by more than 1 hour?
			print "(da) *** warning *** skipping ahead by %s hours ..." % str(ds/3600)
			dt_exp = dt_flg # skip ahead in time
		elif(ds>0): # within 1 hours
			print "(da) expected line missing, using next line..."
			dt_exp = dt_flg # skip ahead in time
		elif (ds<0): # line out of sequence!
			print "(da) out of sequence, skipping..."
			get_flg() # get next line
		else:
			print "(da) error date unexpected"
			exit(3)

	if (dt_fvalg == pdt_fvalg): # check for duplicate (should be check for lower values?)
		print na_fvalg + "[" + str(i_fvalg) +"]: " + str(dt_fvalg)
		print "(va) duplicate, skipping..." 
		get_fvalg() # should only be one dupe ?
#		exit(2)
	if (dt_exp != dt_fvalg):
#		print na_fvalg + "[" + str(i_fvalg) +"]: " + str(dt_fvalg)
		print "(va) expected: " + str(dt_exp)
		while (dt_exp > dt_fvalg): # older va line?
			print na_fvalg + "[" + str(i_fvalg) +"]: " + str(dt_fvalg)
			print "(va) line mismatch, skipping..."
			get_fvalg()
		if (dt_exp != dt_fvalg):
			if (dt_exp.minute == 0): # special case for 1st line in older file
				print na_fvalg + "[" + str(i_fvalg) +"]: " + str(dt_fvalg)
				print "(va) 1st line missing, using next line as 1st..."
				fvalg.seek(tell_fvalg) # backup file 1 line
				i_fvalg -= 1
				dt_fvalg = dt_exp # pretend they match
			else:
				print na_fvalg + "[" + str(i_fvalg) +"]: " + str(dt_fvalg)
				print "(va) line missing, now what?..."
				exit(4)

	if (dt_finlg == pdt_finlg): # check for duplicate (should be check for lower values?)
		print na_finlg + "[" + str(i_finlg) +"]: " + str(dt_finlg)
		print "(in) duplicate, skipping..." 
		get_finlg()
#		exit(2)
	if (dt_exp != dt_finlg):
		print "(in) expected: " + str(dt_exp)
		if (dt_exp.minute == 0): # special case for 1st line in older file
			print na_finlg + "[" + str(i_finlg) +"]: " + str(dt_finlg)
			print "(in) 1st line missing, using next line as 1st..."
			finlg.seek(tell_finlg) # backup file 1 line
			i_finlg -= 1
			dt_finlg = dt_exp # pretend they match
		elif (dt_exp < dt_finlg):
			print na_finlg + "[" + str(i_finlg) +"]: " + str(dt_finlg)
			print "(in) line missing, using previous..."
			d_finlg = pd_finlg
			#w_finlg[3:2] = w_flg[3:2] # adjust hour, minute
			dt_finlg = pdt_finlg
			d_finlg['hour'] = dt_exp.hour
			d_finlg['minute'] = dt_exp.minute
			finlg.seek(tell_finlg) # backup file 1 line
			i_finlg -= 1
		else:
			while (dt_exp > dt_finlg): # older va line?
				print na_finlg + "[" + str(i_finlg) +"]: " + str(dt_finlg)
				print "(in) line mismatch, skipping..."
				get_finlg()
			if (dt_exp != dt_finlg):
				print "(in) line missing, now what?..."
				exit(4)
	
	pd_flg = d_flg #save previous line
	pd_fvalg = d_fvalg
	pd_finlg = d_finlg
	pdt_flg = dt_flg
	pdt_fvalg = dt_fvalg
	pdt_finlg = dt_finlg
	
	#p = get_pressures(altitude, barometer, currentTempF, humidity)
	pressureIn, altimeterIn = get_pressures(altitude, float(d_flg['barometer']), float(d_flg['temperature']), float(d_flg['humidity']))
	#dt = datetime(d.year,d.month,d.day,t.hour,t.minute,t.second)
	utcs = int(time.mktime(dt_exp.timetuple()))
	# weewx rainrate is inches/hour (or cm/hour); rain is inches (or cm) for the archive period
	# WD keeps rainlastmin, railyrain, monthlyrain & yearlyrain in the lg logfile
	rr = float(d_flg['rainlastmin'])/60.0 # rain rate if interval = 60
	wc = windchillF(d_flg['temperature'], d_flg['windspeed'])
	fwriter.writerow([utcs, 1, int(interval/60), \
		d_flg['barometer'], pressureIn, altimeterIn, d_finlg['temperature'], d_flg['temperature'], d_finlg['humidity'], d_flg['humidity'], \
		d_flg['windspeed'], d_flg['direction'], d_flg['gustspeed'], d_flg['direction'], rr, d_flg['rainlastmin'], d_flg['dewpoint'], \
		wc, d_flg['heatindex'], d_fvalg['ET'], d_fvalg['radiation'], d_fvalg['UV'], 
		])

#	dt_exp += datetime.timedelta(seconds=60) # add 1 minute
	dt_exp += timedelta(seconds=interval) # add 1 minute

flg.close()
fvalg.close()
finlg.close()
fout.close()

# Tuples for the fields in the 3 Davis log files
# WD nyyyylg.txt 
#t_wdlg = 'day month year hour minute temperature humidity dewpoint barometer windspeed gustspeed direction rainlastmin dailyrain monthlyrain yearlyrain heatindex'
#WD nyyyyvantagelg.txt
#t_wdvalg = 'day month year hour minute radiation UV ET soilmoist soiltemp'
# WD nyyyyindoorlg.txt
#t_wdinlg = 'day month year hour minute temperature humidity'

""" written database fields
"dateTime","usUnits","interval",
"barometer","pressure","altimeter","inTemp","outTemp","inHumidity","outHumidity",
"windSpeed","windDir","windGust","windGustDir","rainRate","rain","dewpoint",
"windchill","heatindex","ET","radiation","UV",
"""