#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Are there more hot days during the school year than there used to be? I define "hot day" as a max temperature of 90F or more. 
I define "school year" as September to June. I compare the 1960s to the previous 10 years (2013-2023). I chose Hartford CT and 
Nashua NH because they are near where my wife and I grew up. 

The data is from https://www.ncdc.noaa.gov/cdo-web/search, Dataset = Daily Summaries, Search For = Cities. Some wrinkles to be 
aware of about this data:

-- There is a limit on how much data you can retrieve at once, so you may have to break your request into convient date ranges. 
Each of the files used here was retrieved in one request, but a single request for 1960-2023 was too much.

-- When you ask for a "city" search, NOAA tries to be helpful and gives you nearby data also. So if you want data from only 
one collection station for consistency, you may have to filter the received data to throw out all but one station.

-- The data collection stations may be different in different time periods. I grew up in Norwalk CT, but could not find data 
from this town for both time periods. 

Chuck Connell, fall 2023

"""

# Libraries

#import os
#import fnmatch
import pandas as pd 
#import numpy as np
#import datetime as dt

# Include files

# Constants

DATA_FILES = ["Hartford-1960s.txt", "Hartford-Last10.txt"]
#DATA_FILES = ["Nashua-1960s.txt", "Nashua-Last10.txt"]
STATION_SUBSTRING = "HARTFORD BRADLEY"
#STATION = "NASHUA 2"
LOCAL_DATA_DIR = "/Users/chuck/Desktop/Articles/NOAA/"

HOT = 90   # a max temp of this or higher is a hot day

'''
HPD_CLOUD_DIR = "https://www.ncei.noaa.gov/data/coop-hourly-precipitation/v2/access/"  # Hourly Precipitation Data (HPD)
HPD_LOCAL_DIR = "/Users/chuck/Desktop/Articles/NOAA/HPD/"
START_DATE = "20000101"  # This is the date format for pandas query().
END_DATE = "20220101"
STATION_YEARLY_MIN = 0  # to throw out stations with very little yearly rain. Zero means don't throw out any data for this reason.
STATION_YEARLY_MAX = 9999  # to throw out stations with a lot of yearly rain, 9999 means don't throw out any data for this reason.
SKIP_COUNT = 1  # skip input files so we don't do all 2000. 1 = don't skip any.
STATION_LIST_OUTPUT = "/Users/chuck/Desktop/Articles/hpd_stations_used_list.txt"
STATION_LIST_INPUT = "/Users/chuck/Desktop/Articles/hpd_stations_used_list_1940-1950.txt"
ALL_STATIONS = True  # use every station, or a specific list ?
'''

# Initialize some variables.

schoolDF = pd.DataFrame()   # empty dataframe to hold combined data across input files

# Loop over all the input files, adding to an overall dataset

for f in DATA_FILES:
    
    data_path = LOCAL_DATA_DIR + f
    dataDF = pd.read_fwf(data_path)   # the data files are returned from NOAA as fixed-width text
    
    # Concat with overal dataset.
    
    schoolDF = pd.concat([schoolDF, dataDF])
    
    # End of loop over stations

# Data cleanup.
    
schoolDF = schoolDF.drop(columns=["TAVG"])   # the average temp column does not contain useful data

schoolDF = schoolDF.query("DATE != '--------'")    # throw out the first row that just contains a line of hyphens

schoolDF = schoolDF[schoolDF["STATION_NAME"].str.contains(STATION_SUBSTRING, regex=False)]  # just the stations we want

schoolDF["DATE"] = pd.to_datetime(schoolDF["DATE"], errors='coerce')  # put in true date format

schoolDF = schoolDF[schoolDF['TMAX'].notna()]   # throw out rows with empty temperatures
schoolDF = schoolDF[schoolDF['TMIN'].notna()]

schoolDF["TMAX"] = schoolDF["TMAX"].astype(int)   # put in true numberic type
schoolDF["TMIN"] = schoolDF["TMIN"].astype(int)
    
# Report overall stats

'''
TotalRows = len(hpdDF)
print ("\nStations used = " + str(stations_used_count) + ". Total curated data points (rows) for all stations and days = " + str(TotalRows )) 

overall_daily_mean = round(hpdDF["DlySumToday"].mean(), 4)
overall_yearly_mean = round((overall_daily_mean * 365), 1)

print ("\nAverage rain per day overall = " + str(overall_daily_mean) + " inches")
print ("\nAverage rain per year overall = " + str(overall_yearly_mean) + " inches")

TotalRainy = len(hpdDF[hpdDF['DlySumToday'] >= RAINY])
PctRainy = round(((TotalRainy / TotalRows) * 100), 1)
print ("\nFraction of days overall that are rainy = " + str(PctRainy) + "%")

TotalDry = len(hpdDF[hpdDF['DlySumToday'] <= DRY])
PctDry = round(((TotalDry / TotalRows) * 100), 1)
print ("\nFraction of days overall that are dry = " + str(PctDry) + "%")

'''
