#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Does a rainy day (or sequence of rainy days) predict that it is more likely to rain the next day? 
Or maybe it is less likely because the clouds have "used up" their rain?

In other words, does rain predict rain? 

Chuck Connell, summer 2023

General description of daily rainfall from https://weatherins.com/rain-guidelines/
1/10 (0.10) of an inch of rain – A light rain for 30-45 minutes, moderate rain for 10 minutes or heavy rain for 5 minutes. Small puddles would form but usually disappear after a short while.
1/4 (0.25) of an inch of rain – A light rain for 2-3 hours, moderate rain for 30-60 minutes or heavy rain for 15 minutes. There would be many puddles on the ground and they would not disappear easily.
1/2 (0.5) of an inch of rain – A light rain never reaches this amount, moderate rain for 1-2 hours or heavy rain for 30-45 minutes. There would be deep standing water and they would last for long periods of time.
3/4 (0.75) of an inch of rain – A light moderate rain never reaches this amount, heavy rain lasting for 2-4 hours. There would be deep standing water for long periods of time.
One (1.00) inch of rain – A light moderate rain never reaches this amount, heavy rain for several hours (2-5 hours). There would be deep standing water for long periods of time.
"""

# TODO Use len not shape

# Libraries

#import os
#import fnmatch
import pandas as pd 
#import datetime as dt

# Include files

from rain_helpers import STATION_FILES, STATION_FILESx, STATION_FILESxx

# Constants

HPD_CLOUD_DIR = "https://www.ncei.noaa.gov/data/coop-hourly-precipitation/v2/access/"  # Hourly Precipitation Data (HPD)
HPD_LOCAL_DIR = "/Users/chuck/Desktop/Articles/NOAA/"
DRY = 0.05  # less than this is considered "a dry day"
RAINY = 0.5     # more than this is considered "a rainy day"
START_DATE = "20000101"  # This is the format needed by pandas query().
END_DATE = "20500101"  

# Tell user key settings.

print ("\nTaking data after " + str(pd.to_datetime(START_DATE)) + " and before " + str(pd.to_datetime(END_DATE)) + ".")
print ("\nUsing daily rainfall >= " + str(RAINY) + " inches as a 'rainy' day, and <= " + str(DRY) + " inches as a 'dry' day.")

# Make an empty dataframe to hold combined data across stations.

hpdDF = pd.DataFrame()

# Loop over all the stations, processing and enhancing that data, then add it to an overall dataset

for f in STATION_FILESxx:
    
    # Get data for one station and report to user.
    
    station_url = HPD_LOCAL_DIR + f   # Can read from local or NOAA cloud, just by changing the DIR
    stationDF = pd.read_csv(station_url, sep=',', header='infer', dtype=str)
    station_raw_rows = len(stationDF)
    print ("\nWorking on Station ID " + stationDF["STATION"].iloc[1] + " at location " + stationDF["NAME"].iloc[1] + "." )

    # Some data cleanup.

    stationDF = stationDF.query("DlySumQF != 'P'")    # throw out dates with a bad data quality flag
    stationDF = stationDF[["STATION","NAME","DATE","DlySum"]]    # keep only fields we need
    stationDF = stationDF.rename({"DlySum":"DlySumToday"}, axis='columns')  # to distinquish from other days we will join in
    stationDF["DlySumToday"] = stationDF["DlySumToday"].astype(int) / 100   # convert totals from hundreths to inches
    stationDF["DATE"] = pd.to_datetime(stationDF["DATE"], errors='coerce')  # put in true date format
    stationDF = stationDF.query("DATE >= " + START_DATE)  
    stationDF = stationDF.query("DATE <= " + END_DATE) 
    stationDF = stationDF.query("DlySumToday >= 0")    # throw out dates with negative rainfall (yes there are some).

    # Grab a snapshot for a self-join later. Adjust fields names to avoid confusion after the join.

    stationCopyDF = stationDF
    stationCopyDF = stationCopyDF[["STATION","DATE","DlySumToday"]]  # keep just what we need
    stationCopyDF = stationCopyDF.rename({"DlySumToday":"DlySumOther", "DATE":"DATEother"}, axis='columns')  

    # Add in some other dates, for which we will pull in rainfall.

    stationDF["DATE_minus6"] = stationDF["DATE"] - pd.offsets.Day(6)
    stationDF["DATE_minus5"] = stationDF["DATE"] - pd.offsets.Day(5)
    stationDF["DATE_minus4"] = stationDF["DATE"] - pd.offsets.Day(4)
    stationDF["DATE_minus3"] = stationDF["DATE"] - pd.offsets.Day(3)
    stationDF["DATE_minus2"] = stationDF["DATE"] - pd.offsets.Day(2)
    stationDF["DATE_minus1"] = stationDF["DATE"] - pd.offsets.Day(1)
    stationDF["DATE_plus1"] = stationDF["DATE"] + pd.offsets.Day(1)

    # Join other rainfall onto base record. Adjust column names to make clear what we did.
    stationDF = stationDF.merge(stationCopyDF, how='inner', left_on=["STATION","DATE_minus6"], right_on = ["STATION","DATEother"])
    stationDF = stationDF.rename({"DlySumOther":"DlySum6DaysAgo"}, axis='columns')  
    stationDF = stationDF.drop(columns=["DATEother"])

    stationDF = stationDF.merge(stationCopyDF, how='inner', left_on=["STATION","DATE_minus5"], right_on = ["STATION","DATEother"])
    stationDF = stationDF.rename({"DlySumOther":"DlySum5DaysAgo"}, axis='columns')  
    stationDF = stationDF.drop(columns=["DATEother"])

    stationDF = stationDF.merge(stationCopyDF, how='inner', left_on=["STATION","DATE_minus4"], right_on = ["STATION","DATEother"])
    stationDF = stationDF.rename({"DlySumOther":"DlySum4DaysAgo"}, axis='columns')  
    stationDF = stationDF.drop(columns=["DATEother"])

    stationDF = stationDF.merge(stationCopyDF, how='inner', left_on=["STATION","DATE_minus3"], right_on = ["STATION","DATEother"])
    stationDF = stationDF.rename({"DlySumOther":"DlySum3DaysAgo"}, axis='columns')  
    stationDF = stationDF.drop(columns=["DATEother"])

    stationDF = stationDF.merge(stationCopyDF, how='inner', left_on=["STATION","DATE_minus2"], right_on = ["STATION","DATEother"])
    stationDF = stationDF.rename({"DlySumOther":"DlySum2DaysAgo"}, axis='columns')  
    stationDF = stationDF.drop(columns=["DATEother"])

    stationDF = stationDF.merge(stationCopyDF, how='inner', left_on=["STATION","DATE_minus1"], right_on = ["STATION","DATEother"])
    stationDF = stationDF.rename({"DlySumOther":"DlySum1DayAgo"}, axis='columns')  
    stationDF = stationDF.drop(columns=["DATEother"])

    stationDF = stationDF.merge(stationCopyDF, how='inner', left_on=["STATION","DATE_plus1"], right_on = ["STATION","DATEother"])
    stationDF = stationDF.rename({"DlySumOther":"DlySumTomorrow"}, axis='columns')  
    stationDF = stationDF.drop(columns=["DATEother"])

    # Create a column that shows (for each day) how many days it has been rainy. 
    # 1 = just today; 2 = today and yesterday; etc.

    stationDF["DaysOfRain"] = 0   
    stationDF.loc[(stationDF["DlySumToday"] >= RAINY), "DaysOfRain"] = 1
    stationDF.loc[(stationDF['DlySumToday'] >= RAINY) & (stationDF['DlySum1DayAgo'] >= RAINY), 'DaysOfRain'] = 2
    stationDF.loc[(stationDF['DlySumToday'] >= RAINY) & (stationDF['DlySum1DayAgo'] >= RAINY) & (stationDF['DlySum2DaysAgo'] >= RAINY), 'DaysOfRain'] = 3
    stationDF.loc[(stationDF['DlySumToday'] >= RAINY) & (stationDF['DlySum1DayAgo'] >= RAINY) & (stationDF['DlySum2DaysAgo'] >= RAINY) & (stationDF['DlySum3DaysAgo'] >= RAINY), 'DaysOfRain'] = 4
    stationDF.loc[(stationDF['DlySumToday'] >= RAINY) & (stationDF['DlySum1DayAgo'] >= RAINY) & (stationDF['DlySum2DaysAgo'] >= RAINY) & (stationDF['DlySum3DaysAgo'] >= RAINY) & (stationDF['DlySum4DaysAgo'] >= RAINY), 'DaysOfRain'] = 5
    stationDF.loc[(stationDF['DlySumToday'] >= RAINY) & (stationDF['DlySum1DayAgo'] >= RAINY) & (stationDF['DlySum2DaysAgo'] >= RAINY) & (stationDF['DlySum3DaysAgo'] >= RAINY) & (stationDF['DlySum4DaysAgo'] >= RAINY)  & (stationDF['DlySum5DaysAgo'] >= RAINY),'DaysOfRain'] = 6
    stationDF.loc[(stationDF['DlySumToday'] >= RAINY) & (stationDF['DlySum1DayAgo'] >= RAINY) & (stationDF['DlySum2DaysAgo'] >= RAINY) & (stationDF['DlySum3DaysAgo'] >= RAINY) & (stationDF['DlySum4DaysAgo'] >= RAINY)  & (stationDF['DlySum5DaysAgo'] >= RAINY)  & (stationDF['DlySum6DaysAgo'] >= RAINY),'DaysOfRain'] = 7
 
    # Create a column that shows (for each day) how many days it has been DRY. 
    # 1 = just today; 2 = today and yesterday; etc.

    stationDF["DaysOfDry"] = 0   
    stationDF.loc[(stationDF["DlySumToday"] <= DRY), "DaysOfDry"] = 1
    stationDF.loc[(stationDF['DlySumToday'] <= DRY) & (stationDF['DlySum1DayAgo'] <= DRY), 'DaysOfDry'] = 2
    stationDF.loc[(stationDF['DlySumToday'] <= DRY) & (stationDF['DlySum1DayAgo'] <= DRY) & (stationDF['DlySum2DaysAgo'] <= DRY), 'DaysOfDry'] = 3
    stationDF.loc[(stationDF['DlySumToday'] <= DRY) & (stationDF['DlySum1DayAgo'] <= DRY) & (stationDF['DlySum2DaysAgo'] <= DRY) & (stationDF['DlySum3DaysAgo'] <= DRY), 'DaysOfDry'] = 4
    stationDF.loc[(stationDF['DlySumToday'] <= DRY) & (stationDF['DlySum1DayAgo'] <= DRY) & (stationDF['DlySum2DaysAgo'] <= DRY) & (stationDF['DlySum3DaysAgo'] <= DRY) & (stationDF['DlySum4DaysAgo'] <= DRY), 'DaysOfDry'] = 5
    stationDF.loc[(stationDF['DlySumToday'] <= DRY) & (stationDF['DlySum1DayAgo'] <= DRY) & (stationDF['DlySum2DaysAgo'] <= DRY) & (stationDF['DlySum3DaysAgo'] <= DRY) & (stationDF['DlySum4DaysAgo'] <= DRY)  & (stationDF['DlySum5DaysAgo'] <= DRY),'DaysOfDry'] = 6
    stationDF.loc[(stationDF['DlySumToday'] <= DRY) & (stationDF['DlySum1DayAgo'] <= DRY) & (stationDF['DlySum2DaysAgo'] <= DRY) & (stationDF['DlySum3DaysAgo'] <= DRY) & (stationDF['DlySum4DaysAgo'] <= DRY)  & (stationDF['DlySum5DaysAgo'] <= DRY)  & (stationDF['DlySum6DaysAgo'] <= DRY),'DaysOfDry'] = 7

    # Add columns for "dry tomorrow" and "rainy tomorrow"

    stationDF["RainTomorrow"] = ""   
    stationDF.loc[(stationDF["DlySumTomorrow"] >= RAINY), "RainTomorrow"] = "Y"

    stationDF["DryTomorrow"] = ""   
    stationDF.loc[(stationDF["DlySumTomorrow"] <= DRY), "DryTomorrow"] = "Y"

    # Report data facts for this station.
    
    station_final_rows = len(stationDF)
    print ("Raw rows in this station = " + str(station_raw_rows) + ", final (curated) rows = " + str(station_final_rows) + ".")
    
    # Join this station to all stations
    
    hpdDF = pd.concat([hpdDF, stationDF], ignore_index=True)

    # End of loop over stations

# Show some overall stats

TotalRows = len(hpdDF)
print ("\nTotal curated data points (rows) for all stations and days = " + str(TotalRows)) 

print ("\nAverage rain per day overall = " + str(round(hpdDF["DlySumToday"].mean(), 2)) + " inches")

TotalRainy = len(hpdDF[hpdDF['DlySumToday'] >= RAINY])
PctRainy = round((TotalRainy / TotalRows * 100), 1)
print ("\nFraction of days overall that are rainy = " + str(PctRainy) + "%")

TotalDry = len(hpdDF[hpdDF['DlySumToday'] <= DRY])
PctDry = round((TotalDry / TotalRows * 100), 1)
print ("\nFraction of days overall that are dry = " + str(PctDry) + "%")

# Make subsets for each number of rainy and dry days, 

Rainy1DayDF = hpdDF.query("DaysOfRain == 1")
Rainy2DaysDF = hpdDF.query("DaysOfRain == 2")
Rainy3DaysDF = hpdDF.query("DaysOfRain == 3")
Rainy4DaysDF = hpdDF.query("DaysOfRain == 4")
Rainy5DaysDF = hpdDF.query("DaysOfRain == 5")
Rainy6DaysDF = hpdDF.query("DaysOfRain == 6")
Rainy7DaysDF = hpdDF.query("DaysOfRain == 7")

Dry1DayDF = hpdDF.query("DaysOfDry == 1")
Dry2DaysDF = hpdDF.query("DaysOfDry == 2")
Dry3DaysDF = hpdDF.query("DaysOfDry == 3")
Dry4DaysDF = hpdDF.query("DaysOfDry == 4")
Dry5DaysDF = hpdDF.query("DaysOfDry == 5")
Dry6DaysDF = hpdDF.query("DaysOfDry == 6")
Dry7DaysDF = hpdDF.query("DaysOfDry == 7")

# Report the runs of rainy/dry days found.

Rainy1DayCount = len(Rainy1DayDF)
print ("\nFound " + str(Rainy1DayCount) + " runs of 1 rainy day.")
Rainy2DaysCount = len(Rainy2DaysDF)
print ("Found " + str(Rainy2DaysCount) + " runs of 2 rainy days.")
Rainy3DaysCount = len(Rainy3DaysDF)
print ("Found " + str(Rainy3DaysCount) + " runs of 3 rainy days.")
Rainy4DaysCount = len(Rainy4DaysDF)
print ("Found " + str(Rainy4DaysCount) + " runs of 4 rainy days.")
Rainy5DaysCount = len(Rainy5DaysDF)
print ("Found " + str(Rainy5DaysCount) + " runs of 5 rainy days.")
Rainy6DaysCount = len(Rainy6DaysDF)
print ("Found " + str(Rainy6DaysCount) + " runs of 6 rainy days.")
Rainy7DaysCount = len(Rainy7DaysDF)
print ("Found " + str(Rainy7DaysCount) + " runs of 7 rainy days.")

Dry1DayCount = len(Dry1DayDF)
print ("\nFound " + str(Dry1DayCount) + " runs of 1 dry day.")
Dry2DaysCount = len(Dry2DaysDF)
print ("Found " + str(Dry2DaysCount) + " runs of 2 dry days.")
Dry3DaysCount = len(Dry3DaysDF)
print ("Found " + str(Dry3DaysCount) + " runs of 3 dry days.")
Dry4DaysCount = len(Dry4DaysDF)
print ("Found " + str(Dry4DaysCount) + " runs of 4 dry days.")
Dry5DaysCount = len(Dry5DaysDF)
print ("Found " + str(Dry5DaysCount) + " runs of 5 dry days.")
Dry6DaysCount = len(Dry6DaysDF)
print ("Found " + str(Dry6DaysCount) + " runs of 6 dry days.")
Dry7DaysCount = len(Dry7DaysDF)
print ("Found " + str(Dry7DaysCount) + " runs of 7 dry days.")

# Find average rain on the day after each run.

print ("\nAverage rainfall after 1 day of rain... " + str(round(Rainy1DayDF["DlySumTomorrow"].mean(), 2)))
print ("Average rainfall after 2 days of rain... " + str(round(Rainy2DaysDF["DlySumTomorrow"].mean(), 2)))
print ("Average rainfall after 3 days of rain... " + str(round(Rainy3DaysDF["DlySumTomorrow"].mean(), 2)))
print ("Average rainfall after 4 days of rain... " + str(round(Rainy4DaysDF["DlySumTomorrow"].mean(), 2)))
print ("Average rainfall after 5 days of rain... " + str(round(Rainy5DaysDF["DlySumTomorrow"].mean(), 2)))
print ("Average rainfall after 6 days of rain... " + str(round(Rainy6DaysDF["DlySumTomorrow"].mean(), 2)))
print ("Average rainfall after 7 days of rain... " + str(round(Rainy7DaysDF["DlySumTomorrow"].mean(), 2)))

print ("\nAverage rainfall after 1 day of dry... " + str(round(Dry1DayDF["DlySumTomorrow"].mean(), 2)))
print ("Average rainfall after 2 days of dry... " + str(round(Dry2DaysDF["DlySumTomorrow"].mean(), 2)))
print ("Average rainfall after 3 days of dry... " + str(round(Dry3DaysDF["DlySumTomorrow"].mean(), 2)))
print ("Average rainfall after 4 days of dry... " + str(round(Dry4DaysDF["DlySumTomorrow"].mean(), 2)))
print ("Average rainfall after 5 days of dry... " + str(round(Dry5DaysDF["DlySumTomorrow"].mean(), 2)))
print ("Average rainfall after 6 days of dry... " + str(round(Dry6DaysDF["DlySumTomorrow"].mean(), 2)))
print ("Average rainfall after 7 days of dry... " + str(round(Dry7DaysDF["DlySumTomorrow"].mean(), 2)))

# Find chance of rainy/dry day after each run.
# TODO


