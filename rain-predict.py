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

# Libraries

#import os
#import fnmatch
import pandas as pd 
#import datetime as dt

# Include files

from rain_helpers import STATION_FILES

# Constants

HPD_CLOUD_DIR = "https://www.ncei.noaa.gov/data/coop-hourly-precipitation/v2/access/"  # Hourly Precipitation Data (HPD)
HPD_LOCAL_DIR = "/Users/chuck/Desktop/Articles/NOAA/"
SUNNY = 0.05  # less than this is considered "a sunny day"
RAINY = 0.5     # more than this is considered "a rainy day"
CUTOFF_DATE = "20000101"  # Only look at recent data in case climate change has affected things. This is the format needed by pandas query().

# Tell user key settings.

print ("\nTaking data after " + str(pd.to_datetime(CUTOFF_DATE)) + ".")
print ("\nUsing daily rainfall >= " + str(RAINY) + " inches as a 'rainy' day, and <= " + str(SUNNY) + " inches as a 'sunny' day.\n")

# Make an empty dataframe to hold combined data across stations.

hpdDF = pd.DataFrame()

# Loop over all the stations, processing and enhancing that data, then add it to an overall dataset

for f in STATION_FILES:
    
    # Get data for one station and report to user.
    
    station_url = HPD_LOCAL_DIR + f 
    stationDF = pd.read_csv(station_url, sep=',', header='infer', dtype=str)
    print ("Working on Station ID " + stationDF["STATION"].iloc[1] + " at location " + stationDF["NAME"].iloc[1] + ", with " + str(stationDF.shape[0]) + " rows.")

    # Some data cleanup.

    stationDF = stationDF.query("DlySumQF != 'P'")    # throw out dates with a bad daily quality flag
    stationDF = stationDF[["STATION","NAME","DATE","DlySum"]]    # keep only fields we need
    stationDF = stationDF.rename({"DlySum":"DlySumToday"}, axis='columns')  # to distinquish from other days we will join in
    stationDF["DlySumToday"] = stationDF["DlySumToday"].astype(int) / 100   # convert totals from hundreths to inches
    stationDF["DATE"] = pd.to_datetime(stationDF["DATE"], errors='coerce')  # put in true date format
    stationDF = stationDF.query("DATE >= " + CUTOFF_DATE)   # only take recent days

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
 
    # Create a column that shows (for each day) how many days it has been sunny. 
    # 1 = just today; 2 = today and yesterday; etc.

    stationDF["DaysOfSun"] = 0   
    stationDF.loc[(stationDF["DlySumToday"] <= SUNNY), "DaysOfSun"] = 1
    stationDF.loc[(stationDF['DlySumToday'] <= SUNNY) & (stationDF['DlySum1DayAgo'] <= SUNNY), 'DaysOfSun'] = 2
    stationDF.loc[(stationDF['DlySumToday'] <= SUNNY) & (stationDF['DlySum1DayAgo'] <= SUNNY) & (stationDF['DlySum2DaysAgo'] <= SUNNY), 'DaysOfSun'] = 3
    stationDF.loc[(stationDF['DlySumToday'] <= SUNNY) & (stationDF['DlySum1DayAgo'] <= SUNNY) & (stationDF['DlySum2DaysAgo'] <= SUNNY) & (stationDF['DlySum3DaysAgo'] <= SUNNY), 'DaysOfSun'] = 4
    stationDF.loc[(stationDF['DlySumToday'] <= SUNNY) & (stationDF['DlySum1DayAgo'] <= SUNNY) & (stationDF['DlySum2DaysAgo'] <= SUNNY) & (stationDF['DlySum3DaysAgo'] <= SUNNY) & (stationDF['DlySum4DaysAgo'] <= SUNNY), 'DaysOfSun'] = 5
    stationDF.loc[(stationDF['DlySumToday'] <= SUNNY) & (stationDF['DlySum1DayAgo'] <= SUNNY) & (stationDF['DlySum2DaysAgo'] <= SUNNY) & (stationDF['DlySum3DaysAgo'] <= SUNNY) & (stationDF['DlySum4DaysAgo'] <= SUNNY)  & (stationDF['DlySum5DaysAgo'] <= SUNNY),'DaysOfSun'] = 6
    stationDF.loc[(stationDF['DlySumToday'] <= SUNNY) & (stationDF['DlySum1DayAgo'] <= SUNNY) & (stationDF['DlySum2DaysAgo'] <= SUNNY) & (stationDF['DlySum3DaysAgo'] <= SUNNY) & (stationDF['DlySum4DaysAgo'] <= SUNNY)  & (stationDF['DlySum5DaysAgo'] <= SUNNY)  & (stationDF['DlySum6DaysAgo'] <= SUNNY),'DaysOfSun'] = 7
 
    # Join this station to all stations
    
    hpdDF = pd.concat([hpdDF, stationDF], ignore_index=True)

    # End of loop over stations

print ("\nTotal days with valid data = " + str(hpdDF.shape[0])) 

# Make subsets for each number of rainy and sunny days, and report average rain the next day

Rainy0DaysDF = hpdDF.query("DaysOfRain == 0")
Rainy1DayDF = hpdDF.query("DaysOfRain == 1")
Rainy2DaysDF = hpdDF.query("DaysOfRain == 2")
Rainy3DaysDF = hpdDF.query("DaysOfRain == 3")
Rainy4DaysDF = hpdDF.query("DaysOfRain == 4")
Rainy5DaysDF = hpdDF.query("DaysOfRain == 5")
Rainy6DaysDF = hpdDF.query("DaysOfRain == 6")
Rainy7DaysDF = hpdDF.query("DaysOfRain == 7")

print ("\nFound " + str(Rainy0DaysDF.shape[0]) + " not-rainy days.")
print ("Found " + str(Rainy1DayDF.shape[0]) + " runs of 1 rainy day.")
print ("Found " + str(Rainy2DaysDF.shape[0]) + " runs of 2 rainy days.")
print ("Found " + str(Rainy3DaysDF.shape[0]) + " runs of 3 rainy days.")
print ("Found " + str(Rainy4DaysDF.shape[0]) + " runs of 4 rainy days.")
print ("Found " + str(Rainy5DaysDF.shape[0]) + " runs of 5 rainy days.")
print ("Found " + str(Rainy6DaysDF.shape[0]) + " runs of 6 rainy days.")
print ("Found " + str(Rainy7DaysDF.shape[0]) + " runs of 7 rainy days.")

Sunny0DaysDF = hpdDF.query("DaysOfSun == 0")
Sunny1DayDF = hpdDF.query("DaysOfSun == 1")
Sunny2DaysDF = hpdDF.query("DaysOfSun == 2")
Sunny3DaysDF = hpdDF.query("DaysOfSun == 3")
Sunny4DaysDF = hpdDF.query("DaysOfSun == 4")
Sunny5DaysDF = hpdDF.query("DaysOfSun == 5")
Sunny6DaysDF = hpdDF.query("DaysOfSun == 6")
Sunny7DaysDF = hpdDF.query("DaysOfSun == 7")

print ("\nFound " + str(Sunny0DaysDF.shape[0]) + " not-sunny days.")
print ("Found " + str(Sunny1DayDF.shape[0]) + " runs of 1 sunny day.")
print ("Found " + str(Sunny2DaysDF.shape[0]) + " runs of 2 sunny days.")
print ("Found " + str(Sunny3DaysDF.shape[0]) + " runs of 3 sunny days.")
print ("Found " + str(Sunny4DaysDF.shape[0]) + " runs of 4 sunny days.")
print ("Found " + str(Sunny5DaysDF.shape[0]) + " runs of 5 sunny days.")
print ("Found " + str(Sunny6DaysDF.shape[0]) + " runs of 6 sunny days.")
print ("Found " + str(Sunny7DaysDF.shape[0]) + " runs of 7 sunny days.")

# Calc average rainfall on the day after a run of sunny or rainy days of varying lengths.

print ("\nAverage rain after 0 days of rain... " + str(round(Rainy0DaysDF["DlySumTomorrow"].mean(), 2)))
print ("Average rain after 1 day of rain... " + str(round(Rainy1DayDF["DlySumTomorrow"].mean(), 2)))
print ("Average rain after 2 days of rain... " + str(round(Rainy2DaysDF["DlySumTomorrow"].mean(), 2)))
print ("Average rain after 3 days of rain... " + str(round(Rainy3DaysDF["DlySumTomorrow"].mean(), 2)))
print ("Average rain after 4 days of rain... " + str(round(Rainy4DaysDF["DlySumTomorrow"].mean(), 2)))
print ("Average rain after 5 days of rain... " + str(round(Rainy5DaysDF["DlySumTomorrow"].mean(), 2)))
print ("Average rain after 6 days of rain... " + str(round(Rainy6DaysDF["DlySumTomorrow"].mean(), 2)))
print ("Average rain after 7 days of rain... " + str(round(Rainy7DaysDF["DlySumTomorrow"].mean(), 2)))

print ("\nAverage rain after 0 days of sun... " + str(round(Sunny0DaysDF["DlySumTomorrow"].mean(), 2)))
print ("Average rain after 1 day of sun... " + str(round(Sunny1DayDF["DlySumTomorrow"].mean(), 2)))
print ("Average rain after 2 days of sun... " + str(round(Sunny2DaysDF["DlySumTomorrow"].mean(), 2)))
print ("Average rain after 3 days of sun... " + str(round(Sunny3DaysDF["DlySumTomorrow"].mean(), 2)))
print ("Average rain after 4 days of sun... " + str(round(Sunny4DaysDF["DlySumTomorrow"].mean(), 2)))
print ("Average rain after 5 days of sun... " + str(round(Sunny5DaysDF["DlySumTomorrow"].mean(), 2)))
print ("Average rain after 6 days of sun... " + str(round(Sunny6DaysDF["DlySumTomorrow"].mean(), 2)))
print ("Average rain after 7 days of sun... " + str(round(Sunny7DaysDF["DlySumTomorrow"].mean(), 2)))

