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

#import os
#import fnmatch
import pandas as pd 
#import datetime as dt

HPD_DIR = "https://www.ncei.noaa.gov/data/coop-hourly-precipitation/v2/access/"
ACADIA = "USC00170100.csv" 
AMARILLO = "USW00023047.csv" 
ETNA = "USC00042899.csv" 
STATIONS = [ACADIA, AMARILLO, ETNA]
NO_RAIN = 0.1   # less than this is considered "no rain today"
RAINY = 0.5     # more than this is considered "a rainy day"

# Make an empty dataframe to hold combined data across stations.

hpdDF = pd.DataFrame()

# Loop over all the stations, processing and enhancing that data, then add it to an overall dataset

for s in STATIONS:
    
    # Get data for one station and do some basic data cleanup.

    station_url = HPD_DIR + s 
    stationDF = pd.read_csv(station_url, sep=',', header='infer', dtype=str)

    stationDF = stationDF.query("DlySumQF != 'P'")    # throw out dates with a bad daily quality flag
    stationDF = stationDF[["STATION","NAME","DATE","DlySum"]]    # keep only fields we need
    stationDF = stationDF.rename({"DlySum":"DlySumToday"}, axis='columns')  # to distinquish from other days we will join in
    stationDF["DlySumToday"] = stationDF["DlySumToday"].astype(int) / 100   # convert totals from hundreths to inches
    stationDF["DATE"] = pd.to_datetime(stationDF["DATE"], errors='coerce')  # put in true date format

    # Grab a snapshot for a self-join later. Adjust fields names to avoid confusion after the join.

    stationCopyDF = stationDF
    stationCopyDF = stationCopyDF[["STATION","DATE","DlySumToday"]]  # keep just what we need
    stationCopyDF = stationCopyDF.rename({"DlySumToday":"DlySumOther", "DATE":"DATEother"}, axis='columns')  

    # Add in some other dates, for which we will pull in rainfall.

    stationDF["DATE_minus3"] = stationDF["DATE"] - pd.offsets.Day(3)
    stationDF["DATE_minus2"] = stationDF["DATE"] - pd.offsets.Day(2)
    stationDF["DATE_minus1"] = stationDF["DATE"] - pd.offsets.Day(1)
    stationDF["DATE_plus1"] = stationDF["DATE"] + pd.offsets.Day(1)

    # Join other rainfall onto base record. Adjust column names to make clear what we did.

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

    # Join the lookback/lookahead data to the base data
    
    hpdDF = pd.concat([hpdDF, stationDF], ignore_index=True)

    # End of loop over stations
 
