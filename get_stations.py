#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Jul 12 12:25:22 2023

@author: chuck
"""

from rain_helpers import ALL_STATION_FILES


from urllib import request

HPD_CLOUD_DIR = "https://www.ncei.noaa.gov/data/coop-hourly-precipitation/v2/access/"  # Hourly Precipitation Data (HPD)
HPD_LOCAL_DIR = "/Users/chuck/Desktop/Articles/NOAA/HPD/"
SKIP_COUNT = 1


for i in range (0, len(ALL_STATION_FILES), SKIP_COUNT):
    print ("\n" + ALL_STATION_FILES[i])
    request.urlretrieve(HPD_CLOUD_DIR + ALL_STATION_FILES[i], HPD_LOCAL_DIR + ALL_STATION_FILES[i])


