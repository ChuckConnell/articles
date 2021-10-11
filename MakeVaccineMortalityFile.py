#!/usr/bin/env python
# coding: utf-8

# In[1]:

# This program creates a data file that contains covid vaccine rates versus mortality, 
# across time periods, for all US counties.

import pandas as pd 
from month_abbreviation import us_state_to_abbrev
#from sys import exit

OVERALL_START_DATE = pd.to_datetime("2021-03-01")
PERIOD_LENGTH = 60  # days over which to count deaths
PERIOD_COUNT = 3  # how many time blocks to count
VAX_BACKDATE = 30     # how far back from start/end dates do we look for vax info
COUNTY_OUTPUT_FILE = "AllCountiesAllPeriods.tsv"

# Get the source data. 

# https://data.cdc.gov/Vaccinations/COVID-19-Vaccinations-in-the-United-States-County/8xkx-amqh 
path = "C:/Users/chuck/Desktop/COVID Data/CDC/COVID-19_Vaccinations_in_the_United_States_County_04oct2021.tsv"
VaxDF = pd.read_csv(path, sep='\t', header='infer', dtype=str)

# https://github.com/nytimes/covid-19-data
path = "C:/Users/chuck/Desktop/COVID Data/NYTimes/us-counties-04oct2021.txt"
DeathDF = pd.read_csv(path, sep=',', header='infer', dtype=str)

# https://www.census.gov/programs-surveys/popest/technical-documentation/research/evaluation-estimates/2020-evaluation-estimates/2010s-counties-total.html  
path = "C:/Users/chuck/Desktop/COVID Data/US Census/co-est2020.csv"
PopDF = pd.read_csv(path, sep=',', header='infer', dtype=str, encoding='latin-1')

# We only need a few columns from the files. 

VaxDF = VaxDF[["Date", "Recip_County", "Recip_State", "Series_Complete_Yes", "Administered_Dose1_Recip"]]
DeathDF = DeathDF[["date", "county", "state", "deaths"]]
PopDF = PopDF[["STNAME", "CTYNAME", "POPESTIMATE2020"]]

# Fix the data types and clean up missing values. For any column name that has a space, change it to underscore.

VaxDF["Date"] = pd.to_datetime(VaxDF["Date"], errors='coerce')
VaxDF["Series_Complete_Yes"] = pd.to_numeric(VaxDF["Series_Complete_Yes"], errors='coerce').fillna(0).astype(int)
VaxDF["Administered_Dose1_Recip"] = pd.to_numeric(VaxDF["Administered_Dose1_Recip"], errors='coerce').fillna(0).astype(int)

DeathDF["date"] = pd.to_datetime(DeathDF["date"], errors='coerce')
DeathDF["deaths"] = pd.to_numeric(DeathDF["deaths"], errors='coerce').fillna(0).astype(int)

PopDF["POPESTIMATE2020"] = pd.to_numeric(PopDF["POPESTIMATE2020"], errors='coerce').fillna(0).astype(int)

# Misc data clean up looking for obviously bad rows.

VaxDF = VaxDF[VaxDF.Series_Complete_Yes >= 0]
VaxDF = VaxDF[VaxDF.Administered_Dose1_Recip >= 0]
DeathDF = DeathDF[DeathDF.deaths >= 0]
PopDF = PopDF[PopDF.POPESTIMATE2020 > 0]   

# In the population and death files, the states are spelled out. We want the abbreviation.

DeathDF["state_abbr"] = DeathDF['state'].map(us_state_to_abbrev).fillna(DeathDF["state"])
PopDF["ST_ABBR"] = PopDF['STNAME'].map(us_state_to_abbrev).fillna(PopDF["STNAME"])

DeathDF = DeathDF.drop(columns=["state"])   # don't need these anymore
PopDF = PopDF.drop(columns=["STNAME"])

# Make the counties upper case.

VaxDF["Recip_County"] = VaxDF["Recip_County"].str.upper()
DeathDF["county"] = DeathDF["county"].str.upper()
PopDF["CTYNAME"] = PopDF["CTYNAME"].str.upper()

# The Vax and Pop files have the word COUNTY on the county name. Trim this for consistency.

VaxDF["Recip_County"] = VaxDF["Recip_County"].str.split(" COUNTY").str[0]
PopDF["CTYNAME"] = PopDF["CTYNAME"].str.split(" COUNTY").str[0]

# Make a new column that is "STATE-COUNTY" in each dataset. 
# Note that some of the counties are not counties, but something like "Bethel Census Area". We will go with this for now
# because it might join to a matching item across our files.

VaxDF["STATE-COUNTY"] = VaxDF["Recip_State"] + "-" + VaxDF["Recip_County"]
DeathDF["STATE-COUNTY"] = DeathDF["state_abbr"] + "-" + DeathDF["county"]
PopDF["STATE-COUNTY"] = PopDF["ST_ABBR"] + "-" + PopDF["CTYNAME"]

# Get rid of the separate state and county fields, but keep one for state since it could be useful for some analysis.

VaxDF = VaxDF.drop(columns=["Recip_State", "Recip_County"])  
DeathDF = DeathDF.drop(columns=["state_abbr", "county"])
PopDF = PopDF.drop(columns=["CTYNAME"])   # keep ST_ABBR

'''
print (VaxDF.head(10), "\n")
print (DeathDF.head(10), "\n")
print (PopDF.head(10), "\n")

print (VaxDF.dtypes, "\n")
print (DeathDF.dtypes, "\n")
print (PopDF.dtypes, "\n")
'''

# Make a DataFrame that will hold all of our results.

AllCountiesAllPeriodsDF = pd.DataFrame()

# Loop over our whole time period. 

for this_period in range(PERIOD_COUNT):
    
    # Calc the dates we need.
    
    this_period_start = OVERALL_START_DATE + pd.offsets.Day(this_period * PERIOD_LENGTH)
    this_period_end = this_period_start + pd.offsets.Day(PERIOD_LENGTH)
    this_period_vax_start = this_period_start - pd.offsets.Day(VAX_BACKDATE)
    this_period_vax_end = this_period_end - pd.offsets.Day(VAX_BACKDATE)
    
    print ("Working on mortality between {} and {}, with vax dates of {} and {}...\n".format(this_period_start.date(), this_period_end.date(), this_period_vax_start.date(), this_period_vax_end.date()))
    
    # Get the deaths in each county for the start and end of this time period. We get two DFs and will join them later.
    
    PeriodStartDeathDF = DeathDF[DeathDF.date == this_period_start]
    PeriodEndDeathDF = DeathDF[DeathDF.date == this_period_end]

    # Rename some death columns so we can keep them straight after the join.
    
    PeriodStartDeathDF = PeriodStartDeathDF.rename(columns={"date": "StartDate"})
    PeriodEndDeathDF = PeriodEndDeathDF.rename(columns={"date": "EndDate"})
    
    PeriodStartDeathDF = PeriodStartDeathDF.rename(columns={"deaths": "StartDeaths"})
    PeriodEndDeathDF = PeriodEndDeathDF.rename(columns={"deaths": "EndDeaths"})

    # Get the vax facts for start and end of this time period.
    
    PeriodStartVaxDF = VaxDF[VaxDF.Date == this_period_vax_start]
    PeriodEndVaxDF = VaxDF[VaxDF.Date == this_period_vax_end]

    # Rename some vax columns so we can keep them straight after the join.

    PeriodStartVaxDF = PeriodStartVaxDF.rename(columns={"Date": "VaxStartDate"})
    PeriodEndVaxDF = PeriodEndVaxDF.rename(columns={"Date": "VaxEndDate"})

    PeriodStartVaxDF = PeriodStartVaxDF.rename(columns={"Series_Complete_Yes": "Series_Complete_Yes_Start"})
    PeriodEndVaxDF = PeriodEndVaxDF.rename(columns={"Series_Complete_Yes": "Series_Complete_Yes_End"})

    PeriodStartVaxDF = PeriodStartVaxDF.rename(columns={"Administered_Dose1_Recip": "Administered_Dose1_Recip_Start"})
    PeriodEndVaxDF = PeriodEndVaxDF.rename(columns={"Administered_Dose1_Recip": "Administered_Dose1_Recip_End"})  
    
    # Join all the info we have so each row is one county for this time period. Use inner join because we only want counties
    # that we have full info about.
    
    AllCountiesOnePeriodDF = PeriodEndDeathDF.merge(PeriodStartDeathDF, how='inner', on="STATE-COUNTY")
    AllCountiesOnePeriodDF = AllCountiesOnePeriodDF.merge(PopDF, how='inner', on="STATE-COUNTY")
    AllCountiesOnePeriodDF = AllCountiesOnePeriodDF.merge(PeriodStartVaxDF, how='inner', on="STATE-COUNTY")
    AllCountiesOnePeriodDF = AllCountiesOnePeriodDF.merge(PeriodEndVaxDF, how='inner', on="STATE-COUNTY")
    
    # Add all counties for this time period to the overall result set. 
    
    AllCountiesAllPeriodsDF = AllCountiesAllPeriodsDF.append(AllCountiesOnePeriodDF)
        
    # End of big loop gathering data for each time period. Now we can start the analysis on it.

# Make a column that shows deaths in each county in each period. 
# This will produce some negative numbers, since death counts can be readjusted later or incorrect. Throw out those rows.

AllCountiesAllPeriodsDF["Deaths"] = (AllCountiesAllPeriodsDF["EndDeaths"] - AllCountiesAllPeriodsDF["StartDeaths"])
AllCountiesAllPeriodsDF = AllCountiesAllPeriodsDF[AllCountiesAllPeriodsDF.Deaths >= 0]

# Make a column that is average vax for this period, for both vax numbers.

AllCountiesAllPeriodsDF["Series_Complete_Yes_Mid"] = (AllCountiesAllPeriodsDF["Series_Complete_Yes_Start"] + AllCountiesAllPeriodsDF["Series_Complete_Yes_End"]) / 2
AllCountiesAllPeriodsDF["Administered_Dose1_Recip_Mid"] = (AllCountiesAllPeriodsDF["Administered_Dose1_Recip_Start"] + AllCountiesAllPeriodsDF["Administered_Dose1_Recip_End"]) / 2

# Make new columns that are "deaths per 100k pop", "fully vaccinated per 100" and "one+ vax per 100".

AllCountiesAllPeriodsDF["FullVaxPer100"] = (100*(AllCountiesAllPeriodsDF["Series_Complete_Yes_Mid"]/AllCountiesAllPeriodsDF["POPESTIMATE2020"])).round(1)
AllCountiesAllPeriodsDF["OnePlusVaxPer100"] = (100*(AllCountiesAllPeriodsDF["Administered_Dose1_Recip_Mid"]/AllCountiesAllPeriodsDF["POPESTIMATE2020"])).round(1)
AllCountiesAllPeriodsDF["DeathsPer100k"] = (100000*(AllCountiesAllPeriodsDF["Deaths"]/AllCountiesAllPeriodsDF["POPESTIMATE2020"])).round(1)

# If vax % is greater than 100, probably bad data, throw it out.

AllCountiesAllPeriodsDF = AllCountiesAllPeriodsDF[AllCountiesAllPeriodsDF.FullVaxPer100 <= 100]  
AllCountiesAllPeriodsDF = AllCountiesAllPeriodsDF[AllCountiesAllPeriodsDF.OnePlusVaxPer100 <= 100]

# Write to file.

print ("Writing county output to", COUNTY_OUTPUT_FILE, "with", AllCountiesAllPeriodsDF.shape[0], "rows.\n")
AllCountiesAllPeriodsDF.to_csv(COUNTY_OUTPUT_FILE, encoding='utf-8', sep='\t', index=False)



# In[ ]:





# In[ ]:





# In[ ]:




