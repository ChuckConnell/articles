# Databricks notebook source
# This program creates a dataset that contains covid vaccine rates versus mortality, across time periods, for all US counties, then analyzes the data.

# PANDAS/PS-PANDAS DIFFERENCE - Import pyspark.pandas instead of pandas. Change all your pandas references from pd to pspd. (Some Databricks blogs 
# suggest ps as the alias, but I think pspd (PySpark Pandas) is clearer as it distinguishes from plain PySpark.)

#import pandas as pd
from pyspark import pandas as pspd   
import datetime

# PANDAS/PS-PANDAS DIFFERENCE - The standard method for checking your pandas version does not work under PySpark. Instead, check the PySpark version, which
# includes the pandas API.
#print(pd.__version__)
print (sc.version)

OVERALL_START_DATE = pspd.to_datetime("2021-03-01")
PERIOD_LENGTH = 30  # days over which to count deaths
PERIOD_COUNT = 7  # how many time blocks to count
VAX_BACKDATE = 21     # how far back from start/end dates do we look for vax info

# COMMAND ----------

# Some datasets come with states spelled out, some with abbreviations. We need to make them all the same.
# Note that Python dicts are case sensitive, so this lookup assumes the full name is capitalized correctly.

us_state_to_abbrev = { 
    "Alabama": "AL",
    "Alaska": "AK",
    "Arizona": "AZ",
    "Arkansas": "AR",
    "California": "CA",
    "Colorado": "CO",
    "Connecticut": "CT",
    "Delaware": "DE",
    "Florida": "FL",
    "Georgia": "GA",
    "Hawaii": "HI",
    "Idaho": "ID",
    "Illinois": "IL",
    "Indiana": "IN",
    "Iowa": "IA",
    "Kansas": "KS",
    "Kentucky": "KY",
    "Louisiana": "LA",
    "Maine": "ME",
    "Maryland": "MD",
    "Massachusetts": "MA",
    "Michigan": "MI",
    "Minnesota": "MN",
    "Mississippi": "MS",
    "Missouri": "MO",
    "Montana": "MT",
    "Nebraska": "NE",
    "Nevada": "NV",
    "New Hampshire": "NH",
    "New Jersey": "NJ",
    "New Mexico": "NM",
    "New York": "NY",
    "North Carolina": "NC",
    "North Dakota": "ND",
    "Ohio": "OH",
    "Oklahoma": "OK",
    "Oregon": "OR",
    "Pennsylvania": "PA",
    "Rhode Island": "RI",
    "South Carolina": "SC",
    "South Dakota": "SD",
    "Tennessee": "TN",
    "Texas": "TX",
    "Utah": "UT",
    "Vermont": "VT",
    "Virginia": "VA",
    "Washington": "WA",
    "West Virginia": "WV",
    "Wisconsin": "WI",
    "Wyoming": "WY",
    "District of Columbia": "DC",
    "American Samoa": "AS",
    "Guam": "GU",
    "Northern Mariana Islands": "MP",
    "Puerto Rico": "PR",
    "United States Minor Outlying Islands": "UM",
    "U.S. Virgin Islands": "VI",
}


# COMMAND ----------

# PANDAS/PS-PANDAS DIFFERENCE - Spark runs on a cloud cluster. It cannot see your local computer so cannot do read_csv() of a local file.
# Before running this code, use the Databricks GUI to manually copy the source files to a DBFS directory. You can move multiple files in one click. 
# (You don't need a Databricks table, just move the files to DBFS.)
# Note that the process of copying files from a local directory to DBFS will change hyphen characters to underscore.

# Get the source data. 

# https://data.cdc.gov/Vaccinations/COVID-19-Vaccinations-in-the-United-States-County/8xkx-amqh 
path = "/FileStore/tables/county-months/COVID_19_Vaccinations_in_the_United_States_County_04oct2021.tsv"
VaxDF = pspd.read_csv(path, sep='\t', header='infer', dtype=str)

# https://github.com/nytimes/covid-19-data
path = "/FileStore/tables/county-months/us_counties_04oct2021.txt"
DeathDF = pspd.read_csv(path, sep=',', header='infer', dtype=str)

# https://www.census.gov/programs-surveys/popest/technical-documentation/research/evaluation-estimates/2020-evaluation-estimates/2010s-counties-total.html  
path = "/FileStore/tables/county-months/co_est2020.csv"
PopDF = pspd.read_csv(path, sep=',', header='infer', dtype=str, encoding='Windows-1252')

# PANDAS/PS-PANDAS DIFFERENCE - pspd does not support latin-1 encoding. If your data file uses it, try changing to Windows-1252, which is similar but not the same.

# We only need a few columns from the files. 

VaxDF = VaxDF[["Date", "Recip_County", "Recip_State", "Series_Complete_Yes", "Administered_Dose1_Recip"]]
DeathDF = DeathDF[["date", "county", "state", "deaths"]]
PopDF = PopDF[["STNAME", "CTYNAME", "POPESTIMATE2020"]]

# Fix the data types and clean up missing values. For any column name that has a space, change it to underscore.

VaxDF["Date"] = pspd.to_datetime(VaxDF["Date"], errors='coerce')
VaxDF["Series_Complete_Yes"] = pspd.to_numeric(VaxDF["Series_Complete_Yes"]).fillna(0).astype(int)
VaxDF["Administered_Dose1_Recip"] = pspd.to_numeric(VaxDF["Administered_Dose1_Recip"]).fillna(0).astype(int)

DeathDF["date"] = pspd.to_datetime(DeathDF["date"], errors='coerce')
DeathDF["deaths"] = pspd.to_numeric(DeathDF["deaths"]).fillna(0).astype(int)

PopDF["POPESTIMATE2020"] = pspd.to_numeric(PopDF["POPESTIMATE2020"]).fillna(0).astype(int)

# PANDAS/PS-PANDAS DIFFERENCE - pspd.to_numeric() does not support the errors option.

# PANDAS/PS-PANDAS DIFFERENCE - pspd.to_datetime() sometimes throws a warning to "specify type hints for pandas UDF instead of specifying pandas 
# UDF type. See SPARK-28264." This seems to happen randomly and I have not seen it for a while . In all cases, the function still works. 

# Misc data clean up looking for obviously bad rows.

VaxDF = VaxDF[VaxDF.Series_Complete_Yes >= 0]
VaxDF = VaxDF[VaxDF.Administered_Dose1_Recip >= 0]
DeathDF = DeathDF[DeathDF.deaths >= 0]
PopDF = PopDF[PopDF.POPESTIMATE2020 > 0]   

# In the population and death files, the states are spelled out. We want the abbreviation.

DeathDF["state_abbr"] = DeathDF['state'].map(us_state_to_abbrev)
PopDF["ST_ABBR"] = PopDF['STNAME'].map(us_state_to_abbrev)

# PANDAS/PS-PANDAS DIFFERENCE - pspd.DataFrame.map() does not support .fillna(). So this is not allowed:
# DeathDF["state_abbr"] = DeathDF['state'].map(us_state_to_abbrev).fillna(DeathDF["state"])

DeathDF = DeathDF.drop(columns=["state"])   # don't need these anymore
PopDF = PopDF.drop(columns=["STNAME"])

# Make the counties upper case.

VaxDF["Recip_County"] = VaxDF["Recip_County"].str.upper()
DeathDF["county"] = DeathDF["county"].str.upper()
PopDF["CTYNAME"] = PopDF["CTYNAME"].str.upper()

# The Vax and Pop files have the word COUNTY on the county name. Trim this for consistency.

VaxDF["Recip_County"] = VaxDF["Recip_County"].str.replace(" COUNTY", '', 1)
PopDF["CTYNAME"] = PopDF["CTYNAME"].str.replace(" COUNTY", '', 1)

# PANDAS/PS-PANDAS DIFFERENCE - In pandas you can strip a suffix as below, but that does not work in pspd.
# VaxDF["Recip_County"] = VaxDF["Recip_County"].str.split(" COUNTY").str[0]

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

print (VaxDF.dtypes, "\n")
print (DeathDF.dtypes, "\n")
print (PopDF.dtypes, "\n")

display (VaxDF.head(10))
display (DeathDF.head(10))
display (PopDF.head(10))



# COMMAND ----------

# Make a DataFrame that will hold all of our results.

AllCountiesAllPeriodsDF = pspd.DataFrame()

# Loop over our whole time period. 

for this_period in range(PERIOD_COUNT):
    
    # Calc the dates we need.
    
    this_period_start = OVERALL_START_DATE + datetime.timedelta(days=(this_period * PERIOD_LENGTH))
    this_period_end = this_period_start + datetime.timedelta(days=PERIOD_LENGTH)
    this_period_vax_start = this_period_start - datetime.timedelta(days=VAX_BACKDATE)
    this_period_vax_end = this_period_end - datetime.timedelta(days=VAX_BACKDATE)
    
    # PANDAS/PS-PANDAS DIFFERENCE: In pandas you can use pd.offsets.Day() to adjust a datetime. In pspd you must re-code this to something like the above.

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
    
print (AllCountiesAllPeriodsDF.dtypes, "\n")

display (AllCountiesAllPeriodsDF)


# COMMAND ----------

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

# PANDAS/PS-PANDAS DIFFERENCE: In the pandas version of this program, the code writes the DataFrame to a text file on the local computer, then re-opens that
# file to do the analysis. The reason is that the file is useful for sanity checking of the data and to perform other analysis that you might think of later. 
# But writing to a local file does not work with pspd.to_csv(). The output goes to a *set* of files with strange names under the Databricks file 
# system on its cloud cluster. It is possible to copy/merge those files to your local machine, but it is big PITA. 
# So under pspd it is easier to keep the data in its DataFrame and go directly to the analysis phase.

print (AllCountiesAllPeriodsDF.dtypes, "\n")

display (AllCountiesAllPeriodsDF)


# COMMAND ----------

# Compute correlation between death ranking and vax rankings. 

county_rows = AllCountiesAllPeriodsDF.shape[0]

FullVaxCorr = (AllCountiesAllPeriodsDF["DeathsPer100k"].corr(AllCountiesAllPeriodsDF["FullVaxPer100"], method="spearman")).round(3)
OnePlusVaxCorr = (AllCountiesAllPeriodsDF["DeathsPer100k"].corr(AllCountiesAllPeriodsDF["OnePlusVaxPer100"], method="spearman")).round(3)

print ("Full vax to death correlation (Spearman) for all US counties over", county_rows, "data points is", FullVaxCorr, "\n")
print ("1+ vax to death correlation (Spearman) for all US counties over", county_rows, "data points is ", OnePlusVaxCorr, "\n")

# PANDAS/PS-PANDAS DIFFERENCE: You can ignore warnings about Arrow optimization, as long as it appears you are getting the right answers, which I did.


# COMMAND ----------

AllCountiesAllPeriodsDF.plot.scatter(x="FullVaxPer100", y="DeathsPer100k", title="US Counties -- Full Vax per 100 vs Deaths per 100k -- " + str(county_rows) + " data points" )


# COMMAND ----------

AllCountiesAllPeriodsDF.plot.scatter(x="OnePlusVaxPer100", y="DeathsPer100k", title="US Counties -- 1+ Vax per 100 vs Deaths per 100k -- " + str(county_rows) + " data points")


# COMMAND ----------

# PANDAS/PS-PANDAS DIFFERENCE: With pspd doing a histogram on one column out of a full DataFrame does not seem to be working (or at least not in my case). 
# The line shown yields the following error.

# AllCountiesAllPeriodsDF.plot.hist(column="FullVaxPer100", bins=20)  
# cannot resolve 'least(min(EndDate), min(EndDeaths), min(`STATE-COUNTY`), min(StartDate), min(StartDeaths), min(POPESTIMATE2020), 
# min(ST_ABBR), min(VaxStartDate), min(Series_Complete_Yes_Start), min(Administered_Dose1_Recip_Start), min(VaxEndDate), min(Series_Complete_Yes_End),
# min(Administered_Dose1_Recip_End), min(Deaths), min(Series_Complete_Yes_Mid), min(Administered_Dose1_Recip_Mid), min(FullVaxPer100), 
# min(OnePlusVaxPer100), min(DeathsPer100k))' due to data type mismatch: The expressions should all have the same type, got LEAST(timestamp, bigint, 
# string, timestamp, bigint, bigint, string, timestamp, bigint, bigint, timestamp, bigint, bigint, bigint, double, double, double, double, double).;

# As a workaround, create a one-column DataFrame that selects just the field you want, then make a histogram of that.

# COMMAND ----------

OneColumnDF = AllCountiesAllPeriodsDF["FullVaxPer100"]
print("US Counties -- FullVaxPer100 -- " + str(county_rows) + " data points")  # because titles don't work for hist
OneColumnDF.plot.hist(bins=20, title="US Counties -- FullVaxPer100 -- " + str(county_rows) + " data points")

#PANDAS/PS-PANDAS DIFFERENCE: In pspd the title option does not work for histogram. 

# COMMAND ----------

OneColumnDF = AllCountiesAllPeriodsDF["OnePlusVaxPer100"]
print ("US Counties -- OnePlusVaxPer100 -- " + str(county_rows) + " data points")
OneColumnDF.plot.hist(bins=20, title="US Counties -- OnePlusVaxPer100 -- " + str(county_rows) + " data points")


# COMMAND ----------

OneColumnDF = AllCountiesAllPeriodsDF["DeathsPer100k"]
print ("US Counties -- DeathsPer100k -- " + str(county_rows) + " data points")
OneColumnDF.plot.hist(bins=20, title="US Counties -- DeathsPer100k -- " + str(county_rows) + " data points")

# COMMAND ----------

OneColumnDF = (AllCountiesAllPeriodsDF[AllCountiesAllPeriodsDF.DeathsPer100k <= 50])["DeathsPer100k"]
rows = OneColumnDF.shape[0]
print ("US Counties -- DeathsPer100k (<=50) -- " + str(rows) + " data points")
OneColumnDF.plot.hist(bins=20, range=[0, 50], title="US Counties -- DeathsPer100k (<50) -- " + str(rows) + " data points")

#PANDAS/PS-PANDAS DIFFERENCE: In pspd the range option does not work for histograms. As a workaround, select the range first.
