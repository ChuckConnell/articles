
# Computes some basic statistics from the Global.health covid dataset, and
# write a file that is both a spreadsheet and input to mapping.

# Chuck Connell

import os
import fnmatch
import pandas as pd 
import datetime as dt

WHO_COVID_FILE = "https://covid19.who.int/WHO-COVID-19-global-data.csv"
GH_COUNTRY_DIR = "/Users/chuck/Desktop/COVID Programming/Global.Health/gh_2023-04-04/country/"
GH_SUMMARY_FILE = "gh_summary.tsv"

# Tell user what set of G.h files we are looking at. 
# For now, this must be a manual download because the G.h API only supports "one country at a time". We want all the countries, in separate files. 
# The G.h CSV files are named with each country's ISO-2 country code.

print ("\nWorking in directory: " + GH_COUNTRY_DIR)

# Get WHO data about cases and deaths per country. 
# Throw out colummns we don't need. Clarify some field naming. Keep just the latest data.

who_DF = pd.read_csv(WHO_COVID_FILE, sep=',', header='infer', dtype=str)

who_DF = who_DF[["Date_reported", "Country_code", "Cumulative_cases", "Cumulative_deaths"]] 
who_DF = who_DF.rename({"Date_reported":"who_Date_reported", "Cumulative_cases":"who_Cumulative_cases", "Cumulative_deaths":"who_Cumulative_deaths"}, axis='columns')

who_DF["who_Date_reported"] = pd.to_datetime(who_DF["who_Date_reported"], infer_datetime_format=True)
latest_who = who_DF["who_Date_reported"].max()
who_DF = who_DF.loc[who_DF["who_Date_reported"] == latest_who]

# Make a dataframe that will hold the output.

summary_DF = pd.DataFrame()

# Loop over all the files in the input directory.
 
files = os.scandir(GH_COUNTRY_DIR)

for f in files:

    # Throw out files we don't want.
    
    if not (f.is_file()): continue
    if not (fnmatch.fnmatch(f, "*.gz")): continue

    # Get the filename and country.
    
    gh_path = GH_COUNTRY_DIR + f.name   # each is a *.gz file
    fname, fext = os.path.splitext(f.name) # find *.csv within gz
    country, fext = os.path.splitext(fname) # strip ".csv" to get ISO country code
    country = country.upper()  
    print ("\nWorking on: " + country)

    # Find the number of rows and last date.
    
    gh_DF = pd.read_csv(gh_path, sep=',', header='infer', dtype=str)
    gh_rows = gh_DF.shape[0]

    gh_DF["events.confirmed.date"] = pd.to_datetime(gh_DF["events.confirmed.date"], infer_datetime_format=True)
    gh_latest = gh_DF["events.confirmed.date"].max()

    # Lowercase the fields we care about, just to prevent upper/lower issues

    gh_DF["events.outcome.value"] = gh_DF["events.outcome.value"].str.lower()
    gh_DF["events.hospitalAdmission.value"] = gh_DF["events.hospitalAdmission.value"].str.lower()
    gh_DF["events.icuAdmission.value"] = gh_DF["events.icuAdmission.value"].str.lower()

    # Extract the fields we want for this country, getting value subtotals, and convert to Python dict.
    
    outcomes = gh_DF["events.outcome.value"].value_counts().to_dict()
    hospitals = gh_DF["events.hospitalAdmission.value"].value_counts().to_dict()
    icus = gh_DF["events.icuAdmission.value"].value_counts().to_dict()

    # Get counts of known outcomes.
    
    outcome_admit = outcomes.get("hospitaladmission", 0)
    outcome_icu = outcomes.get("icuadmission", 0)
    outcome_death = outcomes.get("death", 0)

    hospital_yes = hospitals.get("yes", 0)
    icu_yes = icus.get("yes", 0)
    
    # Add info for this file to the overall output spreadsheet.

    this_country_DF = pd.DataFrame({"country":[country], "latest":[gh_latest], "cases":[gh_rows], "hospital_yes":[hospital_yes], "icu_yes":[icu_yes], "outcome_admit":[outcome_admit], "outcome_icu":[outcome_icu], "outcome_death":[outcome_death]})
    summary_DF = pd.concat([summary_DF, this_country_DF])
    
# Done with file loop. Close the file list.

files.close()

# Join the G.h data with the WHO data.

summary_DF = summary_DF.merge(who_DF, how='left', left_on="country", right_on = "Country_code")
summary_DF = summary_DF.drop(columns=["Country_code"])

# Calc the percent of WHO cases that G.h has for each country

summary_DF["cases"] = summary_DF["cases"].fillna(0).astype(int)
summary_DF["who_Cumulative_cases"] = summary_DF["who_Cumulative_cases"].fillna(0).astype(int)

summary_DF["case_pct"] = ((summary_DF["cases"] / summary_DF["who_Cumulative_cases"]) * 100).round()

# Calc pct of WHO deaths that G.h has for each country.

summary_DF["outcome_death"] = summary_DF["outcome_death"].fillna(0).astype(int)
summary_DF["who_Cumulative_deaths"] = summary_DF["who_Cumulative_deaths"].fillna(0).astype(int)

summary_DF["death_pct"] = ((summary_DF["outcome_death"] / summary_DF["who_Cumulative_deaths"]) * 100).round()

# Calc overall mortality percent from G.h cases for each country.

summary_DF["mortality"] = ((summary_DF["outcome_death"] / summary_DF["cases"]) * 100).round(1)

# How old is the latest G.h data for each country?

today = dt.date.today()
summary_DF["data_age"] = (today - summary_DF["latest"].dt.date).dt.days

# Create a column of "age category" which helps with color coding the map.

summary_DF["data_cat"] = "1y+"
summary_DF.loc[summary_DF["data_age"] < 360, "data_cat"] = "<360d"
summary_DF.loc[summary_DF["data_age"] < 180, "data_cat"] = "<180d"
summary_DF.loc[summary_DF["data_age"] < 90, "data_cat"] = "<90d"
summary_DF.loc[summary_DF["data_age"] < 30, "data_cat"] = "<30d"
summary_DF.loc[summary_DF["data_age"] < 15, "data_cat"] = "<15d"

# Write the spreadsheet / map file.

print ("\nWriting summary data to " + GH_SUMMARY_FILE)
summary_DF.to_csv(GH_SUMMARY_FILE, encoding='utf-8', sep='\t', index=False)

