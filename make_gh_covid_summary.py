
# Computes some basic statistics from the Global.health covid dataset, then puts them in a 
# spreadsheet for easy scanning. 

# The spreadsheet is a sanity check on the g.h covid data. For each country: number of case/list rows,
# number of various case outcomes, date of latest case in file. 

# Chuck Connell, November 2022

import os
import fnmatch
import pandas as pd 

# 1 means to read all the input data. Set to 100 or 1000 for faster close approximations. 
# Just make sure to multiply results by this number.

SAMPLE_SKIP = 1

# Directory that holds all the g.h CSV country files.  (input)

GH_COUNTRY_DIR = "/Users/chuck/Desktop/COVID Programming/Global.Health/gh_2023-03-20/country/"

# The spreadsheet file we create. (output)

GH_SUMMARY_FILE = "gh_summary.tsv"

#  EXECUTABLE CODE

# Show the skip count, if any.

print ("\nSkip count: " + str(SAMPLE_SKIP))

# Get all the file objects in the input directory.

files = os.scandir(GH_COUNTRY_DIR)

# Make a dataframe that will hold the output.

summary_DF = pd.DataFrame(data=None, dtype=str, columns=["file", "latest_case", "rows", "hospital_yes", "icu_yes", "outcome_admit", "outcome_icu", "outcome_death"])

# Loop over all the files in the input directory.
 
for f in files:

    # Throw out files we don't want.
    
    if not (f.is_file()): continue
    if not (fnmatch.fnmatch(f, "*.csv")): continue

    # Get  and show name of this file.
    
    print ("\nWorking on: " + f.name)
    GH_PATH = GH_COUNTRY_DIR + f.name

    # Find the number of rows and last date.
    
    gh_DF = pd.read_csv(GH_PATH, sep=',', header=0, dtype=str, skiprows=(lambda i : i % SAMPLE_SKIP != 0))
    rows = str(gh_DF.shape[0])
    latest = str(gh_DF["events.confirmed.date"].max())

    # Lowercase the fields are care about, just to prevent upper/lower issues

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
    
    # Append info for this file to the overall output spreadsheet.
    
    summary_DF = summary_DF.append({"file":f.name, "latest_case":latest, "rows":rows, "hospital_yes":hospital_yes, "icu_yes":icu_yes, "outcome_admit":outcome_admit, "outcome_icu":outcome_icu, "outcome_death":outcome_death}, ignore_index=True)
    
# Done with file loop. Close the file list.

files.close()

# Write the spreadsheet.

print ("\nWriting summary data to " + GH_SUMMARY_FILE)
summary_DF.to_csv(GH_SUMMARY_FILE, encoding='utf-8', sep='\t', index=False)


