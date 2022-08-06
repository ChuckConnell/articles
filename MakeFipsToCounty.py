# Translate FIPS codes to state and county names.

# The source data is found here: https://www.census.gov/geographies/reference-files/2021/demo/popest/2021-fips.html
# There is a file of FIPS to states, and another of FIPS to counties (which uses the states).
# To start, I manually opened the XLSX files, cleaned them up, then saved as TSV.
## Removed extra header rows other than column names
## Removed parenthetical remarks in column names
## Removed spaces in column names
## Fixed the spelling of Consolidated in that column name

import pandas as pd

from fips_helpers import us_state_to_abbrev

FIPS_COUNTY_OUTPUT_FILE = "fips2county.tsv"

# Open source files from US Census.

path = "~/Desktop/COVID Programming/US Census/state-geocodes-v2021.tsv"
StateDF = pd.read_csv(path, sep='\t', header='infer', dtype=str, encoding='latin-1')

path = "~/Desktop/COVID Programming/US Census/all-geocodes-v2021.tsv"
CountyDF = pd.read_csv(path, sep='\t', header='infer', dtype=str, encoding='latin-1')

# Get rid of states that are not really states and counties that are not counties.

StateDF = StateDF.query("State != '00'")

CountyDF = CountyDF.query("StateCode != '00'")
CountyDF = CountyDF.query("StateCode != '72'")   # Puerto Rico
CountyDF = CountyDF.query("CountyCode != '000'")   
CountyDF = CountyDF.query("PlaceCode == '00000'")
CountyDF = CountyDF.query("CountySubdivisionCode == '00000'")
CountyDF = CountyDF.query("ConsolidatedCityCode == '00000'")

# Clarify some field names.

StateDF = StateDF.rename(columns={"State": "StateFIPS"})
StateDF = StateDF.rename(columns={"Name": "StateName"})

CountyDF = CountyDF.rename(columns={"StateCode": "StateFIPS"})
CountyDF = CountyDF.rename(columns={"CountyCode": "CountyFIPS_3"})  # just the 3 digit county suffix
CountyDF = CountyDF.rename(columns={"AreaName": "CountyName"})

# Keep only fields we need.

StateDF = StateDF[["StateFIPS", "StateName"]]
CountyDF = CountyDF[["StateFIPS", "CountyFIPS_3", "CountyName"]]

# Add state name column to the county list.

CountyDF = CountyDF.merge(StateDF, how="left", on="StateFIPS")

# Drop the string " County" from the counties.

CountyDF["CountyName"] = CountyDF["CountyName"].str.split(" County").str[0]

# Make the full 5-digit FIPS county codes, which is how they are commonly used.

CountyDF["CountyFIPS"] = CountyDF["StateFIPS"] + CountyDF["CountyFIPS_3"]

# Add state abbreviation.

CountyDF["StateAbbr"] = CountyDF["StateName"].str.upper().map(us_state_to_abbrev).fillna(CountyDF["StateName"])

# Add STATE-COUNTY

CountyDF["STATE_COUNTY"] = CountyDF["StateAbbr"] + " | " + CountyDF["CountyName"].str.upper()  

# Write it out.

CountyDF.to_csv(FIPS_COUNTY_OUTPUT_FILE, encoding='utf-8', sep='\t', index=False)


