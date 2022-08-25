
# For an article about how to make a choropleth map on Flourish. 
# For this example, use data from US CDC about polio vaccination coverage for 3 year olds.

# Chuck Connell, summer 2022

import pandas as pd 
from sodapy import Socrata

POLIO_MAP_DATA = "polio_coverage_map.tsv"
VAX_DATASET = "fhky-rtsk"   # CDC code for this dataset

# Get the data, which covers many vaccines and age groups.

cdc_client = Socrata("data.cdc.gov", None)
VaxDF = pd.DataFrame.from_records(cdc_client.get(VAX_DATASET, limit=1000000))

# Select just the data rows we want.

PolioDF = VaxDF.query("vaccine == 'Polio'")
PolioDF = PolioDF.query("dose == '≥3 Doses'")   # this is "fully vaxed" for 3 year olds

PolioDF = PolioDF.query("geography_type == 'States/Local Areas'")
PolioDF = PolioDF.query("geography != 'Puerto Rico'")  # does not work well with intended map

PolioDF = PolioDF.query("year_season == '2018'")   # latest available

PolioDF = PolioDF.query("dimension_type == 'Age'")
PolioDF = PolioDF.query("dimension == '35 Months'")

# Select just the columns we need for mapping.

PolioDF = PolioDF[["geography", "coverage_estimate", "population_sample_size"]]

# Make the data file that becomes the input to the map.

print ("\nWriting map data to " + POLIO_MAP_DATA)
PolioDF.to_csv(POLIO_MAP_DATA, encoding='utf-8', sep='\t', index=False)
