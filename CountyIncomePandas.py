
# PYSPARK
# from pyspark.sql.functions import col, to_date, regexp_replace, round, max, when

import pandas as pd 
#import numpy as np

# PYSPARK
# Bring in data tables as DataFrames.
# CountyDF = sqlContext.table("county") # from Mass DPH
# CountyIncomeDF = sqlContext.table("countyincome")  # from IndexMundi.com
# CountyPopDF = sqlContext.table("countypop")  # from from IndexMundi.com

# In Pandas we bring in plain text files (csv or tsv) not Databricks file system tables.

path = "C:/Users/chuck/Desktop/COVID Data/Mass DPH/2021-01-03/County.csv"
CountyDF = pd.read_csv(path)

path = "C:/Users/chuck/Desktop/COVID Data/Mass DPH/CountyIncome.tsv"
CountyIncomeDF = pd.read_csv(path, sep='\t', thousands=',')  

path = "C:/Users/chuck/Desktop/COVID Data/Mass DPH/CountyPop.tsv"
CountyPopDF = pd.read_csv(path, sep='\t', thousands=',')

# PYSPARK
# CountyDF = CountyDF\
#.withColumn( "Date", to_date(col("Date"), 'M/d/yyyy'))\
#.withColumn( "NewConfirmedCases", col("New Confirmed Cases").cast("integer"))\
#.withColumn( "TotalConfirmedCases", col("Total Confirmed Cases").cast("integer"))

# Put the data columns into their natural types with better names (no spaces). 

CountyDF["Date"] = pd.to_datetime(CountyDF["Date"], errors='coerce')
CountyDF["NewConfirmedCases"] = pd.to_numeric(CountyDF["New Confirmed Cases"], errors='coerce')
CountyDF["TotalConfirmedCases"] = pd.to_numeric(CountyDF["Total Confirmed Cases"], errors='coerce')
CountyDF["NewProbableAndConfirmedDeaths"] = pd.to_numeric(CountyDF["New Probable and Confirmed Deaths"], errors='coerce')
CountyDF["TotalProbableAndConfirmedDeaths"] = pd.to_numeric(CountyDF["Total Probable and Confirmed Deaths"], errors='coerce')

CountyIncomeDF["AnnualIncome"] = pd.to_numeric(CountyIncomeDF["AnnualIncome"], errors='coerce')

CountyPopDF["Population"] = pd.to_numeric(CountyPopDF["Population"], errors='coerce')

# PYSPARK
#CountyDF = CountyDF\
#.drop("New Confirmed Cases")\
#.drop("Total Confirmed Cases")\

# Drop old column names.

CountyDF = CountyDF.drop(columns=["New Confirmed Cases", "Total Confirmed Cases", "New Probable and Confirmed Deaths", "Total Probable and Confirmed Deaths"])

# PYSPARK
#CountyDF = CountyDF\
#.withColumn("NewProbableAndConfirmedDeaths", when(col("NewProbableAndConfirmedDeaths").isNull(), 0)\
#.otherwise(col("NewProbableAndConfirmedDeaths")))\
#.withColumn("TotalProbableAndConfirmedDeaths", when(col("TotalProbableAndConfirmedDeaths").isNull(), 0)\
#.otherwise(col("TotalProbableAndConfirmedDeaths")))

# Cases and deaths have a null where there should be a zero. Fix this.

CountyDF["NewConfirmedCases"].fillna(0, inplace=True)
CountyDF["TotalConfirmedCases"].fillna(0, inplace=True)
CountyDF["NewProbableAndConfirmedDeaths"].fillna(0, inplace=True)
CountyDF["TotalProbableAndConfirmedDeaths"].fillna(0, inplace=True)

# PYSPARK
#CountyDF = CountyDF.filter((col("County") != 'Unknown') & (col("County") != 'Dukes and Nantucket'))

# Get rid of the county rows for "Unknown" and "Dukes and Nantucket". These are not real counties, so we can't get
# any demographic data for them.

CountyDF = CountyDF[CountyDF.County !="Unknown"]
CountyDF = CountyDF[CountyDF.County !="Dukes and Nantucket"]

# PYSPARK
#display (CountyDF)
#display (CountyIncomeDF)
#display (CountyPopDF)

# Look at the data for sanity check.

print ("\nCounty CASES AND DEATHS...\n")
print (CountyDF.dtypes)
print (CountyDF.head(5))
print (CountyDF.describe())

print ("\nCounty INCOME...\n")
print (CountyIncomeDF.dtypes)
print (CountyIncomeDF.head(5))
print (CountyIncomeDF.describe())

print ("\nCounty POPULATION...\n")
print (CountyPopDF.dtypes)
print (CountyPopDF.head(5))
print (CountyPopDF.describe())

# PYSPARK
#CountyDF = CountyDF.join(CountyIncomeDF, CountyDF.County == CountyIncomeDF.County, how = "left")\
#.drop(CountyIncomeDF.County)
#CountyDF = CountyDF.join(CountyPopDF, CountyDF.County == CountyPopDF.County, how = "left")\
#.drop(CountyPopDF.County)

# Join the income and population data to the covid data.

AllInfoDF = CountyDF.merge(CountyIncomeDF, how='left', on="County", suffixes=("_left", "_right"))
AllInfoDF = AllInfoDF.merge(CountyPopDF, how='left', on="County", suffixes=("_left", "_right"))

# Some of the fields came out as floats, but the data is really integers, so fix this.

AllInfoDF["NewConfirmedCases"] = AllInfoDF["NewConfirmedCases"].astype(int)
AllInfoDF["TotalConfirmedCases"] = AllInfoDF["TotalConfirmedCases"].astype(int)
AllInfoDF["NewProbableAndConfirmedDeaths"] = AllInfoDF["NewProbableAndConfirmedDeaths"].astype(int)
AllInfoDF["TotalProbableAndConfirmedDeaths"] = AllInfoDF["TotalProbableAndConfirmedDeaths"].astype(int)

# Look at the data for sanity check.

print ("\nALL FIELDS joined...\n")
print (AllInfoDF.dtypes)
print (AllInfoDF.head(5))
print (AllInfoDF.describe())

#PYSPARK
#CountyDF = CountyDF\
#.withColumn("TotalCasesPer100k", 100000*(col("TotalConfirmedCases")/col("Population")))
#CountyDF = CountyDF\
#.withColumn("TotalCasesPer100k", round(col("TotalCasesPer100k"),1).cast("float"))

# Add a column that shows cases in relation to population.

AllInfoDF["TotalCasesPer100k"] = 100000*(AllInfoDF["TotalConfirmedCases"]/AllInfoDF["Population"])

# PYSPARK
#CountyDF = CountyDF\
#.withColumn("TotalDeathsPer100k", 100000*(col("TotalProbableAndConfirmedDeaths")/col("Population")))
#CountyDF = CountyDF\
#.withColumn("TotalDeathsPer100k", round(col("TotalDeathsPer100k"),1).cast("float"))

# Add a column that shows deaths in relation to population.

AllInfoDF["TotalDeathsPer100k"] = 100000*(AllInfoDF["TotalProbableAndConfirmedDeaths"]/AllInfoDF["Population"])

# Round these values.
AllInfoDF = AllInfoDF.round({'TotalDeathsPer100k': 1, 'TotalCasesPer100k': 1})

# Sanity check.
print ("\nALL INFO...\n")
print (AllInfoDF.dtypes)
print (AllInfoDF.head(5))
print (AllInfoDF.describe())

#PYSPARK
#LastDate = CountyDF.select(max("Date")).collect()[0]["max(Date)"]

# Find the last date in the data set.

LastDate = AllInfoDF["Date"].max()
print ("\nLast date is ", LastDate)

#PYSPARK
#CountyTotalsIncomeDF = CountyDF\
#.filter(col("Date") == LastDate)\
#.select("Date","County","AnnualIncome","TotalDeathsPer100k","TotalCasesPer100k")

# We only need the rows with the last date, and only some of the columns. Make a new DataFrame from this subset.
# (We assume that each county has the same "last reporting date".)
FinalDF = AllInfoDF[AllInfoDF.Date == LastDate]
FinalDF = FinalDF[["Date","County","AnnualIncome","TotalCasesPer100k","TotalDeathsPer100k"]]

# Sanity check.
print ("\nFINAL INFO...\n")
print (FinalDF.dtypes)
print (FinalDF.head(5))
print (FinalDF.describe())

#PYSPARK
#CasesCorr = CountyTotalsIncome.corr("AnnualIncome", "TotalCasesPer100k")
#DeathsCorr = CountyTotalsIncome.corr("AnnualIncome", "TotalDeathsPer100k")

# Compute the Pearson correlation between annual income and cases/deaths. 

CasesCorr = FinalDF["AnnualIncome"].corr(FinalDF["TotalCasesPer100k"], method="pearson").round(3)
DeathsCorr = FinalDF["AnnualIncome"].corr(FinalDF["TotalDeathsPer100k"], method="pearson").round(3)

print ("\nIncome-Case Pearson correlation is ", CasesCorr)
print ("\nIncome-Death Pearson correlation is ", DeathsCorr)


# Based on this data, there is a slight positive correlation between income and cases, and a slight negative correlation between income and deaths. 
# Possible explanations:
# - Wealthy people have better access to testing, so more known cases.
# - Wealthy people have better healthcare, so fewer deaths.
