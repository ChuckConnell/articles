# Databricks notebook source
# Get functions, types, etc we will need
from pyspark.sql.functions import col, to_date, regexp_replace, round, max, when

# COMMAND ----------

# Bring in data tables as DataFrames.
CountyDF = sqlContext.table("county") # from Mass DPH
CountyIncomeDF = sqlContext.table("countyincome")  # from IndexMundi.com
CountyPopDF = sqlContext.table("countypop")  # from from IndexMundi.com

# COMMAND ----------

# Debugging
#display (CountyDF)
#display (CountyIncomeDF)
#display (CountyPopDF)

# COMMAND ----------

# Put the input strings into their natural types with better names (no spaces). Then drop old column names.

# COVID cases by county
CountyDF = CountyDF\
.withColumn( "Date", to_date(col("Date"), 'M/d/yyyy'))\
.withColumn( "NewConfirmedCases", col("New Confirmed Cases").cast("integer"))\
.withColumn( "TotalConfirmedCases", col("Total Confirmed Cases").cast("integer"))\
.withColumn( "NewProbableAndConfirmedDeaths", col("New Probable and Confirmed Deaths").cast("integer"))\
.withColumn( "TotalProbableAndConfirmedDeaths", col("Total Probable and Confirmed Deaths").cast("integer"))

CountyDF = CountyDF\
.drop("New Confirmed Cases")\
.drop("Total Confirmed Cases")\
.drop("New Probable and Confirmed Deaths")\
.drop("Total Probable and Confirmed Deaths")

# County income data. 
# Note that we must remove the commas (for thousands) from the input string before we convert to int.
CountyIncomeDF = CountyIncomeDF\
.withColumn( "AnnualIncome", regexp_replace(col("AnnualIncome"), ",", "").cast("integer"))

# County population data. 
CountyPopDF = CountyPopDF\
.withColumn( "Population", regexp_replace(col("Population"), ",", "").cast("integer"))

# COMMAND ----------

# The death data has a null where it should have a zero. Fix this.

CountyDF = CountyDF\
.withColumn("NewProbableAndConfirmedDeaths", when(col("NewProbableAndConfirmedDeaths").isNull(), 0)\
.otherwise(col("NewProbableAndConfirmedDeaths")))\
.withColumn("TotalProbableAndConfirmedDeaths", when(col("TotalProbableAndConfirmedDeaths").isNull(), 0)\
.otherwise(col("TotalProbableAndConfirmedDeaths")))

# COMMAND ----------

# Get rid of the county rows for "Unknown" and "Dukes and Nantucket". These are not real couunties, so we can't get
# any demographic data for them.

CountyDF = CountyDF.filter((col("County") != 'Unknown') & (col("County") != 'Dukes and Nantucket'))


# COMMAND ----------

# Debugging
#display (CountyDF)
#display (CountyIncomeDF)
#display (CountyPopDF)

# COMMAND ----------

# Add the demographic data to the MA covid data.

CountyDF = CountyDF.join(CountyIncomeDF, CountyDF.County == CountyIncomeDF.County, how = "left")\
.drop(CountyIncomeDF.County)

CountyDF = CountyDF.join(CountyPopDF, CountyDF.County == CountyPopDF.County, how = "left")\
.drop(CountyPopDF.County)


# COMMAND ----------

# Add a column that shows deaths in relation to population.
CountyDF = CountyDF\
.withColumn("TotalDeathsPer100k", 100000*(col("TotalProbableAndConfirmedDeaths")/col("Population")))

# Clean up the datatype. Not strictly required.
CountyDF = CountyDF\
.withColumn("TotalDeathsPer100k", round(col("TotalDeathsPer100k"),1).cast("float"))

# Add a column that shows cases in relation to population.
CountyDF = CountyDF\
.withColumn("TotalCasesPer100k", 100000*(col("TotalConfirmedCases")/col("Population")))
CountyDF = CountyDF\
.withColumn("TotalCasesPer100k", round(col("TotalCasesPer100k"),1).cast("float"))

# display(CountyDF)

# COMMAND ----------

# Find the last date from the data set.
LastDate = CountyDF.select(max("Date")).collect()[0]["max(Date)"]

LastDate

# COMMAND ----------

# We are going to look at total deaths and cases in relation to income, both per capita. So we only care about the 
# last date, since that has the latest total. And we only care about a subset of the columns. Make a new DataFrame from 
# this subset.

CountyTotalsIncomeDF = CountyDF\
.filter(col("Date") == LastDate)\
.select("Date","County","AnnualIncome","TotalDeathsPer100k","TotalCasesPer100k")

# Show the finished (small) data set
display (CountyTotalsIncomeDF)

#.filter("Date == '2020-12-31'")\  # in case we want to hard-code a date


# COMMAND ----------

# Compute the Pearson correlation between annual income and deaths/cases. 

CountyTotalsIncome = CountyTotalsIncomeDF.sort(col("AnnualIncome").desc())

DeathsCorr = CountyTotalsIncome.corr("AnnualIncome", "TotalDeathsPer100k")
CasesCorr = CountyTotalsIncome.corr("AnnualIncome", "TotalCasesPer100k")

DeathsCorr, CasesCorr

# COMMAND ----------

# Based on this data, there is a slight negative correlation between income and deaths, and a slight positive 
# correlation between income and cases. Possible explanations:
# - Wealthy people have better healthcare, so fewer deaths.
# - Wealthy people have better access to testing, so more known cases.
