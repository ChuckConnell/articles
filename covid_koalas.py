# Databricks notebook source
import numpy as np
import pandas as pd
import databricks.koalas as ks
from databricks.koalas.config import set_option, get_option, reset_option

# Some useful Koalas options 
ks.set_option('compute.max_rows', 2000)   # max rows per koalas dataframe
ks.set_option('compute.shortcut_limit' , 1000)   # for small datasets, use pandas under the hood rather than pyspark
ks.set_option('compute.ops_on_diff_frames', False)   # disallow implied joins because they can be slow

# Spark options that are used by Koalas
spark.conf.set('spark.sql.execution.arrow.pyspark.enabled', False)  # Enabling Apache Arrow may help for large data sets
spark.conf.set('spark.sql.execution.arrow.pyspark.fallback.enabled', True)  # if Arrow doesn't work, do the computation anyway


# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC Databricks runs on a cloud cluster, so before accessing your data you must get it to a location where Databricks can see it. The simplest option is to upload data files manually with the Databricks GUI into the Databricks File System (DBFS) and then you can do a normal read_csv() to that location. 
# MAGIC 
# MAGIC To upload local files to DBFS...
# MAGIC - Go to the Databricks GUI
# MAGIC - Data
# MAGIC - Create Table
# MAGIC - DBFS Target Directory = /FileStore/tables/your_new_dir/  (I recommend a new directory so you don't have conflicts with existing files.)
# MAGIC - Drag/drop or browse/select the files you want to upload.
# MAGIC - The Databricks GUI will immediately copy them to DBFS and verify the full pathnames.
# MAGIC - You do not need to create a Databricks table. Your data is already in DBFS.
# MAGIC 
# MAGIC A more advanced option, especially for automatic processing of new data, is to put the files into cloud storage and read from there:
# MAGIC - https://docs.databricks.com/data/data-sources/azure/azure-storage.html
# MAGIC - https://docs.databricks.com/data/data-sources/aws/amazon-s3.html

# COMMAND ----------

# PySpark (and Koalas) are unreliable at inferring datatypes in CSV files. So I prefer to import all data as string, then convert it manually.

CountyCovidKDF = ks.read_csv("/FileStore/tables/chuck1/County.csv", sep=',', header='infer', dtype=str)
CountyCovidKDF.info()
CountyCovidKDF.head(10)

# COMMAND ----------

CountyIncomeKDF = ks.read_csv("/FileStore/tables/chuck1/CountyIncome.tsv", sep='\t', header='infer', dtype=str)
CountyIncomeKDF.info()
CountyIncomeKDF.head(10)

# COMMAND ----------

CountyPopKDF = ks.read_csv("/FileStore/tables/chuck1/CountyPop.tsv", sep='\t', header='infer', dtype=str)
CountyPopKDF.info()
CountyPopKDF.head(10)

# COMMAND ----------

# Put columns into their natural types with better names (no spaces). 

CountyCovidKDF["Date"] = ks.to_datetime(CountyCovidKDF["Date"], errors='coerce') 

CountyCovidKDF["NewConfirmedCases"] = ks.to_numeric(CountyCovidKDF["New Confirmed Cases"]).astype(int)
CountyCovidKDF["TotalConfirmedCases"] = ks.to_numeric(CountyCovidKDF["Total Confirmed Cases"]).astype(int)
CountyCovidKDF["NewProbableAndConfirmedDeaths"] = ks.to_numeric(CountyCovidKDF["New Probable and Confirmed Deaths"]).astype(int)
CountyCovidKDF["TotalProbableAndConfirmedDeaths"] = ks.to_numeric(CountyCovidKDF["Total Probable and Confirmed Deaths"]).astype(int)


# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ks.to_datetime() generates the following warning the first time it is run in a notebook, but works correctly. 
# MAGIC 
# MAGIC `In Python 3.6+ and Spark 3.0+, it is preferred to specify type hints for pandas UDF instead of specifying pandas UDF type,`
# MAGIC `which will be deprecated in the future releases. See SPARK-28264 for more details.`
# MAGIC 
# MAGIC IMO this is an incorrect warning, since to_datetime() appears to have nothing to do with UDFs or the discusssion in SPARK-28264.

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC The errors option is not supported for ks.to_numeric(). So the following line from my original pandas does not work:
# MAGIC 
# MAGIC `CountyCovidKDF["NewConfirmedCases"] = ks.to_numeric(CountyCovidKDF["New Confirmed Cases"], errors='coerce')`

# COMMAND ----------

# The income and populations numbers have commas as thousands separators. Remove them, then cast the type.

CountyIncomeKDF["AnnualIncome"] = CountyIncomeKDF["AnnualIncome"].str.replace(',', '').astype(int)
CountyPopKDF["Population"] = CountyPopKDF["Population"].str.replace(',', '').astype(int)


# COMMAND ----------

# MAGIC %md
# MAGIC Pandas handles thousand-commas invisibly, even without setting a thousands option. 
# MAGIC Koalas does not handle them correctly, instead returning NaN

# COMMAND ----------

# Drop old column names.

CountyCovidKDF = CountyCovidKDF.drop(columns=["New Confirmed Cases", "Total Confirmed Cases", "New Probable and Confirmed Deaths", "Total Probable and Confirmed Deaths"])


# COMMAND ----------

# Cases and deaths have some nulls where there should be a zero. Fix this.

CountyCovidKDF["NewConfirmedCases"].fillna(0, inplace=True)
CountyCovidKDF["TotalConfirmedCases"].fillna(0, inplace=True)
CountyCovidKDF["NewProbableAndConfirmedDeaths"].fillna(0, inplace=True)
CountyCovidKDF["TotalProbableAndConfirmedDeaths"].fillna(0, inplace=True)


# COMMAND ----------

# Get rid of the county rows for "Unknown" and "Dukes and Nantucket". These are not real counties, so we can't get
# any demographic data for them.

CountyCovidKDF = CountyCovidKDF[CountyCovidKDF.County !="Unknown"]
CountyCovidKDF = CountyCovidKDF[CountyCovidKDF.County !="Dukes and Nantucket"]


# COMMAND ----------

# Join the income and population data to the covid data.

AllInfoKDF = CountyCovidKDF.merge(CountyIncomeKDF, how='left', on="County", suffixes=("_left", "_right"))
AllInfoKDF = AllInfoKDF.merge(CountyPopKDF, how='left', on="County", suffixes=("_left", "_right"))


# COMMAND ----------

AllInfoKDF.info()
AllInfoKDF.head(10)

# COMMAND ----------

# Add a column that shows cases in relation to population.

AllInfoKDF["TotalCasesPer100k"] = 100000*(AllInfoKDF["TotalConfirmedCases"]/AllInfoKDF["Population"])

# Add a column that shows deaths in relation to population.

AllInfoKDF["TotalDeathsPer100k"] = 100000*(AllInfoKDF["TotalProbableAndConfirmedDeaths"]/AllInfoKDF["Population"])

# Round these values.
AllInfoKDF = AllInfoKDF.round({'TotalDeathsPer100k': 1, 'TotalCasesPer100k': 1})


# COMMAND ----------

# Find the last date in the data set.

LastDate = AllInfoKDF["Date"].max()
print ("\nLast date is ", LastDate)


# COMMAND ----------

# We only need the rows with the last date, and only some of the columns. Make a new DataFrame from this subset.
# (We assume that each county has the same "last reporting date".)

FinalKDF = AllInfoKDF[AllInfoKDF.Date == LastDate]
FinalKDF = FinalKDF[["Date","County","AnnualIncome","TotalCasesPer100k","TotalDeathsPer100k"]]


# COMMAND ----------

FinalKDF.info()
FinalKDF.head(10)

# COMMAND ----------

# Compute the Pearson correlation between annual income and cases/deaths. 

CasesCorr = FinalKDF["AnnualIncome"].corr(FinalKDF["TotalCasesPer100k"], method="pearson").round(3)
DeathsCorr = FinalKDF["AnnualIncome"].corr(FinalKDF["TotalDeathsPer100k"], method="pearson").round(3)

print ("\nIncome-Case Pearson correlation is ", CasesCorr)
print ("\nIncome-Death Pearson correlation is ", DeathsCorr)


# Based on this data, there is a slight positive correlation between income and cases, and a slight negative correlation between income and deaths. 
# Possible explanations:
# - Wealthy people have better access to testing, so more known cases.
# - Wealthy people have better healthcare, so fewer deaths.


# COMMAND ----------

# MAGIC %md
# MAGIC If you have Arrow optimization enabled, you may see a warning about it during computation of correlation. This is not a problem. The warning just points out that Databricks/Spark is using Apache Arrow where it can to improve preformance. In the case of the correlation result, Arrow cannot be used so Spark falls back to standard computation.
