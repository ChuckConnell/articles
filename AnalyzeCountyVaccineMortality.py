
import pandas as pd 

VAX_COUNTY_FILE = "AllCountiesAllPeriods.tsv"

# Get the source data. 

path = VAX_COUNTY_FILE
CountyVaxMortalityDF = pd.read_csv(path, sep='\t', header='infer')

county_rows = CountyVaxMortalityDF.shape[0]

# Compute correlation between death ranking and vax rankings. We will do both vax rankings because it is easy and might be interesting.

FullVaxCorr = (CountyVaxMortalityDF["DeathsPer100k"].corr(CountyVaxMortalityDF["FullVaxPer100"], method="spearman")).round(3)
OnePlusVaxCorr = (CountyVaxMortalityDF["DeathsPer100k"].corr(CountyVaxMortalityDF["OnePlusVaxPer100"], method="spearman")).round(3)

print ("Full vax to death correlation (Spearman) for all US counties over", county_rows, "data points is", FullVaxCorr, "\n")
print ("1+ vax to death correlation (Spearman) for all US counties over", county_rows, "data points is ", OnePlusVaxCorr, "\n")

# Show some data visualizations.

CountyVaxMortalityDF.plot.scatter(x="FullVaxPer100", y="DeathsPer100k", title="US Counties -- Full Vax per 100 vs Deaths per 100k -- " + str(county_rows) + " data points" )
CountyVaxMortalityDF.plot.scatter(x="OnePlusVaxPer100", y="DeathsPer100k", title="US Counties -- 1+ Vax per 100 vs Deaths per 100k -- " + str(county_rows) + " data points")

# Histograms of mortality and vax %s

CountyVaxMortalityDF.hist(column="FullVaxPer100", bins=10)
CountyVaxMortalityDF.hist(column="OnePlusVaxPer100", bins=10)
CountyVaxMortalityDF.hist(column="DeathsPer100k", bins=10)
CountyVaxMortalityDF.hist(column="DeathsPer100k", bins=10, range=[0, 50])


