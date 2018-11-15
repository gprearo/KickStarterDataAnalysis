from pyspark import SparkContext, SQLContext
from pyspark.sql import Row

sc = SparkContext()
sqlc = SQLContext(sc)

data_file = sc.textFile('ks-projects-201612_modified.csv')

header = data_file.first()
data_file = data_file.filter(lambda line: line != header)

data = data_file.map(lambda line: line.split(","))

# 'ID ', 'name ', 'category ', 'main_category ', 'currency ', 'deadline ', 'goal ', 'launched ', 'pledged ', 'state '
# , 'backers ', 'country ', 'usd pledged '

country = data.map(lambda line: (line[11], 1))
print(country.take(5))

country_cnt = country.reduceByKey(lambda val1, val2: val1 + val2)
print(country_cnt.take(5))

sorted_country_cnt = country_cnt.sortBy(lambda item: item[1], ascending=False)
print(sorted_country_cnt.collect())

srtd_country = sorted_country_cnt.map(lambda x: Row(country=x[0], count=int(x[1])))
schema_country = sqlc.createDataFrame(srtd_country)
schema_country.toPandas().to_csv("country_count.csv", columns=["country", "count"], index=False, quotechar='\'')
