from pyspark import SparkContext, SQLContext
from pyspark.sql import Row


def get_country_count(data):
    # Step 1: create a tuples with the country and the number 1
    country = data.map(lambda line: (line[11], 1))

    # Step 2: add the numbers for the same country
    country_cnt = country.reduceByKey(lambda val1, val2: val1 + val2)

    # Step 3: Sorting (not necessary)
    return country_cnt.sortBy(lambda item: item[1], ascending=False)



sc = SparkContext()
sqlc = SQLContext(sc)

data_file = sc.textFile('ks-projects-201612_modified.csv')

header = data_file.first()
data_file = data_file.filter(lambda line: line != header)

data = data_file.map(lambda line: line.split(","))

# 'ID ', 'name ', 'category ', 'main_category ', 'currency ', 'deadline ', 'goal ', 'launched ', 'pledged ', 'state '
# , 'backers ', 'country ', 'usd pledged '

# Get the number of projects of each country and write the result into a csv file
country_count = get_country_count(data).map(lambda x: Row(country=x[0], count=int(x[1])))
schema_country = sqlc.createDataFrame(country_count)
schema_country.toPandas().to_csv("country_count.csv", columns=["country", "count"], index=False, quotechar='\'')
