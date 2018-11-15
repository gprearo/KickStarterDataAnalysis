from pyspark import SparkContext, SQLContext
from pyspark.sql import Row


def get_country_count(data):
    # Step 1: create tuples with the country and the number 1
    country = data.map(lambda line: (line[11], 1))

    # Step 2: add the numbers for the same country
    country_cnt = country.reduceByKey(lambda val1, val2: val1 + val2)

    # Step 3: sorting (not necessary)
    return country_cnt.sortBy(lambda item: item[1], ascending=False)


def get_category_sum(data):
    # Step 1: create tuples with the category, number 1, and the pledged value of the successful projects
    category = data.filter(lambda lst: lst[9] == "successful").map(lambda lst: (lst[2],
                                                                                [1, float(lst[12].replace("\"", ""))]))

    # Step 2: add the number and pledged values for each category
    category_sum = category.reduceByKey(lambda lst1, lst2: [lst1[0] + lst2[0], lst1[1] + lst2[1]])

    # Step 3: sort by number of projects
    return category_sum.sortBy(lambda item: item[1][0], ascending=False)


def get_us_above_goal(data):
    # Step 1: create tuples with the difference between goal and the pledged value of successful US projects
    us_success = data.filter(lambda lst: lst[11] == "US" and lst[9] == "successful")\
                        .map(lambda lst: float(lst[12]) - float(lst[6]))

    # Step 2: sum the differences
    return us_success.reduce(lambda val1, val2: val1 + val2)


sc = SparkContext()
sqlc = SQLContext(sc)

data_file = sc.textFile('ks-projects-201612_modified.csv')

header = data_file.first()
data_file = data_file.filter(lambda line: line != header)

data = data_file.map(lambda line: line.split(","))

# Header:
# 'ID ', 'name ', 'category ', 'main_category ', 'currency ', 'deadline ', 'goal ', 'launched ', 'pledged ', 'state '
# , 'backers ', 'country ', 'usd pledged '

# Get the number of projects of each country and write the result into a csv file
country_count = get_country_count(data).map(lambda x: Row(country=x[0], count=int(x[1])))
schema_country = sqlc.createDataFrame(country_count)
schema_country.toPandas().to_csv("country_count.csv", columns=["country", "count"], index=False, quotechar='\'')

# Get the 3 pledged values fo the 3 categories with the most successful projects
category_sum = get_category_sum(data)
top3_categories = category_sum.take(3)

print("\nCategorias com mais projetos aprovado:")
print("{:<20} {:>20} {:>20}".format("Categoria", "Projetos Aprovados", "Valor arrecadado"))

for item in list(top3_categories):
    print("{:<20} {:>20} {:>20.2f}".format(item[0], item[1][0], item[1][1]))

# Get the value above goal for US's successful projects
print("\nO valor arrecadado acima do objetivo para os projetos bem sucedidos do país US é {:.2f} dólares"\
      .format(get_us_above_goal(data)))
