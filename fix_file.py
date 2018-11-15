# -*- coding: utf-8 -*-

from pyspark import SparkContext, SQLContext
from pyspark.sql.types import StructType, StructField, StringType


def fix_title(line, field_cnt):
    """
    The project name on the dataset may contain comma, which results in extra fields
    This function concatenates the fields next to the name until the appropriated number of fields is reached,
        replacing the commas with semicolons

    :param line: list with the values of one line of the dataset
    :param field_cnt: number of the dataset fields
    :return: returns the list semicolons instead of commas on field "name"
    """
    title_index = 1
    extra_fields_cnt = len(line) - field_cnt

    if extra_fields_cnt == 0:
        return line
    else:
        # Replaces the comma on titles with semicolon
        extra_fields = line[title_index:title_index+extra_fields_cnt+1]
        s = ';'.join(extra_fields)
        return line[0:title_index] + [s] + line[title_index+extra_fields_cnt+1:]


sc = SparkContext()
sqlc = SQLContext(sc)

data_file = sc.textFile('ks-projects-201612.csv')

# Split fields and remove the blank ones
data = data_file.map(lambda line: list(filter(None, line.split(","))))

# Gets the header fo the file
header = data.first()

# Get field count from header
field_cnt = len(header)


# Fix titles with comma
modified_data = data.map(lambda line: fix_title(line, field_cnt)).filter(lambda line: line != header)

# Convert RDD to DataFrame
schema = StructType([StructField(str(item), StringType(), True) for item in header])
df = sqlc.createDataFrame(modified_data, schema)

# Convert to pandas to write the DataFrame on a CSV file
df.toPandas().to_csv("ks-projects-201612_modified.csv", index=False, quotechar="\'", columns=header)
