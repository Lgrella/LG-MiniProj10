"""
PySpark functions
"""

import os
import requests
from pyspark.sql import SparkSession
from pyspark.sql.functions import when, col

from pyspark.sql.types import (
     StructType, 
     StructField, 
     IntegerType, 
     StringType
)

LOG_FILE = "minilab10_output.md"


def log_output(operation, output, query=None):
    """adds to a markdown file"""
    with open(LOG_FILE, "a") as file:
        file.write(f"{operation}:\n\n")
        if query: 
            file.write(f"{query}\n")
        file.write(output)
        file.write("\n\n-----------------------------------------------------------------\
        \n-----------------------------------------------------------------\n\n")

def start_spark(appName):
    spark = SparkSession.builder.appName(appName).getOrCreate()
    return spark

def end_spark(spark):
    spark.stop()
    return "stopped spark session"

def extract(
    url="""
   https://github.com/fivethirtyeight/data/blob/master/voter-registration/new-voter-registrations.csv?raw=true
    """,
    file_path="data/new-voter-registrations.csv",
    directory="data",
):
    """Extract a url to a file path"""
    if not os.path.exists(directory):
        os.makedirs(directory)
    with requests.get(url) as r:
        with open(file_path, "wb") as f:
            f.write(r.content)
    return file_path

def load_data(spark, data="data/new-voter-registrations.csv", name="NewVoters"):
    """load data"""
    # data preprocessing by setting schema
    schema = StructType([
        StructField("JURISDICTION", StringType(), True),
        StructField("YEAR", IntegerType(), True),
        StructField("MONTH", StringType(), True),
        StructField("NEW_VOTERS", IntegerType(), True)
    ])

    df = spark.read.option("header", "true").schema(schema).csv(data)

    log_output("Load Data", df.limit(5).toPandas().to_markdown())

    return df


def query(spark, df, query, name): 
    """queries using spark sql"""
    df = df.createOrReplaceTempView(name)

    log_output("Query Data", spark.sql(query).toPandas().to_markdown(), query)

    return spark.sql(query).show()

def describe(df):
    summary_stats_str = df.describe().toPandas().to_markdown()
    log_output("Summary Statistics", summary_stats_str)

    return df.describe().show()

def transform(df):
    """does an example transformation on a predefiend dataset"""
    conditions = [
        (col("MONTH") == "Jan")
          | (col("MONTH") == "Feb") 
          | (col("MONTH") == "Mar"),
        (col("MONTH") == "Apr") 
        | (col("MONTH") == "May")
    ]

    categories = ["Winter", "Spring"]

    df = df.withColumn("Season", when(
        conditions[0], categories[0]
        ).when(conditions[1], categories[1]).otherwise("Other"))

    log_output("Transform Data", df.limit(5).toPandas().to_markdown())

    return df.show()