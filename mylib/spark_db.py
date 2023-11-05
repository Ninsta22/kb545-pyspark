"""
Create a PySpark Session

Upload a CSV into the PySpark Session

Create a Spark DataFrame from the CSV

Execute SQL Query
"""
from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def create_session():
    spark = SparkSession.builder.appName("SQL Use").getOrCreate()
    return spark


def csv_to_spark(sess, filePath):
    df = sess.read.csv(filePath, header=True, sep=";")
    df.createOrReplaceTempView("nba_data")
    return df


def execute_query(sess, sql_query):
    results = sess.sql(sql_query)

    results.show()


def determine_activity(df):
    df = df.withColumn("active_player", F.when(F.col("G") > 41, 1).otherwise(0))
    df.createOrReplaceTempView("nba_data")
    return df
