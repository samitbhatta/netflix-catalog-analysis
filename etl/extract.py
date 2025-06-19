from pyspark.sql import SparkSession

def create_spark_session():
    """Create a SparkSession."""
    spark = SparkSession.builder \
        .appName("NetflixPipeline") \
        .getOrCreate()
    return spark

def extract_data(spark):
    """Read CSV and JSON Netflix data."""
    df_csv = spark.read.option("header", True).csv("data/netflix_titles.csv")
    df_json = spark.read.option("multiline", True).json("data/new_netflix_data.json")
   
    return df_csv, df_json

 