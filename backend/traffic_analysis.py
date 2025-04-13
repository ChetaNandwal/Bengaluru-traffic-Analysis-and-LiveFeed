from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, year, month, dayofmonth,
    to_date, dayofweek
)
import os
from dotenv import load_dotenv
load_dotenv()

from urllib.parse import urlparse

# 1. Start Spark session
spark = SparkSession.builder.appName("Bangalore Traffic Dashboard").config("spark.jars","/Users/chetannandwal/Desktop/projects/bengaluru_traffic_analysis and live feed/backend/postgresql-42.7.5.jar").getOrCreate()

# 2. Read dataset
df = spark.read.option("header", True).option("inferSchema", True).csv("Banglore_traffic_Dataset.csv")

# 3. Drop rows with nulls in essential columns
essential_cols = ["Date", "Area Name", "Traffic Volume"]
df_cleaned = df.dropna(subset=essential_cols)

# 4. Convert date column to proper format
df_cleaned = df_cleaned.withColumn("Date", to_date(col("Date"), "yyyy-MM-dd"))

# 5. Extract date features
df_cleaned = df_cleaned.withColumn("Day", dayofmonth("Date"))
df_cleaned = df_cleaned.withColumn("Month", month("Date"))
df_cleaned = df_cleaned.withColumn("Year", year("Date"))
df_cleaned = df_cleaned.withColumn("DayOfWeek", dayofweek("Date"))
df_cleaned = df_cleaned.withColumn("is_weekend", (col("DayOfWeek").isin(1, 7)).cast("boolean"))

# 6. Rename 'Area Name' for easier querying
# 6. Rename columns for easier querying
df_cleaned = df_cleaned.withColumnRenamed("Area Name", "Area")
df_cleaned = df_cleaned.withColumnRenamed("Traffic Volume", "TrafficVolume")
df_cleaned = df_cleaned.withColumnRenamed("Average Speed", "average_speed")      # ✅ NEW
df_cleaned = df_cleaned.withColumnRenamed("Congestion Level", "congestion_level") # ✅ NEW


# 7. Select needed columns
df_final = df_cleaned.select(
    "Area", "Date", "Day", "Month", "Year", "is_weekend",
    "TrafficVolume", "average_speed", "congestion_level"
)


def save_to_postgres(df):
    db_url = os.getenv("DATABASE_URL")
    if not db_url:
        raise Exception("DATABASE_URL not set")

    parsed = urlparse(db_url)

    jdbc_url = f"jdbc:postgresql://{parsed.hostname}:{parsed.port}{parsed.path}"
    user = parsed.username
    password = parsed.password

    df.write \
        .format("jdbc") \
        .option("url", jdbc_url) \
        .option("dbtable", "traffic_data") \
        .option("user", user) \
        .option("password", password) \
        .option("driver", "org.postgresql.Driver") \
        .mode("append") \
        .save()

save_to_postgres(df_final)
