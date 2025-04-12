from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, year, month, dayofmonth,
    to_date, dayofweek
)
# from urllib.parse import urlparse

# 1. Start Spark session
spark = SparkSession.builder.appName("Bangalore Traffic Dashboard").getOrCreate()

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
df_cleaned = df_cleaned.withColumn("IsWeekend", (col("DayOfWeek").isin(1, 7)).cast("int"))

# 6. Rename 'Area Name' for easier querying
df_cleaned = df_cleaned.withColumnRenamed("Area Name", "Area")
df_cleaned = df_cleaned.withColumnRenamed("Traffic Volume", "TrafficVolume")

# 7. Select needed columns
df_final = df_cleaned.select(
    "Area", "Date", "Day", "Month", "Year", "IsWeekend",
    "TrafficVolume", "Average Speed", "Congestion Level"
)

def save_to_postgres(df):
    db_url = os.getenv("DATABASE_URL")
    if not db_url:
        raise Exception("DATABASE_URL not set")

    db_url = db_url.replace("postgres://", "jdbc:postgresql://")

    df.write \
        .format("jdbc") \
        .option("url", db_url) \
        .option("dbtable", "traffic_data") \
        .option("driver", "org.postgresql.Driver") \
        .mode("overwrite") \
        .save()

save_to_postgres(df_final)
