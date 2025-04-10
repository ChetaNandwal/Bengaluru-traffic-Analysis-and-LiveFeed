from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, year, month, dayofmonth,
    to_date, dayofweek
)

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

# 8. Save to PostgreSQL
def save_to_postgres(df):
    df.write \
        .format("jdbc") \
        .option("url", "jdbc:postgresql://localhost:5432/DATABASE_NAME") \
        .option("dbtable", "TABLE_NAME") \
        .option("user", "postgres") \
        .option("password", "DBPASS") \
        .option("driver", "org.postgresql.Driver") \
        .mode("overwrite") \
        .save()

save_to_postgres(df_final)
