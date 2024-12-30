from pyspark.sql import SparkSession
from pyspark.sql.functions import col, isnan, when, count

spark = SparkSession.builder.appName("Handle Missing Values").getOrCreate()

input_path = "s3a://bjahagir-processed-data/data_overview.parquet"
print(f"Loading data from {input_path}...")
df = spark.read.parquet(input_path)

print("Identifying missing values in the DataFrame...")
missing_values = df.select([count(when(isnan(c) | col(c).isNull(), c)).alias(c) for c in df.columns])
missing_values.show()

print("Dropping rows with missing values...")
df_cleaned = df.dropna()
print(f"Number of rows after cleaning: {df_cleaned.count()}")

output_path = "s3a://bjahagir-processed-data/cleaned_data.parquet"
print(f"Saving cleaned data to {output_path}...")
df_cleaned.write.parquet(output_path, mode="overwrite")
print("Cleaned data saved successfully!")
