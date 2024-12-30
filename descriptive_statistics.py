from pyspark.sql import SparkSession
from pyspark.sql.functions import variance, stddev

spark = SparkSession.builder.appName("Descriptive Statistics").getOrCreate()

input_path = "s3a://bjahagir-processed-data/cleaned_data.parquet"
print(f"Loading data from {input_path}...")
df = spark.read.parquet(input_path)

numerical_columns = [col_name for col_name, dtype in df.dtypes if dtype in ["int", "double", "float"]]
print(f"Numerical Columns: {numerical_columns}")
print("Descriptive statistics:")
df.describe(numerical_columns).show()

print("Computing variance and standard deviation for numerical columns...")
stats = {}
for col_name in numerical_columns:
    values = df.select(
        variance(col_name).alias("variance"),
        stddev(col_name).alias("stddev")
    ).collect()
    stats[col_name] = values[0]
    print(f"{col_name}: Variance = {values[0]["variance"]}, Stddev = {values[0]["stddev"]}")

output_path = "s3a://bjahagir-processed-data/descriptive_statistics.txt"
print(f"Saving descriptive statistics to {output_path}...")
with open("/tmp/descriptive_statistics.txt", "w") as file:
    for col_name, values in stats.items():
        file.write(f"{col_name}: Variance = {values["variance"]}, Stddev = {values["stddev"]}\n")
s3.upload_file("/tmp/descriptive_statistics.txt", "<bucket-name>", "output/descriptive_statistics.txt")
print("Descriptive statistics saved successfully!")
