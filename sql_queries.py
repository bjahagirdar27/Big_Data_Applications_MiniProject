from pyspark.sql import SparkSession
import boto3

spark = SparkSession.builder.appName("SQL Queries").getOrCreate()

input_path = "s3a://bjahagir-processed-data/engineered_features.parquet"
print(f"Loading data from {input_path}...")
df = spark.read.parquet(input_path)

df.createOrReplaceTempView("mobile_data")
print("Registered DataFrame as temporary SQL table 'mobile_data'.")

queries = [
    "SELECT AVG(battery_power) AS avg_battery_power FROM mobile_data",
    "SELECT dual_sim, COUNT(*) AS count FROM mobile_data GROUP BY dual_sim",
    "SELECT MAX(ram) AS max_ram, MIN(ram) AS min_ram FROM mobile_data",
    "SELECT price_range, AVG(mobile_wt) AS avg_weight FROM mobile_data GROUP BY price_range",
    "SELECT n_cores, AVG(clock_speed) AS avg_clock_speed FROM mobile_data GROUP BY n_cores"
]

output = []
for query in queries:
    print(f"Executing query: {query}")
    result = spark.sql(query)
    result.show()
    output.append((query, result.collect()))

output_path = "s3a://bjahagir-processed-data/sql_query_results.txt"
print(f"Saving SQL query results to {output_path}...")
with open("/tmp/sql_query_results.txt", "w") as file:
    for query, result in output:
        file.write(f"Query: {query}\n")
        file.write("Results:\n")
        for row in result:
            file.write(f"{row}\n")
        file.write("\n")
s3 = boto3.client("s3")
s3.upload_file("/tmp/sql_query_results.txt", "<bucket-name>", "output/sql_query_results.txt")
print("SQL query results saved successfully!")
