from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, sum, max, min, count

spark = SparkSession.builder.appName("Aggregation").getOrCreate()

input_path = "s3a://bjahagir-processed-data/engineered_features.parquet"
print(f"Loading data from {input_path}...")
df = spark.read.parquet(input_path)

df.printSchema()

# 1. Calculate average battery power by price range
avg_battery_by_price = df.groupBy("price_range").agg(avg("battery_power").alias("avg_battery_power"))
print("Average battery power by price range:")
avg_battery_by_price.show()

# 2. Count the number of mobiles in each price range
count_by_price = df.groupBy("price_range").agg(count("*").alias("mobile_count"))
print("Number of mobiles by price range:")
count_by_price.show()

# 3. Maximum RAM and Minimum RAM by price range
ram_stats_by_price = df.groupBy("price_range").agg(
    max("ram").alias("max_ram"),
    min("ram").alias("min_ram")
)
print("Max and Min RAM by price range:")
ram_stats_by_price.show()

# 4. Total mobile weight for each price range
total_weight_by_price = df.groupBy("price_range").agg(sum("mobile_wt").alias("total_weight"))
print("Total mobile weight by price range:")
total_weight_by_price.show()

# 5. Total cores for each price range
total_weight_by_price = df.groupBy("price_range").agg(sum("n_cores").alias("total_weight"))
print("Total mobile weight by price range:")
total_weight_by_price.show()

output_path = "s3a://bjahagir-processed-data/aggregated_results/"
print(f"Saving aggregated results to {output_path}...")
avg_battery_by_price.write.csv(f"{output_path}avg_battery_by_price", mode="overwrite", header=True)
count_by_price.write.csv(f"{output_path}count_by_price", mode="overwrite", header=True)
ram_stats_by_price.write.csv(f"{output_path}ram_stats_by_price", mode="overwrite", header=True)
total_weight_by_price.write.csv(f"{output_path}total_weight_by_price", mode="overwrite", header=True)

print("Aggregation completed and results saved!")
