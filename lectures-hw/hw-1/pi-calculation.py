import random
from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("PiCalculation")
    .master("spark://10.0.1.28:7077")
    .getOrCreate()
)

sc = spark.sparkContext

def inside(_):
    x, y = random.random(), random.random()
    return x*x + y*y < 1

num_samples = 1_000_000
count = sc.parallelize(range(num_samples)).filter(inside).count()
print(f"Pi is roughly {4.0 * count / num_samples}")

spark.stop()