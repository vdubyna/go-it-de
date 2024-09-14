from pyspark.sql.functions import col
from pyspark.sql.types import IntegerType

# Обробляємо таблицю landing_orders
spark.read.table("landing_orders") \
    .distinct() \
    .dropna(subset=["CustomerID", "OrderID"]) \
    .withColumn("CustomerID", col("CustomerID").cast(IntegerType())) \
    .withColumn("OrderID", col("OrderID").cast(IntegerType())) \
    .repartition(4) \
    .write \
        .format("delta") \
        .mode("overwrite") \
        .saveAsTable("bronze_orders")

# Обробляємо таблицю landing_customers
spark.read.table("landing_customers") \
    .distinct() \
    .dropna(subset=["CustomerID"]) \
    .withColumn("CustomerID", col("CustomerID").cast(IntegerType())) \
    .repartition(4) \
    .write \
        .format("delta") \
        .mode("overwrite") \
        .saveAsTable("bronze_customers")
