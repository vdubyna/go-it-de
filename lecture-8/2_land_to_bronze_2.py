from pyspark.sql.functions import col
from pyspark.sql.types import IntegerType



# Ця функція рекурсивно очищає DataFrame від рядків з відсутніми значеннями в стовпцях, що є первинними ключами, і перетворює тип даних цих стовпців на Integer.
def recursive_column_clean(df, pk_list):
    if len(pk_list) > 0:
        pk_name = pk_list.pop()  # Вилучає останній елемент зі списку первинних ключів
        df_new = df.dropna(subset=[pk_name]) \
                    .withColumn(pk_name, col(pk_name).cast(IntegerType()))  # Видаляє рядки з відсутніми значеннями та змінює тип даних

        return recursive_column_clean(df_new, pk_list)  # Рекурсивний виклик для наступного первинного ключа
    
    return df  # Повертає оброблений DataFrame, коли всі первинні ключі оброблені

# Ця функція приймає DataFrame з "landing" даними та список первинних ключів, викликає recursive_column_clean, видаляє дублікатні рядки.
def convert_landing_to_bronze(landing_df, pk_list):

    return recursive_column_clean(landing_df, pk_list) \
                                    .distinct()
                                    

# Ця функція розділяє DataFrame на 4 частини для кращої продуктивності та записує DataFrame у таблицю з заданим ім'ям у форматі Delta, використовуючи режим перезапису (overwrite).
def overwrite_with_name(df, name):
    df.repartition(4) \
        .write \
        .format("delta") \
        .mode("overwrite") \
        .saveAsTable(name)
        
# landing to bronze. orders
land_ord = spark.read.table("landing_orders")
land_ord_pk_list = ["CustomerID", "OrderID"]
bronze_ord = convert_landing_to_bronze(land_ord, land_ord_pk_list)
overwrite_with_name(bronze_ord, "bronze_orders")

# landing to bronze. customers
land_cust = spark.read.table("landing_customers")
land_cust_pk_list = ["CustomerID"]
bronze_cust = convert_landing_to_bronze(land_cust, land_cust_pk_list)
overwrite_with_name(bronze_cust, "bronze_customers")
