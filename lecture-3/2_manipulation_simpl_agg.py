# Які call_type є найбільш популярними (топ-3)?
nuek_df.groupBy('call_type') \
    .count() \
    .orderBy(col('count').desc()) \
    .limit(3) \
    .show()

# Які call_type є найбільш популярними (топ-3)? (з використанням SQL)
spark.sql("""SELECT call_type, COUNT(call_type) as count 
                    FROM nuek_view 
                    GROUP BY call_type 
                    ORDER BY count DESC
                    LIMIT 3""").show()
