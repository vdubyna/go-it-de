from pyflink.table import (EnvironmentSettings, TableEnvironment, TableDescriptor, Schema, DataTypes)
from pyflink.table.expressions import col

# Створення середовища таблиць у режимі потокової обробки
t_env = TableEnvironment.create(EnvironmentSettings.in_streaming_mode())

# Встановлення паралелізму виконання
t_env.get_config().set("parallelism.default", "1")

# Створення тимчасової таблиці для users.csv
t_env.create_temporary_table(
    'users',
    TableDescriptor.for_connector('filesystem')
    .schema(Schema.new_builder()
            .column('user_id', DataTypes.INT())
            .column('name', DataTypes.STRING())
            .column('age', DataTypes.INT())
            .column('email', DataTypes.STRING())
            .build())
    .option('path', '<absolute_path>/users.csv')  # Вказання шляху до файлу users.csv
    .option('csv.ignore-parse-errors', 'true')  # Ігнорування помилок парсингу
    .format('csv')  # Вказання формату файлу CSV
    .build())

# Зчитування даних з тимчасової таблиці users
tab_users = t_env.from_path('users')
# Виведення таблиці users (розкоментуйте, якщо хочете подивитись на цю таблицю)
# tab_users.execute().print()

# Створення тимчасової таблиці для purchases.csv
t_env.create_temporary_table(
    'purchases',
    TableDescriptor.for_connector('filesystem')
    .schema(Schema.new_builder()
            .column('purchase_id', DataTypes.INT())
            .column('user_id', DataTypes.INT())
            .column('product_id', DataTypes.INT())
            .column('date', DataTypes.DATE())
            .column('quantity', DataTypes.INT())
            .build())
    .option('path', '<absolute_path>/purchases.csv')  # Вказання шляху до файлу purchases.csv
    .format('csv')  # Вказання формату файлу CSV
    .option('csv.ignore-parse-errors', 'true')  # Ігнорування помилок парсингу
    .build())

# Зчитування даних з тимчасової таблиці purchases
purchases_tab = t_env.from_path('purchases')
# Виведення таблиці purchases
# purchases_tab.execute().print()

# Фільтрація таблиці users від NULL значень
filtered_users = tab_users.filter(
    col('user_id').is_not_null &  # Фільтрація стовпця user_id від NULL значень
    col('name').is_not_null &
    col('age').is_not_null &
    col('email').is_not_null
).alias('user_id_u', 'name', 'age', 'email')  # Перейменування стовпця user_id на user_id_u
# Flink не любить об'єднувати таблиці за стовпчиками з однаковими іменами

# Виведення відфільтрованої таблиці users
# filtered_users.execute().print()

# Фільтрація таблиці purchases від NULL значень
filtered_purchases = purchases_tab.filter(
    col('purchase_id').is_not_null &
    col('user_id').is_not_null &
    col('product_id').is_not_null &
    col('date').is_not_null &
    col('quantity').is_not_null
)

# Виведення відфільтрованої таблиці purchases
# filtered_purchases.execute().print()

# Об'єднання таблиць users і purchases за user_id
joined_table = (filtered_users.join(filtered_purchases)
.where(filtered_users.user_id_u == filtered_purchases.user_id) \
.select(
    filtered_purchases.user_id,
    filtered_users.name,
    filtered_users.age,
    filtered_users.email,
    filtered_purchases.purchase_id,
    filtered_purchases.product_id,
    filtered_purchases.date,
    filtered_purchases.quantity
))

# Виведення результатів об'єднання
# joined_table.execute().print()

# Агрегація даних: підрахунок суми за quantity для кожного user_id
aggregated_table = joined_table.group_by(col('user_id')) \
    .select(
    col('user_id'),
    col('quantity').sum.alias('total_quantity')  # Агрегація стовпця quantity та перейменування на total_quantity
)

# Виведення результатів агрегації
aggregated_table.execute().print()
