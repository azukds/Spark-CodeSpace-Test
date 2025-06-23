import csv
import random
from datetime import datetime, timedelta
from pyspark.sql.types import StructType, StructField, IntegerType, ArrayType, StringType, DateType
from pyspark.sql import SparkSession

# Initialize a SparkSession
spark = (
    SparkSession.builder.appName("Spark Example").getOrCreate()
)
random.seed = 123456
def generate_random_digits(length=11):
    return ''.join(str(random.randint(0, 9)) for _ in range(length))

# Generate a list of 500 random strings of 11 digits
random_digit_strings = [generate_random_digits() for _ in range(500)]
# Generate Customer Dataset
customer_data = []
for i in range(1, 501):
    customer_data.append([
        f"{i:03}",
        f"Customer{i}",
        f"{random.randint(1970, 2000)}-{random.randint(1, 12):02d}-{random.randint(1, 28):02d}",
        f"{generate_random_digits()}",
        f"customer{i}@example.com"
    ])

columns = ["CustomerId", "Name", "DoB", "Phone Number", "Email"]
customer_data = spark.createDataFrame(customer_data,columns )
customer_data.write.parquet('customer_dataset.parquet', mode = 'overwrite')

# Generate Product Dataset
product_data = []
for i in range(1, 101):
    product_data.append([
        i,
        f"Product{i}",
        round(random.uniform(10, 100), 2)
    ])

columns = ["ProductID", "Item", "cost"]
product_data = spark.createDataFrame(product_data, columns)
product_data.write.parquet('product_dataset.parquet', mode = 'overwrite', )

# Generate Sales Dataset
sales_data = []
start_date = datetime(2023, 1, 1)
end_date = datetime(2023, 12, 31)

def random_date(start, end):
    return start + timedelta(days=random.randint(0, (end - start).days))

for i in range(1, 1001):
    customer_id = f"{random.randint(1, 500):03}"
    num_products = random.randint(1, 5)
    product_ids = random.sample(range(1, 101), num_products)
    purchase_date = random_date(start_date, end_date).strftime("%Y-%m-%d")
    sales_data.append([
        i,
        customer_id,
        product_ids,
        purchase_date
    ])

# Define the schema
sales_schema = StructType([
    StructField("SaleID", IntegerType(), True),
    StructField("CustomerID", StringType(), True),
    StructField("ProductIDs", ArrayType(IntegerType()), True),
    StructField("Purchase_Date", StringType(), True)  # Using StringType for simplicity
])

salesDF = spark.createDataFrame(data=sales_data,schema=sales_schema)
salesDF.write.parquet('sales_dataset.parquet', mode = 'overwrite')

print("CSV files generated successfully.")
