from pyspark.sql import SparkSession

# Initialize a SparkSession
spark = (
    SparkSession.builder.appName("Spark Example").getOrCreate()
)

# Example DataFrame
data = [
    ("James", "Smith", "USA", 30),
    ("Michael", "Rose", "USA", 45),
    ("Robert", "Williams", "USA", 25),
    ("Maria", "Jones", "USA", 22),
]

columns = ["Firstname", "Lastname", "Country", "Age"]
df = spark.createDataFrame(data, columns)

# Show the DataFrame
df.show()

# Register the DataFrame as a SQL temporary view
df.createOrReplaceTempView("customer")

# Run SQL commands
spark.sql("select FirstName from customer").show()
