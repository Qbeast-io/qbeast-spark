from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from qviz.content_loader import extract_cubes_from_blocks, extract_metadata_from_delta_table, create_delta_table
from faker import Faker
import random
from delta.tables import DeltaTable

# Crear una sesión de Spark
#spark = SparkSession.builder \
#    .appName("GenerarTablaFalsaGrande") \
#    .getOrCreate()

# Instanciar Faker
fake = Faker()

# Definir el esquema de la tabla
schema = StructType([
    StructField("id", IntegerType(), False),
    StructField("name", StringType(), True),
    StructField("address", StringType(), True),
    StructField("email", StringType(), True),
    StructField("age", IntegerType(), True)
])

# Function to generate fake data
def generate_fake_data(partition_id, num_rows):
    Faker.seed(partition_id)  # Seed to make every partition different
    fake = Faker()
    data = []
    for i in range(num_rows):
        data.append({
            "id": partition_id * num_rows + i + 1,
            "name": fake.name(),
            "address": fake.address(),
            "email": fake.email(),
            "age": random.randint(18, 80)
        })
    return data

# Total number of rows & partitions
total_rows = 4_000_000  
partitions = 200
rows_per_partition = total_rows // partitions

# Generate the data in paralel using RDD
rdd = spark.sparkContext.parallelize(range(partitions), partitions) \
    .flatMap(lambda partition_id: generate_fake_data(partition_id, rows_per_partition))

# Convert RDD into a DataFrame
df = spark.createDataFrame(rdd, schema)

# Create three Qbeast tables
df_path1 = "/Users/jorgemarin/Documents/qviz-bug/qbeast-spark/utils/visualizer/tests/resources/big_fake_table/table1"
df_path2 = "/Users/jorgemarin/Documents/qviz-bug/qbeast-spark/utils/visualizer/tests/resources/big_fake_table/table2"
df_path3 = "/Users/jorgemarin/Documents/qviz-bug/qbeast-spark/utils/visualizer/tests/resources/big_fake_table/table3"

age_min = df.select("age").agg({"age": "min"}).collect()[0][0]
age_max = df.select("age").agg({"age": "max"}).collect()[0][0]

df.write.mode("overwrite") \
    .format("qbeast") \
    .option("columnsToIndex", "age") \
    .option("columnStats", f"""{{"age_min":{age_min},"age_max":{age_max}}}""") \
    .option("cubeSize", "500000") \
    .save("df_path1")

filtered_df = df.filter((df["age"] >= 24) & (df["age"] <= 63))
age_min = filtered_df.select("age").agg({"age": "min"}).collect()[0][0]
age_max = filtered_df.select("age").agg({"age": "max"}).collect()[0][0]

filtered_df.write.mode("overwrite") \
    .format("qbeast") \
    .option("columnsToIndex", "age") \
    .option("columnStats", f"""{{"age_min":{age_min},"age_max":{age_max}}}""") \
    .option("cubeSize", "500000") \
    .save("df_path2")

filtered_df = df.filter((df["age"] >= 30) & (df["age"] <= 53))
age_min = filtered_df.select("age").agg({"age": "min"}).collect()[0][0]
age_max = filtered_df.select("age").agg({"age": "max"}).collect()[0][0]

filtered_df.write.mode("overwrite") \
    .format("qbeast") \
    .option("columnsToIndex", "age") \
    .option("columnStats", f"""{{"age_min":{age_min},"age_max":{age_max}}}""") \
    .option("cubeSize", "500000") \
    .save("df_path3")

# COUNT THE NUMBER OF ELEMENTS IN:
# FIRST TABLE
delta_table1 = DeltaTable(df_path1)
d1_adds = delta_table1.get_add_actions(True).to_pandas()
elements_number = d1_adds["num_records"].sum() #4.000.000

metadata1, symbol_count1 = extract_metadata_from_delta_table(delta_table1, "1")
cubes = extract_cubes_from_blocks(delta_table1, symbol_count1)
cubes_elements_number = 0
for cube in cubes.values():
    cubes_elements_number += cube.element_count
print(cubes_elements_number) #4.000.000


# SECOND TABLE
delta_table2 = DeltaTable(df_path2)
d2_adds = delta_table2.get_add_actions(True).to_pandas()
elements_number = d2_adds["num_records"].sum() #2.541.432

metadata1, symbol_count1 = extract_metadata_from_delta_table(delta_table1, "1")
cubes = extract_cubes_from_blocks(delta_table1, symbol_count1)
cubes_elements_number = 0
for cube in cubes_dict.values():
    cubes_elements_number += cube.element_count
print(cubes_elements_number) #2.541.432


# THIRD TABLE
delta_table3 = DeltaTable(df_path3)
d3_adds = delta_table3.get_add_actions(True).to_pandas()
elements_number = d3_adds["num_records"].sum() #1.524.203

metadata1, symbol_count1 = extract_metadata_from_delta_table(delta_table1, "1")
cubes = extract_cubes_from_blocks(delta_table1, symbol_count1)
cubes_elements_number = 0
for cube in cubes.values():
    cubes_elements_number += cube.element_count
print(cubes_elements_number) #1.524.203