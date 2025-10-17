# spark_etl_fast.py
# Faster ETL: Small dataset optimization & avoid unnecessary overhead

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import os
from pymongo import MongoClient

# ----------------------------
# CONFIG
# ----------------------------
BASE_DIR = "../synthetic_data"
MONGO_URI = "mongodb://localhost:27017"
DB_NAME = "popups_analytics"

# ----------------------------
# CREATE SPARK SESSION
# ----------------------------
spark = (
     SparkSession.builder 
    .appName("PopUpMarketETL") 
    .config("spark.ui.showConsoleProgress", "false")
    .getOrCreate()
)

# ----------------------------
# LOAD DATA (LIMITED for speed)
# ----------------------------
stalls_df = spark.read.option("header", True).csv(os.path.join(BASE_DIR, "stalls.csv"))

# For testing, load only first 2 files
tx_files_list = sorted(os.listdir(os.path.join(BASE_DIR, "transactions")))[:2]
tx_files = [os.path.join(BASE_DIR, "transactions", f) for f in tx_files_list]
tx_df = spark.read.option("header", True).csv(tx_files)

footfall_files_list = sorted(os.listdir(os.path.join(BASE_DIR, "footfall")))[:2]
footfall_files = [os.path.join(BASE_DIR, "footfall", f) for f in footfall_files_list]
footfall_df = spark.read.option("header", True).csv(footfall_files)

# ----------------------------
# DATA TRANSFORMATION
# ----------------------------
tx_df = tx_df.withColumn("quantity", F.col("quantity").cast("int")) \
             .withColumn("amount", F.col("amount").cast("int"))
footfall_df = footfall_df.withColumn("visitors", F.col("visitors").cast("int"))

stall_sales = tx_df.groupBy("stall_id").agg(
    F.count("tx_id").alias("num_transactions"),
    F.sum("amount").alias("total_sales")
)
stall_sales = stall_sales.withColumn("avg_tx_amount", F.col("total_sales") / F.col("num_transactions"))

stall_info = stalls_df.join(stall_sales, on="stall_id", how="left")

stall_visitors = footfall_df.groupBy("stall_id").agg(F.sum("visitors").alias("total_visitors"))
stall_info = stall_info.join(stall_visitors, on="stall_id", how="left")

# ----------------------------
# WRITE TO MONGODB (faster)
# ----------------------------
client = MongoClient(MONGO_URI)
db = client[DB_NAME]
stall_info_pd = stall_info.limit(1000).toPandas()  # limit rows for testing
db.stall_kpis.drop()
db.stall_kpis.insert_many(stall_info_pd.to_dict('records'))

print("Fast ETL done ✅ Results written to MongoDB")

# ----------------------------
# STOP SPARK
# ----------------------------
spark.stop()