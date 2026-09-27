from databricks import sql
import os
from dotenv import load_dotenv
from delta.tables import DeltaTable
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType

load_dotenv()
DATABRICKS_TOKEN = os.getenv("DATABRICKS_TOKEN")
DATABRICKS_SERVER_HOST = os.getenv("DATABRICKS_SERVER_HOST")
DATABRICKS_HTTP_PATH = os.getenv("DATABRICKS_HTTP_PATH")
DATABRICKS_CLUSTER_ID = os.getenv("DATABRICKS_CLUSTER_ID")


def databricks_connection():
    for temp in range(1, 3):
        try:
            conn= sql.connect(
                server_hostname = DATABRICKS_SERVER_HOST,
                http_path = DATABRICKS_HTTP_PATH,
                access_token = DATABRICKS_TOKEN
            )
            print("Successfully connected to Databricks")
            return conn
        except Exception as e:
            print(f"Attempt {temp} failed: {e}")
            if temp == 2:
                raise Exception("Failed to connect to Databricks after 3 attempts")

def get_spark() -> SparkSession:

    # Databricks Connect when running locally, the cluster session when running on Databricks
    try:
        from databricks.connect import DatabricksSession
    except ImportError:
        return SparkSession.builder.getOrCreate()

    # No local credentials means we are already running on Databricks
    if not (DATABRICKS_SERVER_HOST and DATABRICKS_TOKEN):
        return DatabricksSession.builder.getOrCreate()

    host = DATABRICKS_SERVER_HOST
    if not host.startswith("https://"):
        host = "https://" + host

    builder = DatabricksSession.builder.host(host).token(DATABRICKS_TOKEN)

    # Databricks Connect needs a cluster or serverless compute, a SQL warehouse will not work
    if DATABRICKS_CLUSTER_ID:
        builder = builder.clusterId(DATABRICKS_CLUSTER_ID)
    else:
        builder = builder.serverless()

    return builder.getOrCreate()

def main():
    load_dotenv()
    connection = databricks_connection()
    cursor = connection.cursor()

    cursor.execute("SELECT * FROM range(3)")
    print(cursor.fetchall())
    
    cursor.close()
    connection.close()

if __name__ == "__main__":
    main()