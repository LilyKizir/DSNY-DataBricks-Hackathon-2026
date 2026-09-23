from databricks import sql
import os
from dotenv import load_dotenv

load_dotenv()
DATABRICKS_TOKEN = os.getenv("DATABRICKS_TOKEN")
DATABRICKS_SERVER_HOST = os.getenv("DATABRICKS_SERVER_HOST")
DATABRICKS_HTTP_PATH = os.getenv("DATABRICKS_HTTP_PATH")


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