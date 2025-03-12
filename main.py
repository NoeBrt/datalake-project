from fastapi import FastAPI, HTTPException,Query
from fastapi.concurrency import run_in_threadpool
from dotenv import load_dotenv
from pydantic import BaseModel
import time
import json
import os
import pandas as pd
# Kafka
from kafka import KafkaConsumer

# boto3 for S3
import boto3
import io
# MySQL connector
import mysql.connector

# Load environment variables
load_dotenv()

app = FastAPI(title="Data Processing Pipeline API")

# -------------------------
# Utility Functions
# -------------------------

def init_kafka_consumer(topic: str):
    """Initialize and return a KafkaConsumer for the given topic."""
    broker = os.getenv("KAFKA_BROKER", "localhost:9092")
    try:
        consumer = KafkaConsumer(
            topic,
            bootstrap_servers=[broker],
            value_deserializer=lambda m: json.loads(m.decode("utf-8")),
            auto_offset_reset="earliest",
            enable_auto_commit=True,
            group_id="fastapi-kafka-group"
        )
    except Exception as e:
        raise Exception(f"Error initializing KafkaConsumer: {e}")
    return consumer

def init_s3_client():
    """Initialize and return a boto3 S3 client."""
    try:
        s3 = boto3.client(
            "s3",
            endpoint_url=os.getenv("AWS_ENDPOINT_URL", "http://localhost:4566"),
            aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
            aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY")
        )
    except Exception as e:
        raise Exception(f"Error initializing S3 client: {e}")
    return s3

def get_db_connection():
    """Establish and return a connection to the MySQL database."""
    try:
        print(os.getenv("MYSQL_HOST", "127.0.0.1"),os.getenv("MYSQL_USER", "root"),os.getenv("MYSQL_PASSWORD", "root"),os.getenv("MYSQL_DATABASE", "curated"),int(os.getenv("MYSQL_PORT", "3307")))
        conn = mysql.connector.connect(
            host=os.getenv("MYSQL_HOST", "127.0.0.1"),
            user=os.getenv("MYSQL_USER", "root"),
            password=os.getenv("MYSQL_PASSWORD", "root"),
            database=os.getenv("MYSQL_DATABASE", "curated"),
            port=int(os.getenv("MYSQL_PORT", "3307"))
        )
    except Exception as e:
        raise Exception(f"Error connecting to MySQL: {e}")
    return conn

# -------------------------
# /raw Endpoint: Kafka
# -------------------------
def fetch_raw_messages(duration: int = 5):
    """
    Connect to Kafka and collect raw messages for a specified duration (in seconds).
    Returns a list of messages.
    """
    consumer = init_kafka_consumer("my-topic")
    messages = []
    start_time = time.time()
    try:
        while time.time() - start_time < duration:
            records = consumer.poll(timeout_ms=500)
            if records:
                for tp_records in records.values():
                    for record in tp_records:
                        messages.append(record.value)
    except Exception as e:
        raise e
    finally:
        consumer.close()
    return messages

@app.get("/raw")
async def raw_data(duration: int = 5):
    """
    Endpoint to fetch raw data from Kafka.
    The query parameter `duration` specifies the number of seconds to collect messages.
    """
    try:
        msgs = await run_in_threadpool(fetch_raw_messages, duration)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    return {"raw_messages": msgs}

# -------------------------
# /staging Endpoint: boto3 / S3
# -------------------------
def list_staging_files(bucket: str = "staging"):
    """
    Uses boto3 to list objects in the specified S3 bucket.
    Returns a list of file keys.
    """
    s3_client = init_s3_client()
    try:
        response = s3_client.list_objects_v2(Bucket=bucket)
        contents = response.get("Contents", [])
        file_keys = [obj["Key"] for obj in contents]
    except Exception as e:
        raise Exception(f"Error listing objects in bucket '{bucket}': {e}")
    return file_keys

@app.get("/staging")
async def staging_data(bucket: str = "staging"):
    """
    Endpoint to list files in the staging bucket using boto3.
    The query parameter `bucket` specifies the S3 bucket name.
    """
    try:
        files = await run_in_threadpool(list_staging_files, bucket)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    return {"staging_files": files}

def read_file_from_s3(bucket: str, key: str) -> pd.DataFrame:
    """
    Reads a CSV file from S3 using boto3 and returns a pandas DataFrame.
    This method downloads the file content into memory before parsing it.
    """
    s3_client = init_s3_client()
    try:
        response = s3_client.get_object(Bucket=bucket, Key=key)
        # Read file content into memory
        data = response["Body"].read()
        # Use io.BytesIO if it's a binary stream, and decode if needed
        df = pd.read_parquet(io.BytesIO(data))
    except Exception as e:
        raise Exception(f"Error reading file '{key}' from bucket '{bucket}': {e}")
    return df

@app.get("/staging/read")
async def read_staging_file(file_key: str, bucket: str = "staging"):
    """
    Endpoint to read a specific file from the staging bucket using pandas.
    The file is expected to be in CSV format.
    """
    try:
        # Running in a thread pool since file I/O and pandas operations are blocking
        df = await run_in_threadpool(read_file_from_s3, bucket, file_key)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    # Optionally convert the DataFrame to a JSON serializable format
    return {"data": df.to_dict(orient="records")}

# -------------------------
# /curated Endpoint: MySQL
# -------------------------
ALLOWED_TABLES = {"aggregation", "sensor", "detection","processed_files"}
def fetch_curated_data(table: str = "aggregation"):
    """
    Connects to the curated MySQL database and fetches data from the specified table.
    Returns the rows as a list of dictionaries.
    Only allowed table names from ALLOWED_TABLES are permitted.
    """
    if table not in ALLOWED_TABLES:
        raise Exception(f"Table '{table}' is not allowed. Allowed tables: {', '.join(ALLOWED_TABLES)}")
    conn = get_db_connection()
    cursor = conn.cursor(dictionary=True)
    try:
        query = f"SELECT * FROM {table};"
        cursor.execute(query)
        rows = cursor.fetchall()
    except Exception as e:
        raise Exception(f"Error querying table '{table}': {e}")
    finally:
        cursor.close()
        conn.close()
    return rows

@app.get("/curated")
async def curated_data(table: str = Query("aggregation", description="Table name to query from MySQL (e.g., aggregation, sensor, detection)")):
    """
    Endpoint to fetch curated data from MySQL.
    The query parameter `table` specifies which table to query.
    Returns the data from the specified table.
    """
    try:
        data = await run_in_threadpool(fetch_curated_data, table)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    return {"curated_data": data}
# -------------------------
# Health Check
# -------------------------
def check_mysql():
    """Check if MySQL is reachable."""
    try:
        conn = mysql.connector.connect(
            host=os.getenv("MYSQL_HOST", "127.0.0.1"),
            user=os.getenv("MYSQL_USER", "root"),
            password=os.getenv("MYSQL_PASSWORD", "root"),
            database=os.getenv("MYSQL_DATABASE", "curated"),
            port=int(os.getenv("MYSQL_PORT", "3307")),
            connection_timeout=5
        )
        conn.close()
        return {"mysql": "OK"}
    except Exception as e:
        return {"mysql": f"ERROR - {str(e)}"}

def check_kafka():
    """Check if Kafka is reachable."""
    broker = os.getenv("KAFKA_BROKER", "localhost:9094")
    try:
        consumer = KafkaConsumer(
            bootstrap_servers=[broker],
            group_id="health-check",
            enable_auto_commit=False
        )
        consumer.close()
        return {"kafka": "OK"}
    except Exception as e:
        return {"kafka": f"ERROR - {str(e)}"}

def check_s3():
    """Check if S3 (LocalStack or AWS) is reachable."""
    try:
        s3 = boto3.client(
            "s3",
            endpoint_url=os.getenv("AWS_ENDPOINT_URL", "http://localhost:4566"),
            aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID", "root"),
            aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY", "root"),
            region_name=os.getenv("AWS_REGION", "us-east-1")
        )
        _ = s3.list_buckets()  # Simple API call to check connectivity
        return {"s3": "OK"}
    except Exception as e:
        return {"s3": f"ERROR - {str(e)}"}

def check_airflow():
    """Check if Airflow webserver is reachable."""
    airflow_url = "http://localhost:8081/health"
    try:
        import requests
        response = requests.get(airflow_url, timeout=5)
        if response.status_code == 200:
            return {"airflow": "OK"}
        else:
            return {"airflow": f"ERROR - Status Code {response.status_code}"}
    except Exception as e:
        return {"airflow": f"ERROR - {str(e)}"}

# -------------------------
# Health Check Endpoint
# -------------------------

@app.get("/health")
def health_check():
    """Health check for MySQL, Kafka, S3, and Airflow."""
    health_status = [
        check_mysql(),
        check_kafka(),
        check_s3(),
        check_airflow()
    ]
    return health_status


# -------------------------
# Run the application
# -------------------------
if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)
