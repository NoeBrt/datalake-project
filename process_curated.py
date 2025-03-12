#!/usr/bin/env python3
import os
import io
import json
import time
import argparse
import datetime
import pandas as pd
import mysql.connector
import boto3
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Initialize boto3 S3 client
s3_client = boto3.client('s3')

db_config = {
    "host": os.getenv("MYSQL_HOST", "127.0.0.1"),
    "user": os.getenv("MYSQL_USER", "root"),
    "password": os.getenv("MYSQL_PASSWORD", "root"),
    "database": os.getenv("MYSQL_DATABASE", "curated"),
    "port": int(os.getenv("MYSQL_PORT", "3307"))
}

# Function to connect to MySQL
def get_db_connection():
    conn = mysql.connector.connect(**db_config)
    return conn

# Function to insert data into the sensor table
def insert_sensor_data(conn, sensor_id, mission_id, location_id, latitude, longitude):
    cursor = conn.cursor()
    cursor.execute("""
        INSERT INTO sensor (sensor_id, mission_id, location_id, latitude, longitude)
        VALUES (%s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE mission_id = %s, location_id = %s, latitude = %s, longitude = %s;
    """, (sensor_id, mission_id, location_id, latitude, longitude,
          mission_id, location_id, latitude, longitude))
    conn.commit()
    cursor.close()

# Function to insert data into the detection table
def insert_detection_data(conn, detection_data):
    # Convert the timestamp from Pandas Timestamp to Python datetime if needed
    ts = detection_data.get('timestamp')
    if hasattr(ts, "to_pydatetime"):
        detection_data['timestamp'] = ts.to_pydatetime()

    cursor = conn.cursor()
    cursor.execute("""
        INSERT INTO detection (frame_num, object_id, class_id, class_label, confidence, top, `left`, width, height, timestamp, sensor_id)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE confidence = %s, top = %s, `left` = %s, width = %s, height = %s;
    """, (detection_data['frame_num'],
          detection_data['object_id'],
          detection_data['class_id'],
          detection_data['class_label'],
          detection_data['confidence'],
          detection_data['top'],
          detection_data['left'],
          detection_data['width'],
          detection_data['height'],
          detection_data['timestamp'],
          detection_data['sensor_id'],
          detection_data['confidence'],
          detection_data['top'],
          detection_data['left'],
          detection_data['width'],
          detection_data['height']))
    conn.commit()
    cursor.close()

def fill_aggregation_table():
    conn = get_db_connection()
    cursor = conn.cursor(dictionary=True)

    # Get all sensor_ids from the sensor table
    cursor.execute("SELECT sensor_id FROM sensor;")
    sensors = cursor.fetchall()

    for sensor in sensors:
        sensor_id = sensor['sensor_id']

        # Aggregate detection data for the current sensor
        cursor.execute("""
            SELECT COUNT(*) AS total_detections,
                   AVG(confidence) AS average_confidence,
                   MAX(confidence) AS max_confidence,
                   MIN(confidence) AS min_confidence
            FROM detection
            WHERE sensor_id = %s;
        """, (sensor_id,))
        agg = cursor.fetchone()
        total_detections = agg['total_detections']
        average_confidence = agg['average_confidence']
        max_confidence = agg['max_confidence']
        min_confidence = agg['min_confidence']

        # Determine the most frequent class and its count for the sensor
        cursor.execute("""
            SELECT class_label, COUNT(*) AS class_count
            FROM detection
            WHERE sensor_id = %s
            GROUP BY class_label
            ORDER BY class_count DESC
            LIMIT 1;
        """, (sensor_id,))
        result = cursor.fetchone()
        if result:
            most_frequent_class = result['class_label']
            most_frequent_class_count = result['class_count']
        else:
            most_frequent_class = None
            most_frequent_class_count = 0

        # Get the current datetime for last_update
        last_update = datetime.datetime.now()

        # Insert or update the aggregation table for the sensor
        cursor.execute("""
            INSERT INTO aggregation (
                sensor_id, total_detections, average_confidence, max_confidence, min_confidence,
                most_frequent_class, most_frequent_class_count, last_update
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
                total_detections = VALUES(total_detections),
                average_confidence = VALUES(average_confidence),
                max_confidence = VALUES(max_confidence),
                min_confidence = VALUES(min_confidence),
                most_frequent_class = VALUES(most_frequent_class),
                most_frequent_class_count = VALUES(most_frequent_class_count),
                last_update = VALUES(last_update);
        """, (sensor_id, total_detections, average_confidence, max_confidence, min_confidence,
              most_frequent_class, most_frequent_class_count, last_update))
        conn.commit()
        print(f"Aggregation updated for sensor {sensor_id}")

    cursor.close()
    conn.close()
    print("Aggregation table has been filled successfully.")

# Fetch parquet files from S3
def fetch_parquet_from_s3(bucket_name, prefix):
    # List objects in the S3 bucket
    response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=prefix)
    files = response.get('Contents', [])

    for file in files:
        file_key = file['Key']
        print(f"Processing file: {file_key}")

        # Get the parquet file from S3
        file_obj = s3_client.get_object(Bucket=bucket_name, Key=file_key)
        parquet_data = file_obj['Body'].read()

        # Read the parquet file into a Pandas DataFrame
        df = pd.read_parquet(io.BytesIO(parquet_data))

        # Process each record
        for _, row in df.iterrows():
            detection_data = row.to_dict()
            sensor_id = detection_data['sensor_id']
            mission_id = detection_data['mission_id']
            location_id = detection_data['location_id']
            latitude = detection_data.get('latitude', None)  # Get latitude if present
            longitude = detection_data.get('longitude', None)  # Get longitude if present

            # Insert sensor data if not already present
            conn = get_db_connection()
            insert_sensor_data(conn, sensor_id, mission_id, location_id, latitude, longitude)

            # Insert detection data
            insert_detection_data(conn, detection_data)

            conn.close()

# Function to display the SQL schema and first 10 rows of each table
def display_schema_and_samples():
    conn = get_db_connection()
    cursor = conn.cursor()

    # Get a list of tables
    cursor.execute("SHOW TABLES;")
    tables = cursor.fetchall()

    for (table_name,) in tables:
        print(f"\n--- Table: {table_name} ---")
        # Display table schema
        cursor.execute(f"DESCRIBE {table_name};")
        schema = cursor.fetchall()
        print("Schema:")
        for row in schema:
            print(row)

        # Display first 10 rows of the table
        cursor.execute(f"SELECT * FROM {table_name} LIMIT 10;")
        rows = cursor.fetchall()
        print("\nSample rows:")
        for row in rows:
            print(row)

    cursor.close()
    conn.close()

# Main function with argparse
def main():
    parser = argparse.ArgumentParser(
        description="Process parquet files from S3, fill aggregation table, and display SQL schema with samples."
    )
    parser.add_argument('--bucket', type=str, default="staging", help='The S3 bucket name')
    parser.add_argument('--prefix', type=str, default="", help='Prefix in the bucket for parquet files')
    args = parser.parse_args()

    # Process parquet files and fill detection/sensor tables
    fetch_parquet_from_s3(args.bucket, args.prefix)
    # Fill/update the aggregation table
    fill_aggregation_table()
    # Display the SQL schema and first 10 rows of each table
    display_schema_and_samples()

if __name__ == "__main__":
    main()
