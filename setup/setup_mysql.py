import mysql.connector
from dotenv import load_dotenv
import os

# Load environment variables from .env file
load_dotenv()

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

# Function to create database if it doesn't exist
def create_database():
    conn = mysql.connector.connect(
        host=db_config['host'],
        user=db_config['user'],
        password=db_config['password'],
        port=db_config['port']  # include the port here
    )
    cursor = conn.cursor()
    cursor.execute(f"CREATE DATABASE IF NOT EXISTS {db_config['database']};")
    conn.commit()
    conn.close()

# Function to create tables
def create_tables():
    conn = get_db_connection()
    cursor = conn.cursor()

    # Create the sensor table
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS sensor (
            sensor_id VARCHAR(255) PRIMARY KEY,
            mission_id VARCHAR(255),
            location_id VARCHAR(255),
            latitude FLOAT,
            longitude FLOAT
        );
    """)

    # Create the detection table
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS detection (
            frame_num INT,
            object_id INT,
            class_id INT,
            class_label VARCHAR(255),
            confidence FLOAT,
            top FLOAT,
            `left` FLOAT,
            width FLOAT,
            height FLOAT,
            timestamp DATETIME,
            sensor_id VARCHAR(255),
            PRIMARY KEY (frame_num, object_id),
            FOREIGN KEY (sensor_id) REFERENCES sensor(sensor_id)
        );
    """)

    # Create the aggregation table
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS aggregation (
            sensor_id VARCHAR(255) PRIMARY KEY,
            total_detections INT,
            average_confidence FLOAT,
            max_confidence FLOAT,
            min_confidence FLOAT,
            most_frequent_class VARCHAR(255),
            most_frequent_class_count INT,
            last_update DATETIME,
            FOREIGN KEY (sensor_id) REFERENCES sensor(sensor_id)
        );
    """)

    # Create the processed_files table
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS processed_files (
            file_key VARCHAR(255) PRIMARY KEY,
            processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
    """)

    conn.commit()
    conn.close()

# Run the setup
if __name__ == "__main__":
    create_database()  # Create database if it doesn't exist
    create_tables()  # Create necessary tables
    print("Database and tables have been set up successfully.")
