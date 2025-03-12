#!/usr/bin/env python3
import os
import json
import time
import argparse
import pandas as pd
from kafka import KafkaConsumer, errors
import boto3
from dotenv import load_dotenv
from io import BytesIO
import datetime
# Environment defaults
DEFAULT_BROKER = "localhost:9094"  # Change to "kafka:9092" for Docker
KAFKA_BROKER = os.getenv("KAFKA_BROKER", DEFAULT_BROKER)

def init_s3_client():
    return boto3.client(
        "s3",
        endpoint_url=os.getenv("AWS_ENDPOINT_URL", "http://localhost:4566"),
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY")
    )

def init_kafka_consumer(topic="my-topic"):
    try:
        return KafkaConsumer(
            topic,
            bootstrap_servers=[KAFKA_BROKER],
            value_deserializer=lambda m: json.loads(m.decode("utf-8")),
            auto_offset_reset="earliest",
            enable_auto_commit=True,
            group_id="parquet-saver-group"
        )
    except errors.NoBrokersAvailable as e:
        print(f"[Error] No brokers available at {KAFKA_BROKER}.")
        raise e

def save_to_parquet(messages, bucket, s3_client, output_dir="./output", local=False):
    if not messages:
        return

    df = preprocess_message(messages)
    #first timestamp of the file is the timestamp of the first message
    if 'timestamp' in df.columns:
        timestamp = df['timestamp'].iloc[0].strftime("%Y%m%d-%H%M%S")
    else:
        timestamp = time.strftime("%Y%m%d-%H%M%S")
    filename = f"data_{timestamp}.parquet"
    if local:
        os.makedirs(output_dir, exist_ok=True)
        filepath = os.path.join(output_dir, filename)
        df.to_parquet(filepath, engine="pyarrow")
        print(f"[Parquet Saver] Saved {len(messages)} messages locally to {filepath}")
        try:
            s3_client.upload_file(filepath, bucket, filename)
            print(f"[S3 Uploader] Uploaded {filename} from local file to bucket '{bucket}'")
        except Exception as e:
            print(f"[Error] Uploading {filepath} to S3: {e}")
    else:
        buffer = BytesIO()
        df.to_parquet(buffer, engine="pyarrow")
        buffer.seek(0)
        print(f"[Parquet Saver] Created in-memory file {filename} with {len(messages)} messages")
        try:
            s3_client.upload_fileobj(buffer, bucket, filename)
            print(f"[S3 Uploader] Uploaded in-memory file '{filename}' to bucket '{bucket}'")
        except Exception as e:
            print(f"[Error] Uploading {filename} to S3: {e}")




def preprocess_message(msg):
    df = pd.DataFrame(msg)
    print(df)

    required_columns = ["frame_num", "object_id", "class_id", "class_label", "confidence", "top", "left", "width", "height", "timestamp", "sensor_id", "mission_id", "location_id", "latitude", "longitude"]
    df = df[required_columns]
    df['confidence'] = pd.to_numeric(df['confidence'], errors='coerce')  # Convert to float and coerce errors to NaN
    df['frame_num'] = pd.to_numeric(df['frame_num'], errors='coerce')
    df['object_id'] = pd.to_numeric(df['object_id'], errors='coerce')
    df['class_id'] = pd.to_numeric(df['class_id'], errors='coerce')
    df['top'] = pd.to_numeric(df['top'], errors='coerce')
    df['left'] = pd.to_numeric(df['left'], errors='coerce')
    df['width'] = pd.to_numeric(df['width'], errors='coerce')
    df['height'] = pd.to_numeric(df['height'], errors='coerce')
    df['latitude'] = pd.to_numeric(df['latitude'], errors='coerce')
    df['longitude'] = pd.to_numeric(df['longitude'], errors='coerce')
    df.dropna(inplace=True)

    def validate_and_convert_timestamp(timestamp):
        try:
            # Try to parse the timestamp to the correct format (YYYYMMDD-HHMMSS to YYYY-MM-DD HH:MM:SS)
            return datetime.datetime.strptime(timestamp, "%Y%m%d-%H%M%S")
        except ValueError:
            return None

    # Apply conversion for timestamp
    df['timestamp'] = df['timestamp'].apply(validate_and_convert_timestamp)

    # Remove rows with invalid timestamp formats
    df.dropna(subset=['timestamp'], inplace=True)

    # 3. Check the Confidence
    # Set a threshold for confidence (e.g., 0.3)
    confidence_threshold = 0.3
    df = df[df['confidence'] >= confidence_threshold]

    # Return the cleaned and processed data as a list of dictionaries (if you want to send it back in the same format)
    return df


def consume_messages(batch_size, batch_timeout, bucket, s3_client, consumer, output_dir, local=False):
    messages = []
    last_save = time.time()
    total_count = 0
    try:
        for msg in consumer:
            messages.append(msg.value)
            total_count += 1
            # When the batch size or timeout is reached, save and upload the batch.
            if len(messages) >= batch_size or (time.time() - last_save) >= batch_timeout:
                print(f"[Consumer] Batch ready: {len(messages)} messages (Total: {total_count})")
                save_to_parquet(messages, bucket, s3_client, output_dir, local)
                messages = []
                last_save = time.time()
    except KeyboardInterrupt:
        print("[Consumer] Interrupted. Saving remaining messages...")
        save_to_parquet(messages, bucket, s3_client, output_dir, local)
        print(f"[Consumer] Exiting. Total messages processed: {total_count}")




if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Consume Kafka messages, save to Parquet, and upload to S3."
    )
    parser.add_argument('--bucket', type=str, default="staging", help='The S3 bucket to upload to')
    parser.add_argument('--topic', type=str, default="my-topic", help='The Kafka topic to consume from')
    parser.add_argument('--output-dir', type=str, default="./output", help='Local directory to save Parquet files')
    parser.add_argument('--batch-size', type=int, default=300, help='Number of messages per batch')
    parser.add_argument('--batch-timeout', type=int, default=10, help='Timeout (seconds) for a batch')
    parser.add_argument('--local', action='store_true', help='Use localstack for S3')
    args = parser.parse_args()

    load_dotenv()
    os.makedirs(args.output_dir, exist_ok=True)
    s3_client = init_s3_client()
    consumer = init_kafka_consumer(args.topic)
    consume_messages(args.batch_size, args.batch_timeout, args.bucket, s3_client, consumer, args.output_dir, args.local)
