import sys
import gi
import os
import json
import queue
import threading
import signal
from datetime import datetime

gi.require_version('Gst', '1.0')
from gi.repository import Gst, GLib

import pyds
from kafka import KafkaProducer

# ----- Kafka Producer Setup -----
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka:9092")
KAFKA_TOPIC  = os.getenv("KAFKA_TOPIC", "my-topic")

SENSOR_ID = os.getenv("SENSOR_ID", "camera-01")
MISSION_ID = os.getenv("MISSION_ID", "mission-01")
LOCATION = os.getenv("LOCATION_ID", "location-01")
LATITUDE = os.getenv("LATITUDE", "0.0")
LONGITUDE = os.getenv("LONGITUDE", "0.0")



producer = KafkaProducer(
    bootstrap_servers=[KAFKA_BROKER],
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

# Create a thread-safe queue for Kafka messages with a max size
message_queue = queue.Queue(maxsize=1000)

def on_send_success(record_metadata):
    """Callback when a message is successfully delivered."""
    print(f"[Kafka] Delivered to {record_metadata.topic} | Partition: {record_metadata.partition} | Offset: {record_metadata.offset}", flush=True)

def on_send_error(exception):
    """Callback when there's an error sending a message."""
    print(f"[Kafka] Error sending message: {exception}", flush=True)

def kafka_sender():
    """Process messages in batches from the queue and send them to Kafka."""
    BATCH_SIZE = 30  # adjust the batch size as needed
    while True:
        messages = []
        try:
            # Block until at least one message is available
            msg = message_queue.get(timeout=1)
            messages.append(msg)
            # Collect additional messages if available without blocking
            while not message_queue.empty() and len(messages) < BATCH_SIZE:
                messages.append(message_queue.get_nowait())
        except queue.Empty:
            pass

        if messages:
            print(f"[Kafka Sender] Sending batch of {len(messages)} messages", flush=True)
            for message in messages:
                try:
                    future = producer.send(KAFKA_TOPIC, message)
                    future.add_callback(on_send_success)
                    future.add_errback(on_send_error)
                except Exception as e:
                    print(f"[Kafka Sender] Exception sending message: {e}", flush=True)
            # Flush to force all messages to be sent before processing the next batch
            producer.flush()

# Start the Kafka sender thread
kafka_thread = threading.Thread(target=kafka_sender, daemon=True)
kafka_thread.start()

def shutdown_handler(signum, frame):
    """Gracefully shutdown the Kafka producer on exit."""
    print("Shutdown signal received. Flushing Kafka producer and exiting...", flush=True)
    producer.flush()
    sys.exit(0)

# Register signal handlers for graceful shutdown
signal.signal(signal.SIGINT, shutdown_handler)
signal.signal(signal.SIGTERM, shutdown_handler)

# ----- DeepStream Probe Callback and Processing -----
def probe_callback(pad, info):
    """GStreamer probe callback to intercept buffers."""
    gst_buffer = info.get_buffer()
    if not gst_buffer:
        print("Unable to get GstBuffer", flush=True)
        return Gst.PadProbeReturn.OK

    process_frame(gst_buffer)
    return Gst.PadProbeReturn.OK

def process_frame(gst_buffer):
    """
    Extract metadata from the DeepStream batch and enqueue
    each object's info as a JSON message to be sent to Kafka.
    """
    # Obtain the batch metadata from the GstBuffer
    batch_meta = pyds.gst_buffer_get_nvds_batch_meta(hash(gst_buffer))
    if not batch_meta:
        print("Unable to get batch metadata", flush=True)
        return Gst.PadProbeReturn.OK

    current_time = datetime.now().strftime('%Y%m%d-%H%M%S')
    print(f"[Debug] Processing new batch at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}", flush=True)

    l_frame = batch_meta.frame_meta_list
    while l_frame:
        frame_meta = pyds.NvDsFrameMeta.cast(l_frame.data)
        frame_number = frame_meta.frame_num
        l_frame = l_frame.next  # Move to next frame meta

        l_obj = frame_meta.obj_meta_list
        while l_obj:
            obj_meta = pyds.NvDsObjectMeta.cast(l_obj.data)
            l_obj = l_obj.next

            # Skip invalid objects
            if obj_meta.object_id == 0xFFFFFFFFFFFFFFFF:
                print(f"[Debug] Skipping invalid object in frame {frame_number}", flush=True)
                continue

            rect = obj_meta.rect_params
            message_dict = {
                "frame_num": frame_number,
                "object_id": obj_meta.object_id,
                "class_id": int(obj_meta.class_id),
                "class_label": obj_meta.obj_label,
                "confidence": obj_meta.confidence,
                "top": rect.top,
                "left": rect.left,
                "width": rect.width,
                "height": rect.height,
                "sensor_id": SENSOR_ID,
                "mission_id": MISSION_ID,
                "location_id": LOCATION,
                "timestamp": current_time,
                "latitude": LATITUDE,
                "longitude": LONGITUDE
            }

            print(f"[Debug] Enqueuing data to Kafka: {message_dict}", flush=True)
            # Enqueue the message for Kafka processing
            message_queue.put(message_dict)

    return Gst.PadProbeReturn.OK
