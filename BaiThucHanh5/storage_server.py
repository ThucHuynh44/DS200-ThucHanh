import os
import sys
import time
import json
import argparse
from datetime import datetime

# Monkey-patch selectors to fix Python 3.12/3.13 bug with kafka-python on Windows
if sys.platform == 'win32':
    import selectors
    orig_unregister = selectors.BaseSelector.unregister
    def patched_unregister(self, fileobj):
        try:
            return orig_unregister(self, fileobj)
        except ValueError as e:
            if "Invalid file descriptor" in str(e):
                return None
            raise
    selectors.BaseSelector.unregister = patched_unregister

from kafka import KafkaConsumer
from pymongo import MongoClient

# Default configuration
KAFKA_BROKER = "localhost:9092"
CONSUME_TOPIC = "detection-results"
MONGO_URI = "mongodb://localhost:27017/"
DB_NAME = "camera_analytics"
COLLECTION_NAME = "detections"

def print_row(frame_id, timestamp_str, count, latency_ms):
    """Prints a formatted row in the console terminal table."""
    print(f"| {str(frame_id):<10} | {timestamp_str:<21} | {str(count):<12} | {f'{latency_ms:.1f}ms':<13} |")

def main():
    parser = argparse.ArgumentParser(description="Storage Server (MongoDB Ingestion)")
    parser.add_argument("--broker", default=KAFKA_BROKER, help="Kafka broker address")
    parser.add_argument("--topic", default=CONSUME_TOPIC, help="Kafka topic for detection results")
    parser.add_argument("--mongo-uri", default=MONGO_URI, help="MongoDB connection URI")
    parser.add_argument("--db", default=DB_NAME, help="MongoDB database name")
    parser.add_argument("--collection", default=COLLECTION_NAME, help="MongoDB collection name")
    
    args = parser.parse_args()
    
    print("--------------------------------------------------")
    print("Storage Server (MongoDB Ingestion) starting...")
    print(f"Broker: {args.broker}")
    print(f"Topic: {args.topic}")
    print(f"Mongo URI: {args.mongo_uri}")
    print(f"Database: {args.db}")
    print(f"Collection: {args.collection}")
    print("--------------------------------------------------")
    
    # Initialize MongoDB Client
    mongo_client = None
    db = None
    collection = None
    for attempt in range(1, 11):
        try:
            mongo_client = MongoClient(args.mongo_uri, serverSelectionTimeoutMS=5000)
            # Check connection
            mongo_client.server_info()
            db = mongo_client[args.db]
            collection = db[args.collection]
            print("Successfully connected to MongoDB!")
            break
        except Exception as e:
            print(f"MongoDB connection attempt {attempt}/10 failed: {e}")
            print("Retrying in 3 seconds...")
            time.sleep(3)
            
    if mongo_client is None:
        print("Could not connect to MongoDB. Exiting.")
        sys.exit(1)
        
    # Initialize Kafka Consumer
    consumer = None
    for attempt in range(1, 11):
        try:
            consumer = KafkaConsumer(
                args.topic,
                bootstrap_servers=[args.broker],
                auto_offset_reset='latest',
                enable_auto_commit=True,
                group_id='mongodb-storage-group',
                value_deserializer=lambda x: json.loads(x.decode('utf-8'))
            )
            print("Successfully connected to Kafka consumer!")
            break
        except Exception as e:
            print(f"Consumer connection attempt {attempt}/10 failed: {e}")
            print("Retrying in 3 seconds...")
            time.sleep(3)
            
    if not consumer:
        print("Could not connect to Kafka consumer. Exiting.")
        sys.exit(1)
        
    # Print beautiful table header
    print("\n" + "="*68)
    print(f"| {'FRAME ID':<10} | {'TIMESTAMP':<21} | {'PERSON COUNT':<12} | {'LATENCY (ms)':<13} |")
    print("="*68)
    
    try:
        for message in consumer:
            payload = message.value
            
            # Format timestamp for displaying
            ts = payload.get("timestamp", time.time())
            dt_obj = datetime.fromtimestamp(ts)
            timestamp_str = dt_obj.strftime("%Y-%m-%d %H:%M:%S")
            
            frame_id = payload.get("frame_id", -1)
            person_count = payload.get("person_count", 0)
            latency_ms = payload.get("process_latency_seconds", 0) * 1000
            
            # Create a document for MongoDB
            # We add a native datetime object for easier querying and indexing
            document = payload.copy()
            document["inserted_at"] = datetime.utcnow()
            document["datetime"] = dt_obj
            
            # Insert into MongoDB
            try:
                collection.insert_one(document)
                # Print row
                print_row(frame_id, timestamp_str, person_count, latency_ms)
            except Exception as e:
                print(f"Error inserting document into MongoDB: {e}")
                
    except KeyboardInterrupt:
        print("\nStopping Storage Server...")
    finally:
        if consumer is not None:
            consumer.close()
        if mongo_client is not None:
            mongo_client.close()
        print("Storage Server stopped.")

if __name__ == "__main__":
    main()
