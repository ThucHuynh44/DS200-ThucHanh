import os
import sys
import time
import json
import argparse
import cv2
import numpy as np

# Monkey-patch selectors to fix Python 3.12/3.13 bug with kafka-python on Windows
if sys.platform == 'win32':
    import selectors as _selectors

    # Wrap the entire SelectSelector class to suppress all socket-cleanup errors
    _OriginalSelectSelector = _selectors.SelectSelector

    class _SafeSelectSelector(_OriginalSelectSelector):
        def unregister(self, fileobj):
            try:
                return super().unregister(fileobj)
            except Exception:
                return None

        def select(self, timeout=None):
            try:
                return super().select(timeout)
            except Exception:
                return []

    _selectors.SelectSelector = _SafeSelectSelector
    _selectors.DefaultSelector = _SafeSelectSelector

from kafka import KafkaConsumer, KafkaProducer

# Default configuration
KAFKA_BROKER = "localhost:9092"
CONSUME_TOPIC = "camera-frames"
PRODUCE_TOPIC = "detection-results"
ANNOTATED_FRAME_PATH = "latest_processed.jpg"

def main():
    parser = argparse.ArgumentParser(description="Processing Server (YOLO Detector)")
    parser.add_argument("--broker", default=KAFKA_BROKER, help="Kafka broker address")
    parser.add_argument("--consume-topic", default=CONSUME_TOPIC, help="Kafka topic to consume frames from")
    parser.add_argument("--produce-topic", default=PRODUCE_TOPIC, help="Kafka topic to send detection results to")
    parser.add_argument("--model", default="yolov8n.pt", help="YOLO model version (e.g., yolov8n.pt)")
    
    args = parser.parse_args()
    
    print("--------------------------------------------------")
    print("Processing Server (YOLO Detector) starting...")
    print(f"Broker: {args.broker}")
    print(f"Consume Topic: {args.consume_topic}")
    print(f"Produce Topic: {args.produce_topic}")
    print(f"YOLO Model: {args.model}")
    print("--------------------------------------------------")
    
    # Initialize YOLO detector
    # This will automatically download yolov8n.pt on the first run (approx. 6MB)
    try:
        from ultralytics import YOLO
        print("Loading YOLOv8 model...")
        model = YOLO(args.model)
        print("YOLOv8 model loaded successfully!")
    except Exception as e:
        print(f"Error loading YOLOv8: {e}")
        print("Falling back to OpenCV HOG Descriptor for person detection.")
        model = None
        
    # Initialize Kafka Producer
    producer = None
    for attempt in range(1, 11):
        try:
            producer = KafkaProducer(
                bootstrap_servers=[args.broker],
                value_serializer=lambda v: json.dumps(v).encode('utf-8')
            )
            print("Successfully connected to Kafka producer!")
            break
        except Exception as e:
            print(f"Producer connection attempt {attempt}/10 failed: {e}")
            print("Retrying in 3 seconds...")
            time.sleep(3)
            
    if not producer:
        print("Could not connect Kafka producer. Exiting.")
        sys.exit(1)
        
    # Initialize Kafka Consumer
    consumer = None
    for attempt in range(1, 11):
        try:
            consumer = KafkaConsumer(
                args.consume_topic,
                bootstrap_servers=[args.broker],
                auto_offset_reset='latest',
                enable_auto_commit=True,
                group_id='yolo-processing-group',
                max_partition_fetch_bytes=5000000  # handle large frames
            )
            print("Successfully connected to Kafka consumer!")
            break
        except Exception as e:
            print(f"Consumer connection attempt {attempt}/10 failed: {e}")
            print("Retrying in 3 seconds...")
            time.sleep(3)
            
    if not consumer:
        print("Could not connect Kafka consumer. Exiting.")
        sys.exit(1)
        
    # Standard OpenCV HOG detector fallback setup
    hog = None
    if model is None:
        hog = cv2.HOGDescriptor()
        hog.setSVMDetector(cv2.HOGDescriptor_getDefaultPeopleDetector())
        
    print("Processing loop started. Waiting for frames...")
    
    try:
        for message in consumer:
            start_time = time.time()
            
            # Extract headers
            frame_id = "unknown"
            frame_timestamp = time.time()
            source = "unknown"
            
            if message.headers:
                for key, val in message.headers:
                    if key == "frame_id":
                        frame_id = int(val.decode('utf-8'))
                    elif key == "timestamp":
                        frame_timestamp = float(val.decode('utf-8'))
                    elif key == "source":
                        source = val.decode('utf-8')
            
            # Decode JPEG image bytes
            nparr = np.frombuffer(message.value, np.uint8)
            frame = cv2.imdecode(nparr, cv2.IMREAD_COLOR)
            
            if frame is None:
                print(f"Error: Could not decode frame #{frame_id}")
                continue
                
            detections = []
            
            if model is not None:
                # Run YOLOv8 detection
                # class 0 is "person" in COCO dataset
                results = model.predict(source=frame, classes=[0], verbose=False)
                
                # Extract results
                if len(results) > 0:
                    result = results[0]
                    boxes = result.boxes
                    for box in boxes:
                        # Bounding box coordinates
                        x1, y1, x2, y2 = box.xyxy[0].tolist()
                        conf = box.conf[0].item()
                        detections.append({
                            "box": [int(x1), int(y1), int(x2), int(y2)],
                            "confidence": float(conf)
                        })
                        
                        # Draw bounding box on frame
                        cv2.rectangle(frame, (int(x1), int(y1)), (int(x2), int(y2)), (0, 255, 0), 2)
                        label = f"Person {conf:.2f}"
                        cv2.putText(frame, label, (int(x1), int(y1) - 10),
                                    cv2.FONT_HERSHEY_SIMPLEX, 0.5, (0, 255, 0), 2)
            else:
                # Fallback HOG Detector
                # Resize image slightly to improve performance
                gray = cv2.cvtColor(frame, cv2.COLOR_BGR2GRAY)
                boxes, weights = hog.detectMultiScale(gray, winStride=(8, 8), padding=(8, 8), scale=1.05)
                for (x, y, w, h), weight in zip(boxes, weights):
                    detections.append({
                        "box": [int(x), int(y), int(x + w), int(y + h)],
                        "confidence": float(weight)
                    })
                    # Draw
                    cv2.rectangle(frame, (x, y), (x + w, y + h), (0, 0, 255), 2)
                    cv2.putText(frame, f"Person {weight:.2f}", (x, y - 10),
                                cv2.FONT_HERSHEY_SIMPLEX, 0.5, (0, 0, 255), 2)
            
            person_count = len(detections)
            process_latency = time.time() - start_time
            total_latency = time.time() - frame_timestamp
            
            # Draw overlay text for metadata
            cv2.putText(frame, f"Count: {person_count}", (20, 40),
                        cv2.FONT_HERSHEY_SIMPLEX, 0.9, (0, 255, 255), 2)
            cv2.putText(frame, f"FPS: {1.0/max(0.001, process_latency):.1f} | Latency: {total_latency*1000:.0f}ms", 
                        (20, 70), cv2.FONT_HERSHEY_SIMPLEX, 0.5, (0, 255, 255), 1)
            cv2.putText(frame, f"Frame: #{frame_id}", (20, 95),
                        cv2.FONT_HERSHEY_SIMPLEX, 0.5, (255, 255, 255), 1)
            
            # Save the annotated frame to disk so the dashboard can load it
            cv2.imwrite(ANNOTATED_FRAME_PATH, frame)
            
            # Create output JSON message
            result_payload = {
                "frame_id": frame_id,
                "timestamp": frame_timestamp,
                "person_count": person_count,
                "detections": detections,
                "process_latency_seconds": process_latency,
                "total_latency_seconds": total_latency
            }
            
            # Publish results
            producer.send(args.produce_topic, value=result_payload)
            
            print(f"[{time.strftime('%H:%M:%S')}] Processed Frame #{frame_id} - "
                  f"People Count: {person_count} - "
                  f"Process Time: {process_latency*1000:.1f}ms - Total Latency: {total_latency*1000:.1f}ms")
                  
    except KeyboardInterrupt:
        print("\nStopping Processing Server...")
    finally:
        if consumer is not None:
            consumer.close()
        if producer is not None:
            producer.close()
        print("Processing Server stopped.")

if __name__ == "__main__":
    main()
