import os
import sys
import time
import argparse
import urllib.request
import cv2
import json

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

from kafka import KafkaProducer

# Default configuration
KAFKA_BROKER = "localhost:9092"
TOPIC_NAME = "camera-frames"
DEFAULT_VIDEO_URL = "https://raw.githubusercontent.com/intel-iot-devkit/sample-videos/master/people-detection.mp4"
DEFAULT_VIDEO_PATH = "people-detection.mp4"

def download_sample_video(url, dest_path):
    """Downloads a sample video of people walking to use for testing."""
    if not os.path.exists(dest_path):
        print(f"Sample video not found locally. Downloading from:\n{url} ...")
        try:
            # Add a user-agent header to avoid getting blocked by some CDNs
            opener = urllib.request.build_opener()
            opener.addheaders = [('User-agent', 'Mozilla/5.0')]
            urllib.request.install_opener(opener)
            urllib.request.urlretrieve(url, dest_path)
            print(f"Download complete! Saved to {dest_path}")
        except Exception as e:
            print(f"Error downloading sample video: {e}")
            print("The system will attempt to fallback to webcam (device 0) or generate synthetic frames.")
            return False
    return True

def generate_synthetic_frame(frame_id):
    """Fallback generator for synthetic frames when video/webcam is unavailable."""
    import numpy as np
    # Create a dark green yard/street background
    img = np.zeros((480, 640, 3), dtype=np.uint8)
    img[:, :] = [40, 60, 40]  # dark green/gray color
    
    # Draw a simulated sidewalk/road
    cv2.rectangle(img, (0, 300), (640, 480), (80, 80, 80), -1)  # grey road
    cv2.line(img, (0, 300), (640, 300), (200, 200, 200), 2)  # curb line
    
    # Draw simulated walking "people" (circles/rectangles with IDs)
    # They move horizontally across the screen
    num_people = 3
    for i in range(num_people):
        x = int((100 + i * 200 + frame_id * (10 + i * 5)) % 640)
        y = int(320 + i * 40)
        # Bounding box simulation
        w, h = 40, 80
        # Draw simulated person body
        cv2.rectangle(img, (x - w//2, y - h//2), (x + w//2, y + h//2), (0, 255, 0), 2)
        cv2.circle(img, (x, y - h//2 + 10), 10, (0, 0, 255), -1)  # simulated head
        cv2.putText(img, f"Person {i+1}", (x - 20, y - h//2 - 5), 
                    cv2.FONT_HERSHEY_SIMPLEX, 0.4, (255, 255, 255), 1)
        
    cv2.putText(img, f"SYNTHETIC STREAM - FRAME {frame_id}", (20, 40), 
                cv2.FONT_HERSHEY_SIMPLEX, 0.7, (0, 255, 255), 2)
    return img

def main():
    parser = argparse.ArgumentParser(description="Camera Server (Kafka Producer)")
    parser.add_argument("--broker", default=KAFKA_BROKER, help="Kafka broker address")
    parser.add_argument("--topic", default=TOPIC_NAME, help="Kafka topic for frames")
    parser.add_argument("--source", default="video", choices=["video", "webcam", "synthetic"], 
                        help="Video source: 'video' (file), 'webcam', or 'synthetic' (generated)")
    parser.add_argument("--fps", type=float, default=2.0, help="Frames per second to stream")
    parser.add_argument("--video-file", default=DEFAULT_VIDEO_PATH, help="Path to video file")
    
    args = parser.parse_args()
    
    print("--------------------------------------------------")
    print("Camera Server (Kafka Producer) starting...")
    print(f"Broker: {args.broker}")
    print(f"Topic: {args.topic}")
    print(f"Source: {args.source}")
    print(f"Target FPS: {args.fps}")
    print("--------------------------------------------------")
    
    # Initialize Kafka Producer
    # Retries connection if Kafka is still booting up
    producer = None
    for attempt in range(1, 11):
        try:
            producer = KafkaProducer(
                bootstrap_servers=[args.broker],
                max_request_size=5000000  # allow larger frames (up to 5MB)
            )
            print("Successfully connected to Kafka broker!")
            break
        except Exception as e:
            print(f"Connection attempt {attempt}/10 failed: {e}")
            print("Retrying in 3 seconds...")
            time.sleep(3)
            
    if not producer:
        print("Could not connect to Kafka. Exiting.")
        sys.exit(1)
        
    cap = None
    use_synthetic = False
    
    if args.source == "video":
        # Ensure sample video exists
        download_sample_video(DEFAULT_VIDEO_URL, args.video_file)
        cap = cv2.VideoCapture(args.video_file)
        if not cap.isOpened():
            print(f"Warning: Could not open video file '{args.video_file}'. Falling back to synthetic stream.")
            use_synthetic = True
            
    elif args.source == "webcam":
        cap = cv2.VideoCapture(0)
        if not cap.isOpened():
            print("Warning: Could not access webcam. Falling back to synthetic stream.")
            use_synthetic = True
            
    else:
        use_synthetic = True

    frame_id = 0
    delay = 1.0 / args.fps
    
    try:
        while True:
            start_time = time.time()
            frame_id += 1
            
            if use_synthetic:
                frame = generate_synthetic_frame(frame_id)
            else:
                ret, frame = cap.read()
                if not ret:
                    # Loop the video
                    cap.set(cv2.CAP_PROP_POS_FRAMES, 0)
                    ret, frame = cap.read()
                    if not ret:
                        print("Error: Could not read video even after resetting. Switching to synthetic.")
                        use_synthetic = True
                        continue
            
            # Resize frame to a standard size (e.g., 640x480) to save network bandwidth and speed up detection
            frame = cv2.resize(frame, (640, 480))
            
            # Encode frame as JPEG
            ret, buffer = cv2.imencode('.jpg', frame)
            if not ret:
                print(f"Failed to encode frame {frame_id}")
                continue
                
            frame_bytes = buffer.tobytes()
            timestamp = time.time()
            
            # Send message. We put metadata in the Kafka headers or as a prefix
            # In this implementation, we send frame bytes as the payload, and metadata as headers.
            # Headers must be list of tuples (key_string, value_bytes)
            headers = [
                ("frame_id", str(frame_id).encode('utf-8')),
                ("timestamp", str(timestamp).encode('utf-8')),
                ("source", args.source.encode('utf-8'))
            ]
            
            # Send asynchronously
            producer.send(
                topic=args.topic,
                value=frame_bytes,
                headers=headers
            )
            
            print(f"[{time.strftime('%H:%M:%S')}] Sent Frame #{frame_id} ({len(frame_bytes)/1024:.1f} KB) - Source: {args.source}")
            
            # Calculate sleep to maintain target FPS
            elapsed = time.time() - start_time
            sleep_time = max(0.01, delay - elapsed)
            time.sleep(sleep_time)
            
    except KeyboardInterrupt:
        print("\nStopping Camera Server...")
    finally:
        if cap is not None:
            cap.release()
        if producer is not None:
            producer.close()
        print("Camera Server stopped.")

if __name__ == "__main__":
    main()
