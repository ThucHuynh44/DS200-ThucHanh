"""
save_results.py
---------------
Xuất kết quả từ MongoDB ra các file CSV và báo cáo tóm tắt.
Chạy sau khi đã có dữ liệu trong MongoDB (sau khi chạy pipeline).

Sử dụng:
    python save_results.py

Kết quả lưu tại:
    results/
    ├── detection_log.csv      # Toàn bộ log phát hiện người theo frame
    ├── summary_report.txt     # Báo cáo tóm tắt thống kê
    └── annotated_frames/      # Ảnh frame đã được annotate (nếu có)
"""

import os
import sys
import csv
import json
from datetime import datetime
from pymongo import MongoClient

MONGO_URI = "mongodb://localhost:27017/"
DB_NAME = "camera_analytics"
COLLECTION_NAME = "detections"
OUTPUT_DIR = "results"


def connect_mongodb():
    try:
        client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
        client.server_info()
        return client
    except Exception as e:
        print(f"[ERROR] Không thể kết nối MongoDB: {e}")
        print("Đảm bảo Docker đang chạy: docker compose up -d")
        sys.exit(1)


def export_detection_log(collection, output_path):
    """Xuất toàn bộ log phát hiện người ra file CSV."""
    print("Xuất detection_log.csv...")
    cursor = collection.find(
        {},
        {
            "_id": 0,
            "frame_id": 1,
            "timestamp": 1,
            "person_count": 1,
            "process_latency_seconds": 1,
            "total_latency_seconds": 1,
        }
    ).sort("frame_id", 1)

    records = list(cursor)
    if not records:
        print("  [WARNING] Không có dữ liệu trong MongoDB. Hãy chạy pipeline trước.")
        return 0

    with open(output_path, "w", newline="", encoding="utf-8") as f:
        fieldnames = ["frame_id", "timestamp", "datetime", "person_count",
                      "process_latency_ms", "total_latency_ms"]
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()

        for rec in records:
            ts = rec.get("timestamp", 0)
            try:
                dt_str = datetime.fromtimestamp(ts).strftime("%Y-%m-%d %H:%M:%S")
            except Exception:
                dt_str = "N/A"

            writer.writerow({
                "frame_id": rec.get("frame_id", ""),
                "timestamp": round(ts, 3),
                "datetime": dt_str,
                "person_count": rec.get("person_count", 0),
                "process_latency_ms": round(rec.get("process_latency_seconds", 0) * 1000, 2),
                "total_latency_ms": round(rec.get("total_latency_seconds", 0) * 1000, 2),
            })

    print(f"  -> Đã lưu {len(records)} bản ghi vào {output_path}")
    return records


def generate_summary_report(records, output_path):
    """Tạo báo cáo tóm tắt thống kê."""
    print("Tạo summary_report.txt...")

    total_frames = len(records)
    if total_frames == 0:
        return

    counts = [r.get("person_count", 0) for r in records]
    latencies = [r.get("process_latency_seconds", 0) * 1000 for r in records]

    total_detections = sum(counts)
    avg_count = total_detections / total_frames
    max_count = max(counts)
    min_count = min(counts)
    avg_latency = sum(latencies) / total_frames
    max_latency = max(latencies)

    frames_with_people = [r for r in records if r.get("person_count", 0) > 0]
    detection_rate = len(frames_with_people) / total_frames * 100

    # Tìm frame có nhiều người nhất
    peak_frame = max(records, key=lambda x: x.get("person_count", 0))
    peak_ts = peak_frame.get("timestamp", 0)
    try:
        peak_time = datetime.fromtimestamp(peak_ts).strftime("%Y-%m-%d %H:%M:%S")
    except Exception:
        peak_time = "N/A"

    report_lines = [
        "=" * 60,
        "  BÁO CÁO KẾT QUẢ - HỆ THỐNG ĐẾM NGƯỜI TỪ CAMERA",
        "=" * 60,
        f"  Thời gian tạo báo cáo : {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
        "",
        "--- THỐNG KÊ TỔNG QUAN ---",
        f"  Tổng số frame xử lý   : {total_frames:,}",
        f"  Tổng lượt phát hiện   : {total_detections:,}",
        f"  Tỉ lệ frame có người  : {detection_rate:.1f}%",
        "",
        "--- SỐ LƯỢNG NGƯỜI (PER FRAME) ---",
        f"  Trung bình            : {avg_count:.2f} người/frame",
        f"  Cao nhất (peak)       : {max_count} người",
        f"  Thấp nhất             : {min_count} người",
        f"  Frame đông nhất       : #{peak_frame.get('frame_id', '?')} lúc {peak_time}",
        "",
        "--- HIỆU NĂNG XỬ LÝ ---",
        f"  Latency trung bình    : {avg_latency:.1f} ms",
        f"  Latency cao nhất      : {max_latency:.1f} ms",
        f"  FPS tương đương       : ~{1000/avg_latency:.1f} FPS",
        "",
        "--- PHÂN PHỐI SỐ NGƯỜI ---",
    ]

    # Phân phối số người per frame
    distribution = {}
    for c in counts:
        distribution[c] = distribution.get(c, 0) + 1
    for k in sorted(distribution.keys()):
        pct = distribution[k] / total_frames * 100
        bar = "#" * int(pct / 2)
        report_lines.append(f"  {k} người : {distribution[k]:5d} frames ({pct:5.1f}%) {bar}")

    report_lines += [
        "",
        "--- FILE KẾT QUẢ ---",
        "  results/detection_log.csv       - Log chi tiết từng frame",
        "  spark_outputs/summary_stats/    - Thống kê tổng hợp (Spark)",
        "  spark_outputs/rolling_averages/ - Rolling average (Spark)",
        "  spark_outputs/peak_frames/      - Các frame đông người (Spark)",
        "",
        "=" * 60,
    ]

    with open(output_path, "w", encoding="utf-8") as f:
        f.write("\n".join(report_lines))

    # In ra console luôn
    print()
    for line in report_lines:
        print(line)

    print(f"\n  -> Đã lưu báo cáo vào {output_path}")


def save_annotated_frame():
    """Copy frame annotate mới nhất (nếu có) vào thư mục results."""
    src = "latest_processed.jpg"
    if not os.path.exists(src):
        return

    import shutil
    frames_dir = os.path.join(OUTPUT_DIR, "annotated_frames")
    os.makedirs(frames_dir, exist_ok=True)

    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    dst = os.path.join(frames_dir, f"sample_detection_{ts}.jpg")
    shutil.copy2(src, dst)
    print(f"  -> Đã lưu frame mẫu vào {dst}")


def main():
    print("=" * 60)
    print("  SAVE RESULTS - Xuất kết quả ra file")
    print("=" * 60)

    # Tạo thư mục output
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    # Kết nối MongoDB
    client = connect_mongodb()
    collection = client[DB_NAME][COLLECTION_NAME]

    total_in_db = collection.count_documents({})
    print(f"\nTìm thấy {total_in_db:,} bản ghi trong MongoDB.\n")

    if total_in_db == 0:
        print("[WARNING] Chưa có dữ liệu. Hãy chạy pipeline trước:")
        print("  1. docker compose up -d")
        print("  2. python storage_server.py")
        print("  3. python processing_server.py")
        print("  4. python camera_server.py --source video")
        client.close()
        return

    # 1. Xuất detection log ra CSV
    log_path = os.path.join(OUTPUT_DIR, "detection_log.csv")
    records = export_detection_log(collection, log_path)

    # 2. Tạo báo cáo tóm tắt
    report_path = os.path.join(OUTPUT_DIR, "summary_report.txt")
    generate_summary_report(records, report_path)

    # 3. Lưu frame annotated mẫu
    save_annotated_frame()

    client.close()
    print("\n[DONE] Hoàn thành! Các file đã sẵn sàng để commit lên git.")
    print(f"       Thư mục: ./{OUTPUT_DIR}/")


if __name__ == "__main__":
    main()
