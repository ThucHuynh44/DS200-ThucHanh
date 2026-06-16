# Bài Thực Hành 5: Hệ Thống Đếm Số Lượng Người Hiện Diện Trong Camera (Big Data Architecture)

Hệ thống này được thiết kế và xây dựng dựa trên kiến trúc xử lý luồng dữ liệu thời gian thực (Real-time Data Streaming) trong ngữ cảnh dữ liệu lớn (Big Data). Hệ thống thu thập dữ liệu từ camera, truyền tải qua broker, xử lý nhận diện đối tượng bằng mô hình AI, và lưu trữ dữ liệu tập trung để phục vụ phân tích.

---

## 1. Kiến Trúc Hệ Thống

Kiến trúc bao gồm 3 máy chủ chính hoạt động độc lập kết nối qua Broker truyền tin và Database:

```
[ Camera Server ] ---> (Kafka: camera-frames) ---> [ Processing Server ]
                                                           | (Chạy YOLOv8)
                                                           v
[ Storage Server ] <--- (Kafka: detection-results) <------+ (Gửi Bounding Box + Person Count)
       |
       v
[ MongoDB ] <====== [ Streamlit Dashboard ] (Web UI giám sát thời gian thực)
     ||
     ++=======> [ PySpark Analytics ] (Phân tích dữ liệu lớn bằng Spark SQL)
     ||
     ++=======> [ save_results.py ] (Xuất kết quả ra CSV & báo cáo)
```

1. **Camera Server (Producer):** Đọc các khung hình từ camera (webcam, video file mẫu hoặc bộ tạo ảnh giả lập), mã hóa thành định dạng JPEG và gửi liên tục lên Kafka topic `camera-frames` dưới dạng byte.
2. **Processing Server (Worker):**
   - Đăng ký nhận dữ liệu (Subscribe) từ Kafka topic `camera-frames`.
   - Sử dụng thư viện **YOLOv8** (`ultralytics`) để phát hiện con người (Class `person`).
   - Vẽ khung đóng (bounding box) và **đếm số lượng người** hiện diện trong mỗi khung hình.
   - Ghi ảnh đã vẽ khung đóng ra đĩa để phục vụ giao diện hiển thị.
   - Xuất kết quả `person_count`, tọa độ bounding box, thời gian xử lý thành chuỗi JSON gửi lên Kafka topic `detection-results`.
3. **Storage Server (Consumer & DB Ingestion):**
   - Đăng ký nhận dữ liệu từ Kafka topic `detection-results`.
   - Lưu trữ các gói JSON (bao gồm số người mỗi frame) vào cơ sở dữ liệu **MongoDB**.
   - Ghi nhận nhật ký (log) trực quan dạng bảng ra console.
4. **Streamlit Dashboard (Giao diện trực quan):**
   - Đọc cơ sở dữ liệu MongoDB và tệp ảnh đã xử lý gần nhất.
   - Hiển thị trực tiếp khung hình camera kèm bounding box của YOLOv8.
   - Biểu diễn biểu đồ thống kê số lượng người thay đổi theo thời gian thực (Line Chart).
   - Hiển thị thông số độ trễ hệ thống và bảng dữ liệu raw lưu trong MongoDB.
5. **PySpark Analytics (Phân tích dữ liệu lớn):**
   - Kết nối dữ liệu từ MongoDB, nạp vào Spark DataFrame.
   - Thực thi các câu truy vấn xử lý thống kê phân tích: tổng số khung hình, trung bình số người, khung hình đạt đỉnh (peak), và sử dụng **Spark Window Functions** để tính toán trung bình trượt (rolling average).
   - Tự động tải Hadoop Winutils cho Windows (không cần cài thủ công).
   - Xuất kết quả ra `spark_outputs/` dạng CSV.
6. **Save Results (Xuất kết quả):**
   - Kết nối MongoDB và xuất toàn bộ log phát hiện người ra `results/detection_log.csv`.
   - Tạo báo cáo thống kê tổng hợp `results/summary_report.txt`.

---

## 2. Công Nghệ Sử Dụng

| Công nghệ | Mục đích |
|---|---|
| **Apache Kafka** | Broker truyền tin phân tán, publish-subscribe, KRaft mode (không cần Zookeeper) |
| **MongoDB** | Cơ sở dữ liệu NoSQL lưu trữ kết quả phát hiện dạng JSON |
| **Mongo Express** | Giao diện quản trị MongoDB trực quan qua web |
| **YOLOv8 (Ultralytics)** | Mô hình Deep Learning nhận diện và đếm người thời gian thực |
| **Apache Spark (PySpark)** | Phân tích dữ liệu lớn, tính toán phân tán với Window Functions |
| **Streamlit** | Web Dashboard giám sát real-time |
| **OpenCV (cv2)** | Xử lý hình ảnh, vẽ bounding box, mã hóa JPEG |
| **Docker** | Container hóa Kafka, MongoDB, Mongo Express |

---

## 3. Cấu Trúc Thư Mục

```
BaiThucHanh5/
├── camera_server.py        # Server 1: Đọc video/webcam, gửi frame lên Kafka
├── processing_server.py    # Server 2: Nhận frame, chạy YOLOv8, đếm người
├── storage_server.py       # Server 3: Nhận kết quả, lưu vào MongoDB
├── dashboard.py            # Streamlit Web UI giám sát real-time
├── spark_analytics.py      # PySpark Big Data analytics + Window functions
├── save_results.py         # Xuất kết quả từ MongoDB ra CSV + báo cáo
├── docker-compose.yml      # Cấu hình Kafka, MongoDB, Mongo Express
├── requirements.txt        # Danh sách thư viện Python
├── run_all.bat             # Script menu chạy toàn hệ thống (Windows)
├── results/
│   ├── detection_log.csv   # Log chi tiết đếm người từng frame
│   └── summary_report.txt  # Báo cáo thống kê tổng hợp
└── spark_outputs/
    ├── summary_stats/      # Thống kê tổng quan (CSV)
    ├── rolling_averages/   # Trung bình trượt 5-frame (CSV)
    └── peak_frames/        # Các frame có nhiều người nhất (CSV)
```

---

## 4. Hướng Dẫn Cài Đặt

### Yêu cầu hệ thống:
- **Python 3.10+** (đã kiểm tra trên Python 3.13)
- **Docker Desktop** đang chạy
- **Java JDK 11** trở lên (phục vụ chạy PySpark)

### Các bước cài đặt:

**Bước 1:** Khởi động Docker Containers:
```bash
docker compose up -d
```

**Bước 2:** Tạo môi trường ảo và cài đặt thư viện:
```bash
python -m venv venv
.\venv\Scripts\pip install -r requirements.txt
```
> Lần đầu sẽ tải ~1.5GB gồm PyTorch, PySpark, YOLOv8. Cần kết nối internet ổn định.

---

## 5. Hướng Dẫn Vận Hành

Mở **4 terminal riêng biệt**, chạy theo thứ tự:

### Terminal 1 — Storage Server (lắng nghe và ghi vào MongoDB)
```bash
.\venv\Scripts\python storage_server.py
```

### Terminal 2 — Processing Server (YOLOv8 nhận diện và đếm người)
```bash
.\venv\Scripts\python processing_server.py
```
> Lần đầu sẽ tải tự động `yolov8n.pt` (~6MB).

### Terminal 3 — Camera Server (gửi frames từ video)
```bash
.\venv\Scripts\python camera_server.py --source video
```
- `--source video`: Tải và phát video mẫu người đi bộ (tự động tải từ GitHub)
- `--source webcam`: Dùng webcam thực tế
- `--source synthetic`: Sinh hình ảnh giả lập (không cần internet)

### Terminal 4 — Streamlit Dashboard (giao diện web)
```bash
.\venv\Scripts\streamlit run dashboard.py
```
Truy cập: **http://localhost:8501**

---

## 6. Phân Tích Dữ Liệu Lớn (PySpark)

Sau khi pipeline đã chạy và có dữ liệu trong MongoDB:

```bash
.\venv\Scripts\python spark_analytics.py
```

**Kết quả phân tích bao gồm:**
- Tổng số frame xử lý, tổng lượt phát hiện người
- Trung bình, max số người per frame
- **Rolling Average 5-frame** (Spark Window Function)
- Frame có nhiều người nhất (peak detection)

Kết quả xuất ra thư mục `spark_outputs/`.

---

## 7. Xuất Kết Quả Ra File

Để xuất toàn bộ dữ liệu từ MongoDB ra file CSV và báo cáo:

```bash
.\venv\Scripts\python save_results.py
```

**Output:**
- `results/detection_log.csv` — Log chi tiết từng frame: frame_id, thời gian, số người, latency
- `results/summary_report.txt` — Báo cáo tổng hợp thống kê

---

## 8. Kết Quả Thực Tế

Hệ thống đã được kiểm thử và xử lý thành công **1,935 frames** từ video người đi bộ:

| Thống kê | Kết quả |
|---|---|
| Tổng frames xử lý | 1,935 |
| Tổng lượt phát hiện người | 1,361 |
| Tỉ lệ frame có người | 40.9% |
| Số người trung bình/frame | 0.70 |
| Số người cao nhất (peak) | **4 người** |
| Latency YOLO trung bình | **58.0 ms** (~17 FPS) |

**Phân phối số người per frame:**
```
0 người: 1,143 frames (59.1%)
1 người:   397 frames (20.5%)
2 người:   236 frames (12.2%)
3 người:   144 frames ( 7.4%)
4 người:    15 frames ( 0.8%)
```

---

## 9. Kiểm Tra Dữ Liệu Trong MongoDB

Truy cập **Mongo Express** để xem trực tiếp dữ liệu:
- URL: **http://localhost:8081**
- Tài khoản: `admin` / Mật khẩu: `admin`
- Database: `camera_analytics` → Collection: `detections`

---

## 10. Script Hỗ Trợ (Windows)

File `run_all.bat` cung cấp menu tương tác:
```
1. Khởi động Docker (Kafka + MongoDB)
2. Cài đặt môi trường Python
3. Chạy Camera Server
4. Chạy Processing Server (YOLOv8)
5. Chạy Storage Server (MongoDB)
6. Chạy Streamlit Dashboard
7. Chạy PySpark Analytics
8. Dừng tất cả Docker containers
```
