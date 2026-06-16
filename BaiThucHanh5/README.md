# Bài Thực Hành 5: Hệ Thống Đếm Số Lượng Người Hiện Diện Trong Camera (Big Data Architecture)

Hệ thống này được thiết kế và xây dựng dựa trên kiến trúc xử lý luồng dữ liệu thời gian thực (Real-time Data Streaming) trong ngữ cảnh dữ liệu lớn (Big Data). Hệ thống thu thập dữ liệu từ camera, truyền tải qua broker, xử lý nhận diện đối tượng bằng mô hình AI, và lưu trữ dữ liệu tập trung để phục vụ phân tích.

---

## 1. Kiến Trúc Hệ Thống

Kiến trúc bao gồm 3 máy chủ chính hoạt động độc lập kết nối qua Broker truyền tin và Database:

```
[ Camera Server ] ---> (Kafka: camera-frames) ---> [ Processing Server ]
                                                           | (Chạy YOLOv8)
                                                           v
[ Storage Server ] <--- (Kafka: detection-results) <-------+ (Gửi Bounding Box)
       |
       v
[ MongoDB ] <====== [ Streamlit Dashboard ] (Web UI giám sát thời gian thực)
     ||
     ++=======> [ PySpark Analytics ] (Phân tích dữ liệu lớn bằng Spark SQL)
```

1. **Camera Server (Producer):** Đọc các khung hình từ camera (webcam, video file mẫu hoặc bộ tạo ảnh giả lập), mã hóa thành định dạng JPEG và gửi liên tục lên Kafka topic `camera-frames` dưới dạng byte.
2. **Processing Server (Worker):** 
   - Đăng ký nhận dữ liệu (Subscribe) từ Kafka topic `camera-frames`.
   - Sử dụng thư viện **YOLOv8** (`ultralytics`) để phát hiện con người (Class `person`).
   - Vẽ khung đóng (bounding box) và tính toán số người.
   - Ghi ảnh đã vẽ khung đóng ra đĩa để phục vụ giao diện hiển thị.
   - Xuất kết quả tọa độ bounding box, số lượng người, thời gian xử lý thành chuỗi JSON gửi lên Kafka topic `detection-results`.
3. **Storage Server (Consumer & DB Ingestion):**
   - Đăng ký nhận dữ liệu từ Kafka topic `detection-results`.
   - Lưu trữ các gói JSON vào cơ sở dữ liệu phi quan hệ (NoSQL) **MongoDB**.
   - Ghi nhận nhật ký (log) trực quan dạng bảng ra console.
4. **Streamlit Dashboard (Giao diện trực quan):**
   - Đọc cơ sở dữ liệu MongoDB và tệp ảnh đã xử lý gần nhất.
   - Hiển thị trực tiếp khung hình camera kèm bounding box của YOLOv8.
   - Biểu diễn biểu đồ thống kê số lượng người thay đổi theo thời gian thực (Line Chart).
   - Hiển thị thông số độ trễ hệ thống và bảng dữ liệu raw lưu trong MongoDB.
5. **PySpark Analytics (Phân tích dữ liệu lớn):**
   - Kết nối dữ liệu từ MongoDB, nạp vào Spark DataFrame.
   - Thực thi các câu truy vấn xử lý thống kê phân tích: tổng số khung hình, trung bình số người, khung giờ/khung hình đạt đỉnh (peak), và sử dụng **Spark Window Functions** để tính toán trung bình trượt (rolling average) làm mượt dữ liệu.

---

## 2. Công Nghệ Sử Dụng

- **Apache Kafka:** Nền tảng phân phối tin nhắn dạng publish-subscribe hiệu năng cao, phân tán và chịu lỗi tốt (chạy chế độ KRaft gọn nhẹ, không cần Zookeeper).
- **MongoDB:** Hệ quản trị cơ sở dữ liệu NoSQL hướng tài liệu, phù hợp lưu trữ dữ liệu dạng JSON từ camera với quy mô lớn.
- **Mongo Express:** Giao diện quản trị MongoDB trực quan dựa trên web.
- **YOLOv8 (Ultralytics):** Mô hình Deep Learning nhận diện vật thể thời gian thực tối tân, chính xác và có hiệu năng cao.
- **Apache Spark (PySpark):** Công cụ phân tích dữ liệu lớn, tính toán phân tán.
- **Streamlit:** Thư viện Python giúp xây dựng nhanh các ứng dụng Web Dashboard đẹp mắt, trực quan và cập nhật thời gian thực.
- **OpenCV (cv2):** Thư viện thị giác máy tính xử lý hình ảnh và luồng video.

---

## 3. Hướng Dẫn Cài Đặt

### Yêu cầu hệ thống:
- Đã cài đặt **Python (phiên bản 3.10 trở lên)** và biến môi trường `python` hoạt động.
- Đã cài đặt **Docker** và **Docker Desktop** đang chạy.
- Đã cài đặt **Java JDK 11** trở lên (phục vụ chạy PySpark).

### Các bước cài đặt:
Dự án cung cấp tệp script tự động `run_all.bat` giúp thực hiện tất cả các thao tác dễ dàng trên Windows.

1. **Khởi động Docker Containers:**
   Chạy `run_all.bat` và chọn tùy chọn **`1`** để tải và khởi động các dịch vụ Kafka, MongoDB, Mongo Express.
   *(Hoặc gõ lệnh: `docker compose up -d`)*

2. **Cấu hình Môi Trường Python ^& Cài Đặt Thư Viện:**
   Chọn tùy chọn **`2`** trong `run_all.bat`. Script sẽ tự động tạo thư mục môi trường ảo `venv` và cài đặt các thư viện từ `requirements.txt` (bao gồm PySpark, Ultralytics YOLOv8, OpenCV, Streamlit, PyMongo).
   *(Hoặc gõ lệnh:*
   ```bash
   python -m venv venv
   .\venv\Scripts\pip install -r requirements.txt
   ```
   *)*

---

## 4. Hướng Dẫn Vận Hành Hệ Thống

Để quan sát hệ thống chạy liên tục, hãy mở **5 cửa sổ Terminal** (hoặc dùng menu `run_all.bat` trong các terminal riêng biệt):

### Bước 1: Chạy Storage Server (Để lắng nghe ghi vào DB)
Mở một terminal và chạy:
```bash
.\venv\Scripts\python storage_server.py
```
*(Hoặc chọn tùy chọn **`5`** trong `run_all.bat`)*

### Bước 2: Chạy Processing Server (Nhận diện YOLOv8)
Mở terminal thứ hai và chạy:
```bash
.\venv\Scripts\python processing_server.py
```
*(Hoặc chọn tùy chọn **`4`** trong `run_all.bat`)*
*Lưu ý: Trong lần chạy đầu tiên, script sẽ tự động tải tệp trọng số YOLOv8 (`yolov8n.pt`, ~6MB) từ internet về.*

### Bước 3: Chạy Camera Server (Gửi khung hình)
Mở terminal thứ ba và chạy:
```bash
.\venv\Scripts\python camera_server.py --source video
```
*(Hoặc chọn tùy chọn **`3`** trong `run_all.bat`)*
- `video`: Sẽ tự động tải tệp video mẫu người đi bộ từ github (`people-detection.mp4`) để chạy thử nghiệm thực tế.
- `webcam`: Nếu bạn có webcam, đổi tham số thành `--source webcam`.
- `synthetic`: Nếu không có internet hoặc camera, đổi thành `--source synthetic` để sinh luồng hình ảnh giả lập.

### Bước 4: Khởi động Dashboard (Giao diện Web giám sát)
Mở terminal thứ tư và chạy:
```bash
.\venv\Scripts\streamlit run dashboard.py
```
*(Hoặc chọn tùy chọn **`6`** trong `run_all.bat`)*
Trình duyệt sẽ tự động mở trang: `http://localhost:8501`. Tại đây bạn sẽ thấy hình ảnh camera trực tiếp có vẽ khung xanh xung quanh người kèm theo đồ thị phân tích thời gian thực.

### Bước 5: Kiểm tra Dữ Liệu và Phân Tích (PySpark)
1. **Tru cập Mongo Express (Web UI của MongoDB):**
   Mở trình duyệt truy cập `http://localhost:8081` (Tài khoản: `admin` / Mật khẩu: `admin`) để xem các bản ghi JSON lưu trữ trong database `camera_analytics`, collection `detections`.
   
2. **Chạy Phân Tích PySpark:**
   Sau khi hệ thống chạy được một khoảng thời gian (khoảng 30-50 khung hình), mở terminal thứ năm và chạy:
   ```bash
   .\venv\Scripts\python spark_analytics.py
   ```
   *(Hoặc chọn tùy chọn **`7`** trong `run_all.bat`)*
   Spark sẽ thực hiện truy vấn phân tích dữ liệu lớn từ MongoDB, in kết quả thống kê trung bình trượt (rolling average) và xuất báo cáo CSV ra thư mục `spark_outputs/`.
