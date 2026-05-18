# -*- coding: utf-8 -*-
"""
Fecom Inc. Spark DataFrame Assignment
Generated from assignments_solved.ipynb

Cách chạy:
1. Đặt file này cùng thư mục với 5 file CSV:
   Customer_List.csv, Order_Items.csv, Order_Reviews.csv, Orders.csv, Products.csv
2. Chạy bằng spark-submit assignments_solved.py hoặc chạy trong môi trường có PySpark.
3. Kết quả sẽ được lưu vào thư mục outputs/ dưới dạng CSV.
"""


# ==============================================================================
# Fecom Inc. là công ty thương mại điện tử có trụ sở tại Berlin, Đức. Từ năm 2022 đến 2024, công ty đã ghi nhận 99.441 đơn hàng từ 102.727 khách hàng duy nhất và theo dõi giao dịch của 3.095 người bán. Bộ dữ liệu chứa thông tin về:
#
# - Đơn hàng (Orders): Thông tin về trạng thái đơn hàng, thời gian mua, duyệt, giao hàng...
# - Khách hàng (Customer_List): Thông tin về ngày đăng ký, ngày đặt hàng đầu tiên, địa chỉ, độ tuổi, giới tính...
# - Chi tiết đơn hàng (Order_Items): Danh sách sản phẩm, giá, phí vận chuyển, ngày giao hàng dự kiến...
# - Sản phẩm (Products): Thông tin về danh mục, kích thước, trọng lượng sản phẩm...
# - Đánh giá đơn hàng (Order_Reviews): Điểm đánh giá, tiêu đề và nội dung bình luận, thời gian đánh giá...
#
# Dữ liệu này đến từ 338 thành phố tại 28 quốc gia, với 32.951 sản phẩm thuộc 72 danh mục khác nhau. Mục tiêu của bài thực hành là sử dụng Spark DataFrame để thực hiện các phân tích bán hàng và tiếp thị.
#
# ![image.png](attachment:image.png)

# ==============================================================================
# ### Hãy sử dụng Spark DataFrame thực hiện các yêu cầu bên dưới:
#
# 1.	Hãy đọc dữ liệu từ các file csv, sử dụng tự suy ra kiểu dữ liệu cho mỗi cột.
# 2.	Thống kê tổng số đơn hàng, số lượng khách hàng và người bán.
# 3.	Phân tích số lượng đơn hàng theo quốc gia, sắp xếp theo thứ tự giảm dần.
# 4.	Phân tích số lượng đơn hàng nhóm theo năm, tháng đặt hàng (Hiển thị theo năm tăng dần, tháng giảm dần)
# 5.	Thống kê điểm đánh giá trung bình, số lượng đánh giá theo từng mức (ví dụ: 1 đến 5).
# Lưu ý: Cần xử lý các giá trị ngoại lệ và NULL trong cột Review_Score

# ==============================================================================
# ### Chọn 3 trong các câu sau để làm:
#
# 6.  Tính doanh thu (giá sản phẩm + phí vận chuyển) trong năm 2024 và nhóm theo danh mục sản phẩm
# 7.  Xác định sản phẩm có số lượng bán ra cao nhất và tính điểm đánh giá trung bình cho từng sản phẩm
# 8.  Tính toán hiệu số giữa ngày giao hàng thực tế (Order_Delivered_Carrier_Date) và ngày giao hàng dự kiến (ví dụ: Shipping_Limit_Date từ bảng Order_Items) để đánh giá hiệu suất giao hàng.
# 9.  Nhóm khách hàng dựa trên số lượng đơn hàng, giá trị trung bình của đơn hàng và tần suất mua sắm.
# 10. Xếp hạng các seller dựa trên tổng doanh thu và số lượng đơn hàng bán được.

# ==============================================================================
# ## Lời giải bằng Spark DataFrame
#
# > Đặt 5 file CSV (`Customer_List.csv`, `Order_Items.csv`, `Order_Reviews.csv`, `Orders.csv`, `Products.csv`) cùng thư mục với notebook này trước khi chạy.  
# > Các file dùng dấu phân cách `;`, vì vậy cần cấu hình `sep=";"` khi đọc dữ liệu.

# ------------------------------------------------------------------------------
# Code cell 4
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

spark = (
    SparkSession.builder
    .appName("Fecom_Sales_Marketing_Analysis")
    .getOrCreate()
)

# Nếu CSV nằm cùng thư mục với notebook thì để DATA_PATH = ""
# Nếu chạy trên Colab/Drive, hãy đổi lại đường dẫn, ví dụ: DATA_PATH = "/content/drive/MyDrive/data/"
DATA_PATH = ""
OUTPUT_PATH = "outputs"

def safe_write_text(path, content):
    """Ghi file text đơn giản bằng Python để mô tả tên bài trong từng thư mục output."""
    import os
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        f.write(content)

def save_result(df, folder_name, exercise_title):
    """
    Lưu DataFrame ra thư mục outputs/folder_name dạng CSV có header.
    Mỗi file CSV có thêm cột Ten_Bai để biết kết quả thuộc câu nào.
    Đồng thời tạo README.txt trong từng thư mục để ghi rõ tên bài.
    """
    output_dir = f"{OUTPUT_PATH}/{folder_name}"

    df_with_title = df.withColumn("Ten_Bai", F.lit(exercise_title))

    # Đưa cột Ten_Bai lên đầu file CSV cho dễ đọc.
    ordered_columns = ["Ten_Bai"] + [c for c in df_with_title.columns if c != "Ten_Bai"]
    df_with_title = df_with_title.select(*ordered_columns)

    (
        df_with_title.coalesce(1)
        .write
        .mode("overwrite")
        .option("header", True)
        .csv(output_dir)
    )

    safe_write_text(
        f"{output_dir}/README.txt",
        f"Tên bài: {exercise_title}\n"
        f"Thư mục output: {output_dir}\n"
        "Ghi chú: Kết quả chính nằm trong file part-*.csv. File CSV cũng có cột Ten_Bai.\n"
    )
    print(f"Đã lưu kết quả: {output_dir} | {exercise_title}")

def read_semicolon_csv(file_name):
    return (
        spark.read
        .option("header", True)
        .option("inferSchema", True)
        .option("sep", ";")
        .option("multiLine", True)
        .option("quote", '"')
        .option("escape", '"')
        .csv(DATA_PATH + file_name)
    )

customers = read_semicolon_csv("Customer_List.csv")
order_items = read_semicolon_csv("Order_Items.csv")
reviews = read_semicolon_csv("Order_Reviews.csv")
orders = read_semicolon_csv("Orders.csv")
products = read_semicolon_csv("Products.csv")

print("Customers schema:")
customers.printSchema()

print("Orders schema:")
orders.printSchema()

print("Order items schema:")
order_items.printSchema()

print("Reviews schema:")
reviews.printSchema()

print("Products schema:")
products.printSchema()


# ==============================================================================
# ### 1. Đọc dữ liệu từ CSV và tự suy ra kiểu dữ liệu
#
# Cell trên đã đọc toàn bộ dữ liệu với `inferSchema=True`. Cell dưới đây hiển thị thử vài dòng của từng bảng.

# ------------------------------------------------------------------------------
# Code cell 6
customers.show(5, truncate=False)
orders.show(5, truncate=False)
order_items.show(5, truncate=False)
reviews.show(5, truncate=False)
products.show(5, truncate=False)


# ==============================================================================
# ### Chuẩn hóa kiểu thời gian và xử lý dữ liệu đánh giá
#
# Tạo thêm các cột timestamp/date để thuận tiện cho các phân tích tiếp theo.  
# Riêng `Review_Score`, chỉ giữ các giá trị hợp lệ từ 1 đến 5 và loại bỏ `NULL`/ngoại lệ.

# ------------------------------------------------------------------------------
# Code cell 8
orders_clean = (
    orders
    .withColumn("Order_Purchase_TS", F.to_timestamp("Order_Purchase_Timestamp", "yyyy-MM-dd HH:mm"))
    .withColumn("Order_Approved_TS", F.to_timestamp("Order_Approved_At", "yyyy-MM-dd HH:mm"))
    .withColumn("Order_Delivered_Carrier_TS", F.to_timestamp("Order_Delivered_Carrier_Date", "yyyy-MM-dd HH:mm"))
    .withColumn("Order_Delivered_Customer_TS", F.to_timestamp("Order_Delivered_Customer_Date", "yyyy-MM-dd HH:mm"))
    .withColumn("Order_Estimated_Delivery_TS", F.to_timestamp("Order_Estimated_Delivery_Date", "yyyy-MM-dd HH:mm"))
)

order_items_clean = (
    order_items
    .withColumn("Shipping_Limit_TS", F.to_timestamp("Shipping_Limit_Date", "yyyy-MM-dd HH:mm"))
    .withColumn("Item_Revenue", F.col("Price") + F.col("Freight_Value"))
)

reviews_clean = (
    reviews
    .withColumn("Review_Score_Int", F.col("Review_Score").cast("int"))
    .filter(F.col("Review_Score_Int").isNotNull())
    .filter(F.col("Review_Score_Int").between(1, 5))
)

print("Số review ban đầu:", reviews.count())
print("Số review hợp lệ sau xử lý:", reviews_clean.count())


# ==============================================================================
# ### 2. Thống kê tổng số đơn hàng, số lượng khách hàng và người bán

# ------------------------------------------------------------------------------
# Code cell 10
total_orders = orders_clean.select("Order_ID").distinct().count()
total_customers = customers.select("Customer_Trx_ID").distinct().count()
total_sellers = order_items_clean.select("Seller_ID").distinct().count()

summary_counts = spark.createDataFrame(
    [
        ("Tổng số đơn hàng", total_orders),
        ("Số lượng khách hàng", total_customers),
        ("Số lượng người bán", total_sellers),
    ],
    ["Metric", "Value"]
)

summary_counts.show(truncate=False)


# ==============================================================================
# ### 3. Phân tích số lượng đơn hàng theo quốc gia, sắp xếp giảm dần

# ------------------------------------------------------------------------------
# Code cell 12
orders_by_country = (
    orders_clean
    .join(
        customers.select("Customer_Trx_ID", "Customer_Country", "Customer_Country_Code"),
        on="Customer_Trx_ID",
        how="left"
    )
    .groupBy("Customer_Country", "Customer_Country_Code")
    .agg(F.countDistinct("Order_ID").alias("Total_Orders"))
    .orderBy(F.desc("Total_Orders"))
)

orders_by_country.show(50, truncate=False)


# ==============================================================================
# ### 4. Phân tích số lượng đơn hàng theo năm và tháng đặt hàng
#
# Yêu cầu: năm tăng dần, tháng giảm dần.

# ------------------------------------------------------------------------------
# Code cell 14
orders_by_year_month = (
    orders_clean
    .filter(F.col("Order_Purchase_TS").isNotNull())
    .withColumn("Purchase_Year", F.year("Order_Purchase_TS"))
    .withColumn("Purchase_Month", F.month("Order_Purchase_TS"))
    .groupBy("Purchase_Year", "Purchase_Month")
    .agg(F.countDistinct("Order_ID").alias("Total_Orders"))
    .orderBy(F.asc("Purchase_Year"), F.desc("Purchase_Month"))
)

orders_by_year_month.show(100, truncate=False)


# ==============================================================================
# ### 5. Thống kê điểm đánh giá trung bình và số lượng đánh giá theo từng mức
#
# Đã xử lý `NULL` và ngoại lệ bằng cách chỉ giữ `Review_Score` trong khoảng 1–5.

# ------------------------------------------------------------------------------
# Code cell 16
avg_review_score = reviews_clean.agg(
    F.round(F.avg("Review_Score_Int"), 3).alias("Average_Review_Score")
)

review_distribution = (
    reviews_clean
    .groupBy("Review_Score_Int")
    .agg(F.count("*").alias("Number_Of_Reviews"))
    .orderBy("Review_Score_Int")
)

avg_review_score.show(truncate=False)
review_distribution.show(truncate=False)


# ==============================================================================
# ## Phần tự chọn
#
# Đề yêu cầu chọn 3 câu từ câu 6 đến 10. Ở đây làm cả 5 câu để bạn có thể chọn phần phù hợp khi nộp.

# ==============================================================================
# ### 6. Tính doanh thu năm 2024 và nhóm theo danh mục sản phẩm
#
# Doanh thu được tính theo công thức:
#
# `Revenue = Price + Freight_Value`

# ------------------------------------------------------------------------------
# Code cell 19
revenue_2024_by_category = (
    order_items_clean
    .join(orders_clean.select("Order_ID", "Order_Purchase_TS"), on="Order_ID", how="inner")
    .join(products.select("Product_ID", "Product_Category_Name"), on="Product_ID", how="left")
    .filter(F.year("Order_Purchase_TS") == 2024)
    .groupBy("Product_Category_Name")
    .agg(
        F.round(F.sum("Item_Revenue"), 2).alias("Total_Revenue_2024"),
        F.count("*").alias("Total_Items_Sold")
    )
    .orderBy(F.desc("Total_Revenue_2024"))
)

revenue_2024_by_category.show(100, truncate=False)


# ==============================================================================
# ### 7. Xác định sản phẩm có số lượng bán ra cao nhất và tính điểm đánh giá trung bình cho từng sản phẩm
#
# Vì đánh giá nằm ở cấp đơn hàng, điểm đánh giá của sản phẩm được lấy bằng cách join `Order_Items` với `Order_Reviews` thông qua `Order_ID`.

# ------------------------------------------------------------------------------
# Code cell 21
product_sales_and_review = (
    order_items_clean
    .join(products.select("Product_ID", "Product_Category_Name"), on="Product_ID", how="left")
    .join(reviews_clean.select("Order_ID", "Review_Score_Int"), on="Order_ID", how="left")
    .groupBy("Product_ID", "Product_Category_Name")
    .agg(
        F.count("*").alias("Units_Sold"),
        F.round(F.avg("Review_Score_Int"), 3).alias("Average_Review_Score")
    )
    .orderBy(F.desc("Units_Sold"), F.desc("Average_Review_Score"))
)

product_sales_and_review.show(20, truncate=False)

top_selling_product = product_sales_and_review.limit(1)
top_selling_product.show(truncate=False)


# ==============================================================================
# ### 8. Tính hiệu số giữa ngày giao hàng thực tế và ngày giao hàng dự kiến
#
# Theo đề bài:  
# - Ngày giao hàng thực tế: `Order_Delivered_Carrier_Date`
# - Ngày giao hàng dự kiến/giới hạn: `Shipping_Limit_Date`
#
# Công thức:
#
# `Delivery_Delay_Days = Order_Delivered_Carrier_Date - Shipping_Limit_Date`
#
# Nếu kết quả > 0: giao trễ so với giới hạn.  
# Nếu kết quả <= 0: giao đúng hạn hoặc sớm.

# ------------------------------------------------------------------------------
# Code cell 23
delivery_performance = (
    orders_clean
    .select("Order_ID", "Order_Status", "Order_Delivered_Carrier_TS")
    .join(
        order_items_clean.select("Order_ID", "Order_Item_ID", "Shipping_Limit_TS"),
        on="Order_ID",
        how="inner"
    )
    .filter(F.col("Order_Delivered_Carrier_TS").isNotNull())
    .filter(F.col("Shipping_Limit_TS").isNotNull())
    .withColumn(
        "Delivery_Delay_Days",
        F.round(
            (F.unix_timestamp("Order_Delivered_Carrier_TS") - F.unix_timestamp("Shipping_Limit_TS")) / 86400,
            2
        )
    )
    .withColumn(
        "Delivery_Performance",
        F.when(F.col("Delivery_Delay_Days") > 0, "Late")
         .otherwise("On time or early")
    )
)

delivery_performance.show(20, truncate=False)

delivery_performance_summary = (
    delivery_performance
    .groupBy("Delivery_Performance")
    .agg(
        F.count("*").alias("Number_Of_Order_Items"),
        F.round(F.avg("Delivery_Delay_Days"), 2).alias("Average_Delay_Days"),
        F.round(F.min("Delivery_Delay_Days"), 2).alias("Min_Delay_Days"),
        F.round(F.max("Delivery_Delay_Days"), 2).alias("Max_Delay_Days")
    )
    .orderBy("Delivery_Performance")
)

delivery_performance_summary.show(truncate=False)


# ==============================================================================
# ### 9. Nhóm khách hàng dựa trên số lượng đơn hàng, giá trị trung bình đơn hàng và tần suất mua sắm
#
# Các chỉ số:
# - `Total_Orders`: tổng số đơn hàng của khách hàng
# - `Average_Order_Value`: giá trị trung bình mỗi đơn hàng
# - `Purchase_Frequency_Days`: số ngày trung bình giữa 2 lần mua
#
# Cách nhóm khách hàng trong lời giải này:
# - `High-value loyal`: mua nhiều và giá trị trung bình cao
# - `Loyal`: mua nhiều
# - `Repeat`: mua lặp lại ở mức vừa
# - `One-time`: mới mua 1 lần

# ------------------------------------------------------------------------------
# Code cell 25
order_value = (
    order_items_clean
    .groupBy("Order_ID")
    .agg(F.round(F.sum("Item_Revenue"), 2).alias("Order_Value"))
)

customer_orders = (
    orders_clean
    .select("Order_ID", "Customer_Trx_ID", "Order_Purchase_TS")
    .join(order_value, on="Order_ID", how="left")
    .filter(F.col("Order_Purchase_TS").isNotNull())
)

customer_metrics = (
    customer_orders
    .groupBy("Customer_Trx_ID")
    .agg(
        F.countDistinct("Order_ID").alias("Total_Orders"),
        F.round(F.avg("Order_Value"), 2).alias("Average_Order_Value"),
        F.min("Order_Purchase_TS").alias("First_Purchase_TS"),
        F.max("Order_Purchase_TS").alias("Last_Purchase_TS")
    )
    .withColumn(
        "Purchase_Frequency_Days",
        F.when(
            F.col("Total_Orders") > 1,
            F.round(
                F.datediff("Last_Purchase_TS", "First_Purchase_TS") / (F.col("Total_Orders") - 1),
                2
            )
        ).otherwise(None)
    )
)

# Ngưỡng giá trị cao: dùng percentile 75% của Average_Order_Value
high_value_threshold = customer_metrics.approxQuantile("Average_Order_Value", [0.75], 0.01)[0]

customer_segments = (
    customer_metrics
    .withColumn(
        "Customer_Segment",
        F.when(
            (F.col("Total_Orders") >= 4) & (F.col("Average_Order_Value") >= F.lit(high_value_threshold)),
            "High-value loyal"
        )
        .when(F.col("Total_Orders") >= 4, "Loyal")
        .when(F.col("Total_Orders").between(2, 3), "Repeat")
        .otherwise("One-time")
    )
)

customer_segments.show(20, truncate=False)

segment_summary = (
    customer_segments
    .groupBy("Customer_Segment")
    .agg(
        F.count("*").alias("Number_Of_Customers"),
        F.round(F.avg("Total_Orders"), 2).alias("Avg_Total_Orders"),
        F.round(F.avg("Average_Order_Value"), 2).alias("Avg_Order_Value"),
        F.round(F.avg("Purchase_Frequency_Days"), 2).alias("Avg_Frequency_Days")
    )
    .orderBy(F.desc("Number_Of_Customers"))
)

segment_summary.show(truncate=False)


# ==============================================================================
# ### 10. Xếp hạng seller dựa trên tổng doanh thu và số lượng đơn hàng bán được
#
# Xếp hạng theo:
# 1. Tổng doanh thu giảm dần
# 2. Số lượng đơn hàng giảm dần

# ------------------------------------------------------------------------------
# Code cell 27
seller_performance = (
    order_items_clean
    .groupBy("Seller_ID")
    .agg(
        F.round(F.sum("Item_Revenue"), 2).alias("Total_Revenue"),
        F.countDistinct("Order_ID").alias("Total_Orders"),
        F.count("*").alias("Total_Items_Sold"),
        F.round(F.avg("Item_Revenue"), 2).alias("Avg_Item_Revenue")
    )
)

seller_rank_window = Window.orderBy(F.desc("Total_Revenue"), F.desc("Total_Orders"))

seller_ranking = (
    seller_performance
    .withColumn("Seller_Rank", F.dense_rank().over(seller_rank_window))
    .orderBy("Seller_Rank")
)

seller_ranking.show(20, truncate=False)


# ==============================================================================
# ## Kết luận ngắn
#
# Notebook đã thực hiện:
# - Đọc dữ liệu CSV bằng Spark DataFrame với tự suy kiểu dữ liệu.
# - Thống kê tổng quan đơn hàng, khách hàng, seller.
# - Phân tích đơn hàng theo quốc gia và theo thời gian.
# - Làm sạch và phân tích điểm đánh giá.
# - Hoàn thành toàn bộ phần tự chọn từ câu 6 đến câu 10 để có đủ lựa chọn khi nộp bài.

# ==============================================================================
# ## Lưu kết quả ra file CSV
#
# Spark sẽ lưu mỗi kết quả thành một thư mục trong `outputs/`.
# Tên thư mục output có dạng `bai_XX_ten_bai`.
# Bên trong mỗi thư mục có:
# - `part-....csv`: file kết quả, có cột `Ten_Bai` ghi rõ tên bài.
# - `README.txt`: mô tả tên bài và đường dẫn output.
# Ví dụ: `outputs/bai_03_don_hang_theo_quoc_gia/part-....csv`

print("\nĐang lưu kết quả phân tích ra thư mục outputs/ ...")

save_result(
    summary_counts,
    "bai_02_thong_ke_tong_quan",
    "Bài 2 - Thống kê tổng số đơn hàng, số lượng khách hàng và người bán"
)
save_result(
    orders_by_country,
    "bai_03_don_hang_theo_quoc_gia",
    "Bài 3 - Phân tích số lượng đơn hàng theo quốc gia, sắp xếp giảm dần"
)
save_result(
    orders_by_year_month,
    "bai_04_don_hang_theo_nam_thang",
    "Bài 4 - Phân tích số lượng đơn hàng theo năm và tháng đặt hàng"
)
save_result(
    avg_review_score,
    "bai_05_diem_danh_gia_trung_binh",
    "Bài 5 - Thống kê điểm đánh giá trung bình sau khi xử lý NULL và ngoại lệ"
)
save_result(
    review_distribution,
    "bai_05_phan_bo_diem_danh_gia",
    "Bài 5 - Thống kê số lượng đánh giá theo từng mức điểm từ 1 đến 5"
)
save_result(
    revenue_2024_by_category,
    "bai_06_doanh_thu_2024_theo_danh_muc",
    "Bài 6 - Tính doanh thu năm 2024 theo danh mục sản phẩm"
)
save_result(
    product_sales_and_review,
    "bai_07_san_pham_ban_ra_va_diem_danh_gia",
    "Bài 7 - Số lượng bán ra và điểm đánh giá trung bình theo từng sản phẩm"
)
save_result(
    top_selling_product,
    "bai_07_san_pham_ban_chay_nhat",
    "Bài 7 - Sản phẩm có số lượng bán ra cao nhất"
)
save_result(
    delivery_performance,
    "bai_08_hieu_suat_giao_hang_chi_tiet",
    "Bài 8 - Hiệu số giữa ngày giao hàng thực tế và ngày giao hàng dự kiến theo từng item"
)
save_result(
    delivery_performance_summary,
    "bai_08_tong_hop_hieu_suat_giao_hang",
    "Bài 8 - Tổng hợp hiệu suất giao hàng đúng hạn hoặc trễ"
)
save_result(
    customer_segments,
    "bai_09_phan_nhom_khach_hang_chi_tiet",
    "Bài 9 - Nhóm khách hàng theo số đơn hàng, giá trị trung bình và tần suất mua"
)
save_result(
    segment_summary,
    "bai_09_tong_hop_phan_nhom_khach_hang",
    "Bài 9 - Tổng hợp các nhóm khách hàng"
)
save_result(
    seller_ranking,
    "bai_10_xep_hang_seller",
    "Bài 10 - Xếp hạng seller theo tổng doanh thu và số lượng đơn hàng"
)

print("Hoàn tất. Tất cả kết quả đã nằm trong thư mục outputs/.")

spark.stop()
