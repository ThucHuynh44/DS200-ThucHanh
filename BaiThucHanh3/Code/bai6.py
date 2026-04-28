from pyspark import SparkConf, SparkContext
from datetime import datetime


# =========================
# Ham tach dong du lieu
# =========================
def split_line(line):
    """
    Tach dong du lieu.
    Ho tro 2 kieu phan cach:
    - Dinh dang MovieLens: ::
    - Dinh dang CSV: ,
    """
    line = line.strip()

    if "::" in line:
        return line.split("::")
    return line.split(",")


# =========================
# Ham chuyen Unix Timestamp sang Year
# =========================
def timestamp_to_year(timestamp):
    """
    Chuyen timestamp Unix sang nam.
    Timestamp trong ratings la dang giay, vi du: 978300760
    """
    return datetime.fromtimestamp(int(timestamp)).year


# =========================
# Tao SparkContext cho YARN
# =========================
conf = SparkConf().setAppName("Bai6_RDD_Time_Rating_YARN")
sc = SparkContext(conf=conf)

sc.setLogLevel("ERROR")


# =========================
# Duong dan HDFS
# Neu localhost:9000 khong dung, kiem tra bang:
# hdfs getconf -confKey fs.defaultFS
# =========================
input_path = "hdfs://localhost:9000/input/lab3"
output_path = "hdfs://localhost:9000/output/lab3/bai6"

ratings_1_path = input_path + "/ratings_1.txt"
ratings_2_path = input_path + "/ratings_2.txt"


# =========================
# Buoc 1:
# Doc ratings_1.txt va ratings_2.txt
# =========================
ratings_1 = sc.textFile(ratings_1_path)
ratings_2 = sc.textFile(ratings_2_path)

ratings = ratings_1.union(ratings_2)


# =========================
# Buoc 2:
# Chuyen moi dong rating thanh:
# Year -> (Rating, 1)
# =========================
year_ratings = ratings.map(split_line) \
    .map(lambda x: (timestamp_to_year(x[3]), (float(x[2]), 1)))

# Vi du:
# UserID::MovieID::Rating::Timestamp
# 1::1193::5::978300760
#
# Timestamp 978300760 -> Year 2000
#
# Sau khi map:
# (2000, (5.0, 1))


# =========================
# Buoc 3:
# Reduce theo nam de tinh tong diem va tong so luot danh gia
# Year -> (total_rating, total_count)
# =========================
year_stats = year_ratings.reduceByKey(
    lambda a, b: (a[0] + b[0], a[1] + b[1])
)


# =========================
# Buoc 4:
# Tinh diem trung binh theo nam
# Year -> (average_rating, total_count)
# =========================
year_avg = year_stats.mapValues(
    lambda x: (x[0] / x[1], x[1])
)


# =========================
# In ket qua ra terminal
# =========================
print("===== TONG SO LUOT DANH GIA VA DIEM TRUNG BINH THEO NAM =====")
print("Year | Average Rating | Total Ratings")

result = year_avg.sortBy(lambda x: x[0]).collect()

for year, value in result:
    avg_rating = value[0]
    count = value[1]
    print(f"{year} | {avg_rating:.2f} | {count}")


# =========================
# Luu ket qua ra HDFS
# =========================
output = year_avg.sortBy(lambda x: x[0]) \
    .map(lambda x: f"{x[0]} | Average Rating: {x[1][0]:.2f} | Total Ratings: {x[1][1]}")

output.saveAsTextFile(output_path)


# =========================
# Dung SparkContext
# =========================
sc.stop()