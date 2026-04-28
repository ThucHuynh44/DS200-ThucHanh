from pyspark import SparkConf, SparkContext


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
# Tao SparkContext cho YARN
# =========================
# Khong dung:
# sc = SparkContext("local[*]", "Bai1_RDD_HDFS")
#
# Khi chay bang:
# spark-submit --master yarn --deploy-mode client bai1_yarn.py
#
# Spark se tu lay master la YARN.
conf = SparkConf().setAppName("Bai1_RDD_HDFS_YARN")
sc = SparkContext(conf=conf)

# Giam log
sc.setLogLevel("ERROR")


# =========================
# Duong dan HDFS
# =========================
# Neu HDFS cua ban khong phai localhost:9000,
# hay kiem tra bang lenh:
# hdfs getconf -confKey fs.defaultFS
#
# Sau do thay hdfs://localhost:9000 bang ket qua dung.
input_path = "hdfs://localhost:9000/input/lab3"
output_path = "hdfs://localhost:9000/output/lab3/bai1"

movies_path = input_path + "/movies.txt"
ratings_1_path = input_path + "/ratings_1.txt"
ratings_2_path = input_path + "/ratings_2.txt"


# =========================
# Buoc 1:
# Doc movies.txt
# Tao RDD: MovieID -> Title
# =========================
movies_rdd = sc.textFile(movies_path)

movie_titles = movies_rdd.map(split_line) \
    .map(lambda x: (int(x[0]), x[1]))


# =========================
# Buoc 2:
# Doc ratings_1.txt va ratings_2.txt
# Tao RDD: MovieID -> (Rating, 1)
# =========================
ratings_1 = sc.textFile(ratings_1_path)
ratings_2 = sc.textFile(ratings_2_path)

ratings = ratings_1.union(ratings_2)

movie_ratings = ratings.map(split_line) \
    .map(lambda x: (int(x[1]), (float(x[2]), 1)))


# =========================
# Buoc 3:
# Reduce tinh tong diem va so luot danh gia
# MovieID -> (total_rating, total_count)
# =========================
rating_stats = movie_ratings.reduceByKey(
    lambda a, b: (a[0] + b[0], a[1] + b[1])
)


# =========================
# Buoc 4:
# Tinh diem trung binh
# MovieID -> (average_rating, total_count)
# =========================
avg_ratings = rating_stats.mapValues(
    lambda x: (x[0] / x[1], x[1])
)


# =========================
# Buoc 5:
# Join voi ten phim
# MovieID -> (Title, AverageRating, TotalRatings)
# =========================
movies_with_rating = movie_titles.join(avg_ratings) \
    .map(lambda x: (x[0], x[1][0], x[1][1][0], x[1][1][1]))


# =========================
# In ket qua moi phim ra terminal
# =========================
print("===== DIEM TRUNG BINH VA TONG SO LUOT DANH GIA CUA MOI PHIM =====")
print("MovieID | Title | Average Rating | Total Ratings")

all_movies = movies_with_rating.sortBy(lambda x: x[0]).collect()

for movie_id, title, avg_rating, count in all_movies:
    print(f"{movie_id} | {title} | {avg_rating:.2f} | {count}")


# =========================
# Tim phim co diem trung binh cao nhat
# Chi xet phim co it nhat 50 luot danh gia
# =========================
filtered_movies = movies_with_rating.filter(lambda x: x[3] >= 50)

print("\n===== PHIM CO DIEM TRUNG BINH CAO NHAT, IT NHAT 50 LUOT DANH GIA =====")

top_movie = None

if filtered_movies.isEmpty():
    print("Khong co phim nao co it nhat 50 luot danh gia.")
else:
    top_movie = filtered_movies.sortBy(lambda x: x[2], ascending=False).first()

    print(f"MovieID: {top_movie[0]}")
    print(f"Title: {top_movie[1]}")
    print(f"Average Rating: {top_movie[2]:.2f}")
    print(f"Total Ratings: {top_movie[3]}")


# =========================
# Luu ket qua ra HDFS
# =========================

# Tat ca phim
output_all = movies_with_rating.sortBy(lambda x: x[0]) \
    .map(lambda x: f"{x[0]} | {x[1]} | Average Rating: {x[2]:.2f} | Total Ratings: {x[3]}")

# Phim top
if top_movie is None:
    output_top = sc.parallelize([
        "Khong co phim nao co it nhat 50 luot danh gia."
    ])
else:
    output_top = sc.parallelize([
        "PHIM CO DIEM TRUNG BINH CAO NHAT, IT NHAT 50 LUOT DANH GIA",
        f"MovieID: {top_movie[0]}",
        f"Title: {top_movie[1]}",
        f"Average Rating: {top_movie[2]:.2f}",
        f"Total Ratings: {top_movie[3]}"
    ])

output_all.saveAsTextFile(output_path + "/all_movies")
output_top.saveAsTextFile(output_path + "/top_movie")


# =========================
# Dung SparkContext
# =========================
sc.stop()