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
conf = SparkConf().setAppName("Bai2_RDD_Genre_Rating_YARN")
sc = SparkContext(conf=conf)

sc.setLogLevel("ERROR")


# =========================
# Duong dan HDFS
# Neu localhost:9000 khong dung, kiem tra bang:
# hdfs getconf -confKey fs.defaultFS
# =========================
input_path = "hdfs://localhost:9000/input/lab3"
output_path = "hdfs://localhost:9000/output/lab3/bai2"

movies_path = input_path + "/movies.txt"
ratings_1_path = input_path + "/ratings_1.txt"
ratings_2_path = input_path + "/ratings_2.txt"


# =========================
# Buoc 1:
# Doc movies.txt
# Tao RDD: MovieID -> List of Genres
# =========================
movies_rdd = sc.textFile(movies_path)

movie_genres = movies_rdd.map(split_line) \
    .map(lambda x: (int(x[0]), x[2].split("|")))

# Vi du:
# 1::Toy Story (1995)::Animation|Children's|Comedy
# Sau khi map:
# (1, ["Animation", "Children's", "Comedy"])


# =========================
# Buoc 2:
# Doc ratings_1.txt va ratings_2.txt
# Tao RDD: MovieID -> Rating
# =========================
ratings_1 = sc.textFile(ratings_1_path)
ratings_2 = sc.textFile(ratings_2_path)

ratings = ratings_1.union(ratings_2)

movie_ratings = ratings.map(split_line) \
    .map(lambda x: (int(x[1]), float(x[2])))

# Vi du:
# UserID::MovieID::Rating::Timestamp
# 1::1193::5::978300760
# Sau khi map:
# (1193, 5.0)


# =========================
# Buoc 3:
# Join MovieID -> Genres voi MovieID -> Rating
# Ket qua:
# MovieID -> (ListGenres, Rating)
# =========================
movie_genre_rating = movie_genres.join(movie_ratings)

# Vi du:
# (1, (["Animation", "Children's", "Comedy"], 5.0))


# =========================
# Buoc 4:
# Tu moi phim co nhieu genre, phat sinh nhieu cap:
# Genre -> (Rating, 1)
# =========================
genre_ratings = movie_genre_rating.flatMap(
    lambda x: [(genre, (x[1][1], 1)) for genre in x[1][0]]
)

# Vi du:
# (1, (["Animation", "Children's", "Comedy"], 5.0))
# Thanh:
# ("Animation", (5.0, 1))
# ("Children's", (5.0, 1))
# ("Comedy", (5.0, 1))


# =========================
# Buoc 5:
# Reduce theo Genre de tinh tong diem va so luot danh gia
# Genre -> (total_rating, total_count)
# =========================
genre_stats = genre_ratings.reduceByKey(
    lambda a, b: (a[0] + b[0], a[1] + b[1])
)


# =========================
# Buoc 6:
# Tinh diem trung binh theo Genre
# Genre -> (average_rating, total_count)
# =========================
genre_avg = genre_stats.mapValues(
    lambda x: (x[0] / x[1], x[1])
)


# =========================
# In ket qua ra terminal
# =========================
print("===== DIEM TRUNG BINH THEO THE LOAI PHIM =====")
print("Genre | Average Rating | Total Ratings")

result = genre_avg.sortBy(lambda x: x[0]).collect()

for genre, value in result:
    avg_rating = value[0]
    count = value[1]
    print(f"{genre} | {avg_rating:.2f} | {count}")


# =========================
# Luu ket qua ra HDFS
# =========================
output = genre_avg.sortBy(lambda x: x[0]) \
    .map(lambda x: f"{x[0]} | Average Rating: {x[1][0]:.2f} | Total Ratings: {x[1][1]}")

output.saveAsTextFile(output_path)


# =========================
# Dung SparkContext
# =========================
sc.stop()