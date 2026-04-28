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
conf = SparkConf().setAppName("Bai3_RDD_Gender_Rating_YARN")
sc = SparkContext(conf=conf)

sc.setLogLevel("ERROR")


# =========================
# Duong dan HDFS
# Neu localhost:9000 khong dung, kiem tra bang:
# hdfs getconf -confKey fs.defaultFS
# =========================
input_path = "hdfs://localhost:9000/input/lab3"
output_path = "hdfs://localhost:9000/output/lab3/bai3"

movies_path = input_path + "/movies.txt"
ratings_1_path = input_path + "/ratings_1.txt"
ratings_2_path = input_path + "/ratings_2.txt"
users_path = input_path + "/users.txt"


# =========================
# Buoc 1:
# Doc users.txt
# Tao RDD: UserID -> Gender
# =========================
users_rdd = sc.textFile(users_path)

user_gender = users_rdd.map(split_line) \
    .map(lambda x: (int(x[0]), x[1]))

# Vi du:
# UserID::Gender::Age::Occupation::Zip-code
# 1::F::1::10::48067
# Sau khi map:
# (1, "F")


# =========================
# Doc movies.txt
# Tao RDD: MovieID -> Title
# De hien thi ten phim trong ket qua
# =========================
movies_rdd = sc.textFile(movies_path)

movie_titles = movies_rdd.map(split_line) \
    .map(lambda x: (int(x[0]), x[1]))

# Vi du:
# (1, "Toy Story (1995)")


# =========================
# Buoc 2:
# Doc ratings_1.txt va ratings_2.txt
# Tao RDD: UserID -> (MovieID, Rating)
# =========================
ratings_1 = sc.textFile(ratings_1_path)
ratings_2 = sc.textFile(ratings_2_path)

ratings = ratings_1.union(ratings_2)

user_ratings = ratings.map(split_line) \
    .map(lambda x: (int(x[0]), (int(x[1]), float(x[2]))))

# Vi du:
# UserID::MovieID::Rating::Timestamp
# 1::1193::5::978300760
# Sau khi map:
# (1, (1193, 5.0))


# =========================
# Join voi users de them thong tin Gender
# UserID -> ((MovieID, Rating), Gender)
# =========================
ratings_with_gender = user_ratings.join(user_gender)

# Vi du:
# (1, ((1193, 5.0), "F"))


# =========================
# Buoc 3:
# Tao key la (MovieID, Gender)
# Value la (Rating, 1)
# =========================
movie_gender_ratings = ratings_with_gender.map(
    lambda x: ((x[1][0][0], x[1][1]), (x[1][0][1], 1))
)

# Giai thich:
# x = (UserID, ((MovieID, Rating), Gender))
# x[1][0][0] = MovieID
# x[1][0][1] = Rating
# x[1][1]    = Gender
#
# Ket qua:
# ((MovieID, Gender), (Rating, 1))


# =========================
# Reduce theo (MovieID, Gender)
# (MovieID, Gender) -> (total_rating, total_count)
# =========================
movie_gender_stats = movie_gender_ratings.reduceByKey(
    lambda a, b: (a[0] + b[0], a[1] + b[1])
)


# =========================
# Tinh diem trung binh
# (MovieID, Gender) -> (average_rating, total_count)
# =========================
movie_gender_avg = movie_gender_stats.mapValues(
    lambda x: (x[0] / x[1], x[1])
)

# Vi du:
# ((1, "F"), (4.2, 10))


# =========================
# Join voi ten phim
# Doi key tu (MovieID, Gender) ve MovieID de join
# MovieID -> (Gender, AverageRating, TotalRatings)
# =========================
movie_gender_for_join = movie_gender_avg.map(
    lambda x: (x[0][0], (x[0][1], x[1][0], x[1][1]))
)

# Sau khi map:
# MovieID -> (Gender, AvgRating, Count)


result_with_title = movie_titles.join(movie_gender_for_join) \
    .map(lambda x: (x[0], x[1][0], x[1][1][0], x[1][1][1], x[1][1][2]))

# Sau khi map:
# (MovieID, Title, Gender, AverageRating, TotalRatings)


# =========================
# In ket qua ra terminal
# =========================
print("===== DIEM TRUNG BINH CUA MOI PHIM THEO GIOI TINH =====")
print("MovieID | Title | Gender | Average Rating | Total Ratings")

result = result_with_title.sortBy(lambda x: (x[0], x[2])).collect()

for movie_id, title, gender, avg_rating, count in result:
    print(f"{movie_id} | {title} | {gender} | {avg_rating:.2f} | {count}")


# =========================
# Luu ket qua ra HDFS
# =========================
output = result_with_title.sortBy(lambda x: (x[0], x[2])) \
    .map(lambda x: f"{x[0]} | {x[1]} | Gender: {x[2]} | Average Rating: {x[3]:.2f} | Total Ratings: {x[4]}")

output.saveAsTextFile(output_path)


# =========================
# Dung SparkContext
# =========================
sc.stop()