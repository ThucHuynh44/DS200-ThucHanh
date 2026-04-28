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
# Ham phan nhom tuoi
# =========================
def get_age_group(age):
    """
    Phan loai tuoi thanh nhom tuoi.
    Theo MovieLens, Age thuong co cac ma:
    1, 18, 25, 35, 45, 50, 56

    Co the hieu thanh:
    1  -> Under 18
    18 -> 18-24
    25 -> 25-34
    35 -> 35-44
    45 -> 45-49
    50 -> 50-55
    56 -> 56+
    """
    age = int(age)

    if age == 1:
        return "Under 18"
    elif age == 18:
        return "18-24"
    elif age == 25:
        return "25-34"
    elif age == 35:
        return "35-44"
    elif age == 45:
        return "45-49"
    elif age == 50:
        return "50-55"
    elif age == 56:
        return "56+"
    else:
        return "Unknown"


# =========================
# Tao SparkContext cho YARN
# =========================
conf = SparkConf().setAppName("Bai4_RDD_Age_Group_Rating_YARN")
sc = SparkContext(conf=conf)

sc.setLogLevel("ERROR")


# =========================
# Duong dan HDFS
# Neu localhost:9000 khong dung, kiem tra bang:
# hdfs getconf -confKey fs.defaultFS
# =========================
input_path = "hdfs://localhost:9000/input/lab3"
output_path = "hdfs://localhost:9000/output/lab3/bai4"

movies_path = input_path + "/movies.txt"
ratings_1_path = input_path + "/ratings_1.txt"
ratings_2_path = input_path + "/ratings_2.txt"
users_path = input_path + "/users.txt"


# =========================
# Buoc 1:
# Doc users.txt
# Tao RDD: UserID -> AgeGroup
# =========================
users_rdd = sc.textFile(users_path)

user_age_group = users_rdd.map(split_line) \
    .map(lambda x: (int(x[0]), get_age_group(x[2])))

# Vi du:
# UserID::Gender::Age::Occupation::Zip-code
# 1::F::1::10::48067
# Sau khi map:
# (1, "Under 18")


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
# Join voi users de them thong tin AgeGroup
# UserID -> ((MovieID, Rating), AgeGroup)
# =========================
ratings_with_age_group = user_ratings.join(user_age_group)

# Vi du:
# (1, ((1193, 5.0), "Under 18"))


# =========================
# Buoc 3:
# Tao key la (MovieID, AgeGroup)
# Value la (Rating, 1)
# =========================
movie_age_ratings = ratings_with_age_group.map(
    lambda x: ((x[1][0][0], x[1][1]), (x[1][0][1], 1))
)

# Giai thich:
# x = (UserID, ((MovieID, Rating), AgeGroup))
# x[1][0][0] = MovieID
# x[1][0][1] = Rating
# x[1][1]    = AgeGroup
#
# Ket qua:
# ((MovieID, AgeGroup), (Rating, 1))


# =========================
# Reduce theo (MovieID, AgeGroup)
# (MovieID, AgeGroup) -> (total_rating, total_count)
# =========================
movie_age_stats = movie_age_ratings.reduceByKey(
    lambda a, b: (a[0] + b[0], a[1] + b[1])
)


# =========================
# Tinh diem trung binh
# (MovieID, AgeGroup) -> (average_rating, total_count)
# =========================
movie_age_avg = movie_age_stats.mapValues(
    lambda x: (x[0] / x[1], x[1])
)

# Vi du:
# ((1, "18-24"), (4.1, 20))


# =========================
# Join voi ten phim
# Doi key tu (MovieID, AgeGroup) ve MovieID de join
# MovieID -> (AgeGroup, AverageRating, TotalRatings)
# =========================
movie_age_for_join = movie_age_avg.map(
    lambda x: (x[0][0], (x[0][1], x[1][0], x[1][1]))
)

# Sau khi map:
# MovieID -> (AgeGroup, AvgRating, Count)


result_with_title = movie_titles.join(movie_age_for_join) \
    .map(lambda x: (x[0], x[1][0], x[1][1][0], x[1][1][1], x[1][1][2]))

# Sau khi map:
# (MovieID, Title, AgeGroup, AverageRating, TotalRatings)


# =========================
# In ket qua ra terminal
# =========================
print("===== DIEM TRUNG BINH CUA MOI PHIM THEO NHOM TUOI =====")
print("MovieID | Title | Age Group | Average Rating | Total Ratings")

result = result_with_title.sortBy(lambda x: (x[0], x[2])).collect()

for movie_id, title, age_group, avg_rating, count in result:
    print(f"{movie_id} | {title} | {age_group} | {avg_rating:.2f} | {count}")


# =========================
# Luu ket qua ra HDFS
# =========================
output = result_with_title.sortBy(lambda x: (x[0], x[2])) \
    .map(lambda x: f"{x[0]} | {x[1]} | Age Group: {x[2]} | Average Rating: {x[3]:.2f} | Total Ratings: {x[4]}")

output.saveAsTextFile(output_path)


# =========================
# Dung SparkContext
# =========================
sc.stop()