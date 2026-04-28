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
conf = SparkConf().setAppName("Bai5_RDD_Occupation_Rating_YARN")
sc = SparkContext(conf=conf)

sc.setLogLevel("ERROR")


# =========================
# Duong dan HDFS
# Neu localhost:9000 khong dung, kiem tra bang:
# hdfs getconf -confKey fs.defaultFS
# =========================
input_path = "hdfs://localhost:9000/input/lab3"
output_path = "hdfs://localhost:9000/output/lab3/bai5"

ratings_1_path = input_path + "/ratings_1.txt"
ratings_2_path = input_path + "/ratings_2.txt"
users_path = input_path + "/users.txt"
occupation_path = input_path + "/occupation.txt"


# =========================
# Buoc 1:
# Doc occupation.txt
# Tao RDD: OccupationID -> OccupationName
# =========================
occupation_rdd = sc.textFile(occupation_path)

occupation_map = occupation_rdd.map(split_line) \
    .map(lambda x: (int(x[0]), x[1]))

# Vi du:
# 0::other
# Sau khi map:
# (0, "other")


# =========================
# Buoc 2:
# Doc users.txt
# Tao RDD: UserID -> OccupationID
# =========================
users_rdd = sc.textFile(users_path)

user_occupation = users_rdd.map(split_line) \
    .map(lambda x: (int(x[0]), int(x[3])))

# Vi du:
# UserID::Gender::Age::Occupation::Zip-code
# 1::F::1::10::48067
# Sau khi map:
# (1, 10)


# =========================
# Buoc 3:
# Join users voi occupation de lay ten nghe nghiep
# Can doi key thanh OccupationID
# OccupationID -> UserID
# =========================
occupation_user = user_occupation.map(
    lambda x: (x[1], x[0])
)

# Join:
# OccupationID -> (UserID, OccupationName)
occupation_user_name = occupation_user.join(occupation_map)

# Doi lai thanh:
# UserID -> OccupationName
user_occupation_name = occupation_user_name.map(
    lambda x: (x[1][0], x[1][1])
)

# Vi du:
# (1, "academic/educator")


# =========================
# Buoc 4:
# Doc ratings_1.txt va ratings_2.txt
# Tao RDD: UserID -> Rating
# =========================
ratings_1 = sc.textFile(ratings_1_path)
ratings_2 = sc.textFile(ratings_2_path)

ratings = ratings_1.union(ratings_2)

user_ratings = ratings.map(split_line) \
    .map(lambda x: (int(x[0]), float(x[2])))

# Vi du:
# UserID::MovieID::Rating::Timestamp
# 1::1193::5::978300760
# Sau khi map:
# (1, 5.0)


# =========================
# Buoc 5:
# Join ratings voi user_occupation_name
# UserID -> (Rating, OccupationName)
# =========================
ratings_with_occupation = user_ratings.join(user_occupation_name)

# Vi du:
# (1, (5.0, "academic/educator"))


# =========================
# Buoc 6:
# Phat hanh key-value:
# OccupationName -> (Rating, 1)
# =========================
occupation_ratings = ratings_with_occupation.map(
    lambda x: (x[1][1], (x[1][0], 1))
)

# Vi du:
# ("academic/educator", (5.0, 1))


# =========================
# Buoc 7:
# Reduce de tinh tong diem va tong so luot danh gia
# OccupationName -> (total_rating, total_count)
# =========================
occupation_stats = occupation_ratings.reduceByKey(
    lambda a, b: (a[0] + b[0], a[1] + b[1])
)


# =========================
# Buoc 8:
# Tinh diem trung binh
# OccupationName -> (average_rating, total_count)
# =========================
occupation_avg = occupation_stats.mapValues(
    lambda x: (x[0] / x[1], x[1])
)


# =========================
# In ket qua ra terminal
# =========================
print("===== DIEM TRUNG BINH VA TONG SO LUOT DANH GIA THEO OCCUPATION =====")
print("Occupation | Average Rating | Total Ratings")

result = occupation_avg.sortBy(lambda x: x[0]).collect()

for occupation, value in result:
    avg_rating = value[0]
    count = value[1]
    print(f"{occupation} | {avg_rating:.2f} | {count}")


# =========================
# Luu ket qua ra HDFS
# =========================
output = occupation_avg.sortBy(lambda x: x[0]) \
    .map(lambda x: f"{x[0]} | Average Rating: {x[1][0]:.2f} | Total Ratings: {x[1][1]}")

output.saveAsTextFile(output_path)


# =========================
# Dung SparkContext
# =========================
sc.stop()