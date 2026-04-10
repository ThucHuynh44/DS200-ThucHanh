-- Load dữ liệu gốc
reviews = LOAD '/input/lab2/hotel-review.csv' USING PigStorage(';') AS (
    id: int,
    review: chararray,
    category: chararray,
    aspect: chararray,
    sentiment: chararray
);

-- PHAN 1: Thống kê tần số xuất hiện của các từ
-- Chỉ lấy mỗi review duy nhất theo (id, review)
unique_reviews = FOREACH reviews GENERATE
    id,
    review;

unique_reviews = DISTINCT unique_reviews;

-- Đưa review về chữ thường, bỏ ký tự đặc biệt, gom khoảng trắng
clean_reviews = FOREACH unique_reviews GENERATE
    id,
    TRIM(
        REPLACE(
            REPLACE(
                LOWER(review),
                '[^\\p{L}\\p{N}\\s]', ' '
            ),
            '\\s+', ' '
        )
    ) AS clean_review;

-- Tách review thành các từ theo khoảng trắng
review_words = FOREACH clean_reviews GENERATE
    FLATTEN(TOKENIZE(clean_review)) AS word;

-- Loại token rỗng
review_words = FILTER review_words BY word IS NOT NULL AND word != '';

-- Load stopword
stopwords_raw = LOAD '/input/lab2/stopwords.txt' USING PigStorage('\n') AS (stopword: chararray);

-- Chỉ bỏ ký tự CR do file Windows
stopwords = FOREACH stopwords_raw GENERATE
    REPLACE(stopword, '\r', '') AS stopword;

stopwords = FILTER stopwords BY stopword IS NOT NULL AND stopword != '';
stopwords = DISTINCT stopwords;

-- Loại stopword bằng JOIN
joined_words = JOIN review_words BY word LEFT OUTER, stopwords BY stopword;
filtered_words = FILTER joined_words BY stopwords::stopword IS NULL;

words_only = FOREACH filtered_words GENERATE
    review_words::word AS word;

-- Đếm tần số từ
grouped_words = GROUP words_only BY word;
word_frequency = FOREACH grouped_words GENERATE
    group AS word,
    COUNT(words_only) AS freq;

-- Chỉ lấy các từ xuất hiện trên 500 lần
words_over_500 = FILTER word_frequency BY freq > 500;

-- PHAN 2: Thống kê số bình luận theo từng category
-- Đếm trên dữ liệu gốc

grouped_category = GROUP reviews BY category;
category_count = FOREACH grouped_category GENERATE
    group AS category,
    COUNT(reviews) AS total_comments;

-- PHAN 3: Thống kê số bình luận theo từng aspect
-- Đếm trên dữ liệu gốc

grouped_aspect = GROUP reviews BY aspect;
aspect_count = FOREACH grouped_aspect GENERATE
    group AS aspect,
    COUNT(reviews) AS total_comments;

-- Lưu kết quả
STORE words_over_500   INTO '/lab2/output/bai2_words_over_500' USING PigStorage('\t');
STORE category_count   INTO '/lab2/output/bai2_category_count' USING PigStorage('\t');
STORE aspect_count     INTO '/lab2/output/bai2_aspect_count' USING PigStorage('\t');