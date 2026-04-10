-- 1. Load dữ liệu (giữ nguyên 5 cột)
reviews = LOAD '/input/lab2/hotel-review.csv' USING PigStorage(';') AS (
    id: int,
    review: chararray,
    category: chararray,
    aspect: chararray,
    sentiment: chararray
);

-- 2. Loại bỏ các dòng trùng lặp y hệt nhau (giữ nguyên tất cả các cột)
unique_reviews = DISTINCT reviews;

-- 3. Làm sạch review (Giữ lại logic Regex xuất sắc của bạn)
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
    ) AS clean_review,
    category,
    aspect,
    sentiment;

-- 4. Tách review thành các từ
review_words = FOREACH clean_reviews GENERATE
    id,
    FLATTEN(TOKENIZE(clean_review)) AS word,
    category,
    aspect,
    sentiment;

-- 5. Loại token rỗng
review_words = FILTER review_words BY word IS NOT NULL AND word != '';

-- 6. Load stopword (Sửa lại PigStorage mặc định để không lỗi)
stopwords_raw = LOAD '/input/lab2/stopwords.txt' USING PigStorage() AS (stopword: chararray);

stopwords = FOREACH stopwords_raw GENERATE
    REPLACE(stopword, '\r', '') AS stopword;

stopwords = FILTER stopwords BY stopword IS NOT NULL AND stopword != '';
stopwords = DISTINCT stopwords;

-- 7. LEFT OUTER JOIN để đối chiếu stopword
joined_data = JOIN review_words BY word LEFT OUTER, stopwords BY stopword;

-- 8. Giữ lại những từ không nằm trong stopword
filtered_words = FILTER joined_data BY stopwords::stopword IS NULL;

-- 9. Dọn dẹp kết quả cuối (Đảm bảo có đủ 5 cột cho các bài sau)
result = FOREACH filtered_words GENERATE
    review_words::id        AS id,
    review_words::word      AS word,
    review_words::category  AS category,
    review_words::aspect    AS aspect,
    review_words::sentiment AS sentiment;

-- 10. Lưu kết quả (Đã đổi tên thư mục thành bai1_v2 để tránh lỗi trùng lặp)
STORE result INTO '/lab2/output/bai1_v2/' USING PigStorage(';');