reviews = LOAD '/input/lab2/hotel-review.csv' USING PigStorage(';') AS (
    id: int,
    review: chararray,
    category: chararray,
    aspect: chararray,
    sentiment: chararray
);

-- Giữ mỗi review duy nhất theo (id, review, category, sentiment)
base_reviews = FOREACH reviews GENERATE
    id,
    review,
    category,
    sentiment;

base_reviews = DISTINCT base_reviews;

-- Làm sạch review
clean_reviews = FOREACH base_reviews GENERATE
    category,
    sentiment,
    TRIM(
        REPLACE(
            REPLACE(
                LOWER(review),
                '[^\\p{L}\\p{N}\\s]', ' '
            ),
            '\\s+', ' '
        )
    ) AS clean_review;

-- Tách từ
review_words = FOREACH clean_reviews GENERATE
    category,
    sentiment,
    FLATTEN(TOKENIZE(clean_review)) AS word;

review_words = FILTER review_words BY word IS NOT NULL AND word != '';

-- Load stopword
stopwords_raw = LOAD '/input/lab2/stopwords.txt' USING PigStorage('\n') AS (stopword: chararray);

stopwords = FOREACH stopwords_raw GENERATE
    REPLACE(stopword, '\r', '') AS stopword;

stopwords = FILTER stopwords BY stopword IS NOT NULL AND stopword != '';
stopwords = DISTINCT stopwords;

-- Loại stopword
joined_data = JOIN review_words BY word LEFT OUTER, stopwords BY stopword;
filtered_words = FILTER joined_data BY stopwords::stopword IS NULL;

words_only = FOREACH filtered_words GENERATE
    review_words::category AS category,
    review_words::sentiment AS sentiment,
    review_words::word AS word;

-- Đếm tần số theo (category, sentiment, word)
grouped_words = GROUP words_only BY (category, sentiment, word);

word_count = FOREACH grouped_words GENERATE
    FLATTEN(group) AS (category, sentiment, word),
    COUNT(words_only) AS freq;

-- Top 5 từ positive theo từng category
positive_words = FILTER word_count BY sentiment == 'positive';

grouped_positive = GROUP positive_words BY category;

top5_positive = FOREACH grouped_positive GENERATE
    FLATTEN(TOP(5, 3, positive_words));

-- Top 5 từ negative theo từng category
negative_words = FILTER word_count BY sentiment == 'negative';

grouped_negative = GROUP negative_words BY category;

top5_negative = FOREACH grouped_negative GENERATE
    FLATTEN(TOP(5, 3, negative_words));

STORE top5_positive INTO '/lab2/output/bai4_top5_positive_words' USING PigStorage('\t');
STORE top5_negative INTO '/lab2/output/bai4_top5_negative_words' USING PigStorage('\t');