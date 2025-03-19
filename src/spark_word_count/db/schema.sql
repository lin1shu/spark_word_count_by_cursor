-- PostgreSQL schema for the Spark Word Count application

-- Word counts table - stores all word frequencies
CREATE TABLE IF NOT EXISTS word_counts (
    word VARCHAR(255) PRIMARY KEY,
    count BIGINT NOT NULL
);

-- Create an index for faster lookups by count (for top words queries)
CREATE INDEX IF NOT EXISTS word_counts_count_idx ON word_counts (count DESC);

-- Create a statistics view for caching common statistics
CREATE OR REPLACE VIEW word_stats AS
SELECT
    SUM(count) AS total_words,
    COUNT(*) AS unique_words,
    AVG(count) AS avg_frequency,
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY count) AS median_frequency
FROM word_counts;

-- Example queries:

-- Get top 10 words by frequency
-- SELECT word, count FROM word_counts ORDER BY count DESC LIMIT 10;

-- Search for words containing a pattern
-- SELECT word, count FROM word_counts WHERE word ILIKE '%pattern%' ORDER BY count DESC;

-- Get overall statistics
-- SELECT * FROM word_stats; 