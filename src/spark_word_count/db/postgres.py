"""
PostgreSQL integration for Spark Word Count.
"""

from typing import Dict

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, explode, length, lower, regexp_replace, split, trim


def word_count_postgres(
    input_path: str,
    jdbc_url: str,
    db_properties: Dict[str, str],
    jdbc_jar_path: str = "jars/postgresql-42.6.0.jar",
    memory: str = "4g",
    executor_memory: str = "4g",
    max_result_size: str = "2g",
    batch_size: int = 10000,
) -> None:
    """
    Count word frequencies in a text document using PySpark and save results to PostgreSQL.

    Args:
        input_path (str): Path to the input text file
        jdbc_url (str): JDBC URL for the PostgreSQL database
        db_properties (dict): Properties for the PostgreSQL connection
        jdbc_jar_path (str): Path to the PostgreSQL JDBC driver jar
        memory (str): Driver memory allocation
        executor_memory (str): Executor memory allocation
        max_result_size (str): Maximum size of result collection
        batch_size (int): Batch size for writing to PostgreSQL
    """
    # Initialize Spark session with PostgreSQL JDBC driver and optimized configuration
    spark = (
        SparkSession.builder.appName("WordCount")
        .config("spark.jars", jdbc_jar_path)
        .config("spark.driver.memory", memory)
        .config("spark.executor.memory", executor_memory)
        .config("spark.driver.maxResultSize", max_result_size)
        .config("spark.sql.shuffle.partitions", "10")
        .config("spark.default.parallelism", "10")
        .getOrCreate()
    )

    # Set log level to reduce verbosity
    spark.sparkContext.setLogLevel("WARN")

    print(f"Processing file: {input_path}")
    print(f"Spark UI available at: {spark.sparkContext.uiWebUrl}")

    # Read the input text file
    df = spark.read.text(input_path)

    # Split the text into words, convert to lowercase, and clean up
    words_df = df.select(
        explode(
            split(
                # Remove punctuation and convert to lowercase
                regexp_replace(lower(trim(col("value"))), "[^a-zA-Z\\s]", ""),
                "\\s+",
            )
        ).alias("word")
    )

    # Filter out empty strings and short words (usually not meaningful)
    words_df = words_df.filter((col("word") != "") & (length(col("word")) > 2))

    # Count word frequencies
    word_counts = words_df.groupBy("word").agg(count("*").alias("count"))

    # Print some word count statistics
    print("Word count statistics:")
    try:
        total_words = word_counts.agg({"count": "sum"}).collect()[0][0]
        print(f"Total words: {total_words}")
    except Exception:
        print("Could not calculate total words")

    try:
        unique_words = word_counts.count()
        print(f"Unique words: {unique_words}")
    except Exception:
        print("Could not calculate unique words")

    # Save word counts to PostgreSQL
    word_counts.write.jdbc(
        url=jdbc_url, table="word_counts", mode="overwrite", properties=db_properties
    )

    print("Word counts successfully saved to PostgreSQL")
    spark.stop()


def word_count_update(
    input_path: str,
    jdbc_url: str,
    db_properties: Dict[str, str],
    jdbc_jar_path: str = "jars/postgresql-42.6.0.jar",
    memory: str = "4g",
    executor_memory: str = "4g",
    max_result_size: str = "2g",
    batch_size: int = 10000,
) -> None:
    """
    Count word frequencies in a text document using PySpark and update results in PostgreSQL.

    Args:
        input_path (str): Path to the input text file
        jdbc_url (str): JDBC URL for the PostgreSQL database
        db_properties (dict): Properties for the PostgreSQL connection
        jdbc_jar_path (str): Path to the PostgreSQL JDBC driver jar
        memory (str): Driver memory allocation
        executor_memory (str): Executor memory allocation
        max_result_size (str): Maximum size of result collection
        batch_size (int): Batch size for writing to PostgreSQL
    """
    # Initialize Spark session with PostgreSQL JDBC driver and optimized configuration
    spark = (
        SparkSession.builder.appName("WordCountUpdate")
        .config("spark.jars", jdbc_jar_path)
        .config("spark.driver.memory", memory)
        .config("spark.executor.memory", executor_memory)
        .config("spark.driver.maxResultSize", max_result_size)
        .config("spark.sql.shuffle.partitions", "10")
        .config("spark.default.parallelism", "10")
        .getOrCreate()
    )

    # Set log level to reduce verbosity
    spark.sparkContext.setLogLevel("WARN")

    print(f"Processing file: {input_path}")
    print(f"Spark UI available at: {spark.sparkContext.uiWebUrl}")

    # Read the input text file
    df = spark.read.text(input_path)

    # Split the text into words, convert to lowercase, and clean up
    words_df = df.select(
        explode(
            split(
                # Remove punctuation and convert to lowercase
                regexp_replace(lower(trim(col("value"))), "[^a-zA-Z\\s]", ""),
                "\\s+",
            )
        ).alias("word")
    )

    # Filter out empty strings and short words
    words_df = words_df.filter((col("word") != "") & (length(col("word")) > 2))

    # Count word frequencies in the new text
    new_word_counts = words_df.groupBy("word").agg(count("*").alias("new_count"))

    # Add JDBC batch options for better performance with large datasets
    jdbc_properties = db_properties.copy()
    jdbc_properties["batchsize"] = str(batch_size)

    # Try to load existing word counts from PostgreSQL
    try:
        existing_word_counts = spark.read.jdbc(
            url=jdbc_url, table="word_counts", properties=db_properties
        )

        # Check if the table exists and has the expected schema
        if "word" in existing_word_counts.columns and "count" in existing_word_counts.columns:
            print("Found existing word_counts table, will update counts")
            has_existing_table = True
        else:
            print("Found table with unexpected schema, will create new table")
            has_existing_table = False
    except Exception as e:
        print(f"No existing word_counts table found, will create new: {str(e)}")
        has_existing_table = False

    if has_existing_table:
        # Join existing and new word counts
        joined = existing_word_counts.join(
            new_word_counts, existing_word_counts.word == new_word_counts.word, "full_outer"
        )

        # Update the counts (add new_count to count, or use new_count if count is null)
        updated_counts = joined.select(
            col("word").alias("word"),
            (
                col("count") + col("new_count")
                if col("count").isNotNull() and col("new_count").isNotNull()
                else col("count") if col("count").isNotNull() else col("new_count")
            ).alias("count"),
        )

        # Save updated word counts to PostgreSQL
        updated_counts.write.jdbc(
            url=jdbc_url, table="word_counts", mode="overwrite", properties=jdbc_properties
        )
        print("Word counts successfully updated in PostgreSQL")
    else:
        # Create a new word_counts table
        new_word_counts = new_word_counts.withColumnRenamed("new_count", "count")
        new_word_counts.write.jdbc(
            url=jdbc_url, table="word_counts", mode="overwrite", properties=jdbc_properties
        )
        print("Word counts successfully saved to PostgreSQL (new table)")

    # Print some statistics
    try:
        total_words = (
            spark.read.jdbc(url=jdbc_url, table="word_counts", properties=db_properties)
            .agg({"count": "sum"})
            .collect()[0][0]
        )
        print(f"Total words in database: {total_words}")
    except Exception:
        print("Could not calculate total words in database")

    spark.stop()


def main() -> None:
    """Command line interface for the PostgreSQL word count functionality."""
    import sys

    if len(sys.argv) < 2:
        print("Usage: python -m spark_word_count.db.postgres <input_path> [--update]")
        sys.exit(1)

    input_path = sys.argv[1]
    update_mode = "--update" in sys.argv

    # PostgreSQL connection info
    jdbc_url = "jdbc:postgresql://localhost:5432/wordcount"
    db_properties = {
        "user": "postgres",
        "password": "sparkdemo",
        "driver": "org.postgresql.Driver",
    }

    if update_mode:
        word_count_update(input_path, jdbc_url, db_properties)
    else:
        word_count_postgres(input_path, jdbc_url, db_properties)


if __name__ == "__main__":
    main()
