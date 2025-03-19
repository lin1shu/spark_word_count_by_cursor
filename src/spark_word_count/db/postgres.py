"""
PostgreSQL integration for Spark Word Count.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, split, lower, trim, regexp_replace, col, count, when, length

def word_count_postgres(input_path, jdbc_url, db_properties, jdbc_jar_path="jars/postgresql-42.6.0.jar", 
                       memory="4g", executor_memory="4g", max_result_size="2g", batch_size=10000):
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
    spark = SparkSession.builder \
        .appName("WordCount") \
        .config("spark.jars", jdbc_jar_path) \
        .config("spark.driver.memory", memory) \
        .config("spark.executor.memory", executor_memory) \
        .config("spark.driver.maxResultSize", max_result_size) \
        .config("spark.sql.shuffle.partitions", "10") \
        .config("spark.default.parallelism", "10") \
        .getOrCreate()
    
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
                "\\s+"
            )
        ).alias("word")
    )
    
    # Filter out empty strings and short words (usually not meaningful)
    words_df = words_df.filter((col("word") != "") & (length(col("word")) > 2))
    
    # Count word frequencies
    word_counts = words_df.groupBy("word").agg(count("*").alias("count"))
    
    # Sort by count in descending order
    word_counts = word_counts.orderBy("count", ascending=False)
    
    # Add JDBC batch options for better performance with large datasets
    jdbc_properties = db_properties.copy()
    jdbc_properties["batchsize"] = str(batch_size)
    
    # Create the table in PostgreSQL if it doesn't exist
    print(f"Saving results to PostgreSQL table 'word_counts'")
    word_counts.write \
        .option("numPartitions", 10) \
        .jdbc(
            url=jdbc_url, 
            table="word_counts", 
            mode="overwrite", 
            properties=jdbc_properties
        )
    
    print(f"Word count results saved to PostgreSQL table 'word_counts'")
    
    # Stop the Spark session
    spark.stop()
    
    return word_counts

def word_count_update(input_path, jdbc_url, db_properties, jdbc_jar_path="jars/postgresql-42.6.0.jar",
                     memory="4g", executor_memory="4g", max_result_size="2g", batch_size=10000):
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
    spark = SparkSession.builder \
        .appName("WordCountUpdate") \
        .config("spark.jars", jdbc_jar_path) \
        .config("spark.driver.memory", memory) \
        .config("spark.executor.memory", executor_memory) \
        .config("spark.driver.maxResultSize", max_result_size) \
        .config("spark.sql.shuffle.partitions", "10") \
        .config("spark.default.parallelism", "10") \
        .getOrCreate()
    
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
                "\\s+"
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
        existing_word_counts = spark.read \
            .jdbc(
                url=jdbc_url,
                table="word_counts",
                properties=db_properties
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
        # Create a temporary view of new counts for better join performance
        new_word_counts.createOrReplaceTempView("new_counts")
        
        # Create a temporary view of existing counts for better join performance
        existing_word_counts.createOrReplaceTempView("existing_counts")
        
        # Perform the join using SQL for better performance with large datasets
        combined_counts = spark.sql("""
            SELECT 
                COALESCE(e.word, n.word) as word,
                COALESCE(e.count, 0) + COALESCE(n.new_count, 0) as count
            FROM 
                existing_counts e
            FULL OUTER JOIN 
                new_counts n
            ON 
                e.word = n.word
        """)
        
        # Sort by count in descending order
        combined_counts = combined_counts.orderBy("count", ascending=False)
        
        # Save the results, overwriting the existing table
        print(f"Saving updated results to PostgreSQL table 'word_counts'")
        combined_counts.write \
            .option("numPartitions", 10) \
            .jdbc(
                url=jdbc_url, 
                table="word_counts", 
                mode="overwrite", 
                properties=jdbc_properties
            )
        
        print(f"Updated word counts in PostgreSQL table 'word_counts'")
        result_df = combined_counts
    else:
        # Rename column to match expected schema
        new_counts_df = new_word_counts.withColumnRenamed("new_count", "count")
        
        # Sort by count in descending order
        new_counts_df = new_counts_df.orderBy("count", ascending=False)
        
        # Create new table
        print(f"Creating new PostgreSQL table 'word_counts'")
        new_counts_df.write \
            .option("numPartitions", 10) \
            .jdbc(
                url=jdbc_url, 
                table="word_counts", 
                mode="overwrite", 
                properties=jdbc_properties
            )
        
        print(f"Created new PostgreSQL table 'word_counts'")
        result_df = new_counts_df
    
    # Stop the Spark session
    spark.stop()
    
    return result_df

def main():
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
        "driver": "org.postgresql.Driver"
    }
    
    if update_mode:
        word_count_update(input_path, jdbc_url, db_properties)
    else:
        word_count_postgres(input_path, jdbc_url, db_properties)

if __name__ == "__main__":
    main() 