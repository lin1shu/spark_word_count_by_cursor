"""
Core functionality for Spark Word Count.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, split, lower, trim, regexp_replace, col, count, length

def word_count(input_path, output_path, memory="4g", executor_memory="4g", max_result_size="2g"):
    """
    Count word frequencies in a text document using PySpark.
    
    Args:
        input_path (str): Path to the input text file
        output_path (str): Path where the word count results will be saved
        memory (str): Driver memory allocation
        executor_memory (str): Executor memory allocation
        max_result_size (str): Maximum size of result collection
    """
    # Initialize Spark session with optimized configuration for large files
    spark = SparkSession.builder \
        .appName("WordCount") \
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
    
    # Save the results
    print(f"Saving results to {output_path}")
    word_counts.write.csv(output_path, header=True, mode="overwrite")
    
    print(f"Word count results saved to {output_path}")
    
    # Stop the Spark session
    spark.stop()
    
    return word_counts

def main():
    """Command line interface for the word count functionality."""
    import sys
    
    if len(sys.argv) != 3:
        print("Usage: python -m spark_word_count.core <input_path> <output_path>")
        sys.exit(1)
    
    input_path = sys.argv[1]
    output_path = sys.argv[2]
    
    word_count(input_path, output_path)

if __name__ == "__main__":
    main() 