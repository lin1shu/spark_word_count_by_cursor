"""
Core functionality for Spark Word Count.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, explode, length, lower, regexp_replace, split, trim
import logging
import os
from typing import Dict, List, Optional, Any

logger = logging.getLogger(__name__)


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
    spark = (
        SparkSession.builder.appName("WordCount")
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

    # Sort by count in descending order
    word_counts = word_counts.orderBy("count", ascending=False)

    # Save the results
    print(f"Saving results to {output_path}")
    word_counts.write.csv(output_path, header=True, mode="overwrite")

    print(f"Word count results saved to {output_path}")

    # Stop the Spark session
    spark.stop()

    return word_counts


def generate_sample_text(output_path: str, size_mb: int = 100) -> None:
    """
    Generate a sample text file for testing.
    
    Args:
        output_path: Path where the sample text file will be created
        size_mb: Approximate size of the file in MB
    """
    logger.info("Generating sample text file of %d MB at %s", size_mb, output_path)
    
    # Ensure the directory exists
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    
    # Sample text blocks to use for generating data
    text_blocks = [
        """
        Machine learning is a field of study that gives computers the ability to learn
        without being explicitly programmed. It is a branch of artificial intelligence
        that focuses on the development of algorithms that can access data and use it
        to learn for themselves.
        """,
        """
        Deep learning is part of a broader family of machine learning methods based on
        artificial neural networks with representation learning. Learning can be supervised,
        semi-supervised or unsupervised.
        """,
        """
        Natural language processing is a subfield of linguistics, computer science, and
        artificial intelligence concerned with the interactions between computers and
        human language, in particular how to program computers to process and analyze
        large amounts of natural language data.
        """,
        """
        Data science is an interdisciplinary field that uses scientific methods, processes,
        algorithms and systems to extract knowledge and insights from structured and
        unstructured data, and apply knowledge and actionable insights from data across
        a broad range of application domains.
        """,
        """
        Big data is a term used to describe the large volume of data – both structured
        and unstructured – that inundates a business on a day-to-day basis. But it's
        not the amount of data that's important. It's what organizations do with the
        data that matters.
        """,
        """
        The field of data mining uses methods from machine learning, statistics, and
        database systems to discover patterns and extract information from large datasets.
        It combines tools from statistics and artificial intelligence with database
        management to analyze large digital collections.
        """,
    ]
    
    # Calculate approximate number of iterations needed to reach the target file size
    # Assuming average of 500 bytes per block
    avg_block_size = sum(len(block) for block in text_blocks) / len(text_blocks)
    iterations = int((size_mb * 1024 * 1024) / avg_block_size)
    
    logger.info("Writing approximately %d text blocks", iterations)
    
    with open(output_path, "w") as f:
        for i in range(iterations):
            # Cycle through the text blocks
            block = text_blocks[i % len(text_blocks)]
            f.write(block)
            if i % 10000 == 0 and i > 0:
                logger.info("Written %d blocks", i)
    
    actual_size = os.path.getsize(output_path) / (1024 * 1024)
    logger.info("Sample text file generated (%.2f MB)", actual_size)


def validate_file_path(path: str) -> bool:
    """
    Validate that a file exists and is readable.
    
    Args:
        path: Path to the file to validate
        
    Returns:
        bool: True if the file exists and is readable, False otherwise
    """
    if not path:
        logger.error("File path is empty")
        return False
    
    if not os.path.isfile(path):
        logger.error("File does not exist: %s", path)
        return False
    
    if not os.access(path, os.R_OK):
        logger.error("File is not readable: %s", path)
        return False
    
    return True


def main() -> None:
    """Command line interface for the core functionality."""
    import argparse
    
    parser = argparse.ArgumentParser(description="Spark Word Count Core Utilities")
    subparsers = parser.add_subparsers(dest="command", help="Command to run")
    
    # Generate sample text command
    generate_parser = subparsers.add_parser(
        "generate", help="Generate a sample text file for testing"
    )
    generate_parser.add_argument(
        "output_path", help="Path where the sample text file will be created"
    )
    generate_parser.add_argument(
        "--size", type=int, default=100, help="Approximate size of the file in MB"
    )
    
    args = parser.parse_args()
    
    if args.command == "generate":
        generate_sample_text(args.output_path, args.size)
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
