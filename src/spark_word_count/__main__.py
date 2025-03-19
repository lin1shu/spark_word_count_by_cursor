"""
Main entry point for the Spark Word Count package.
"""

import sys
import argparse
from pathlib import Path

from spark_word_count.core import word_count
from spark_word_count.db.postgres import word_count_postgres, word_count_update

def main():
    """Main entry point for the Spark Word Count package."""
    parser = argparse.ArgumentParser(description="Count word frequencies in a text document using PySpark")
    
    parser.add_argument("input_path", help="Path to the input text file")
    parser.add_argument("--output", "-o", help="Path where the word count results will be saved")
    parser.add_argument("--postgres", "-p", action="store_true", help="Save results to PostgreSQL")
    parser.add_argument("--update", "-u", action="store_true", help="Update existing word counts in PostgreSQL")
    parser.add_argument("--driver-memory", default="4g", help="Driver memory allocation (default: 4g)")
    parser.add_argument("--executor-memory", default="4g", help="Executor memory allocation (default: 4g)")
    parser.add_argument("--max-result-size", default="2g", help="Maximum result size (default: 2g)")
    parser.add_argument("--batch-size", type=int, default=10000, help="Batch size for database operations (default: 10000)")
    
    args = parser.parse_args()
    
    # Validate input path
    input_path = Path(args.input_path)
    if not input_path.exists():
        print(f"Error: Input file {args.input_path} does not exist")
        sys.exit(1)
    
    if args.postgres:
        # PostgreSQL connection info
        jdbc_url = "jdbc:postgresql://localhost:5432/wordcount"
        db_properties = {
            "user": "postgres",
            "password": "sparkdemo",
            "driver": "org.postgresql.Driver"
        }
        
        if args.update:
            word_count_update(
                str(input_path), 
                jdbc_url, 
                db_properties, 
                memory=args.driver_memory,
                executor_memory=args.executor_memory,
                max_result_size=args.max_result_size,
                batch_size=args.batch_size
            )
        else:
            word_count_postgres(
                str(input_path), 
                jdbc_url, 
                db_properties, 
                memory=args.driver_memory,
                executor_memory=args.executor_memory,
                max_result_size=args.max_result_size,
                batch_size=args.batch_size
            )
    else:
        if not args.output:
            print("Error: Output path is required when not using PostgreSQL")
            sys.exit(1)
        
        word_count(
            str(input_path), 
            args.output,
            memory=args.driver_memory,
            executor_memory=args.executor_memory,
            max_result_size=args.max_result_size
        )

if __name__ == "__main__":
    main() 