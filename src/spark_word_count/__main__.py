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
    
    subparsers = parser.add_subparsers(dest="command", help="Commands")
    
    # Word count command
    count_parser = subparsers.add_parser("count", help="Count words in a text file")
    count_parser.add_argument("input_path", help="Path to the input text file")
    count_parser.add_argument("--output", "-o", help="Path where the word count results will be saved")
    count_parser.add_argument("--postgres", "-p", action="store_true", help="Save results to PostgreSQL")
    count_parser.add_argument("--update", "-u", action="store_true", help="Update existing word counts in PostgreSQL")
    count_parser.add_argument("--driver-memory", default="4g", help="Driver memory allocation (default: 4g)")
    count_parser.add_argument("--executor-memory", default="4g", help="Executor memory allocation (default: 4g)")
    count_parser.add_argument("--max-result-size", default="2g", help="Maximum result size (default: 2g)")
    count_parser.add_argument("--batch-size", type=int, default=10000, help="Batch size for database operations (default: 10000)")
    
    # Web application command
    web_parser = subparsers.add_parser("web", help="Run the web application")
    web_parser.add_argument("--host", default="0.0.0.0", help="Host to run the web application on")
    web_parser.add_argument("--port", type=int, default=5000, help="Port to run the web application on")
    web_parser.add_argument("--debug", action="store_true", help="Run the web application in debug mode")
    
    args = parser.parse_args()
    
    if args.command == "web":
        # Import here to avoid circular imports
        from spark_word_count.webapp import app
        app.run(host=args.host, port=args.port, debug=args.debug)
        return
    
    if args.command == "count" or args.command is None:  # Default to count for backward compatibility
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
    else:
        parser.print_help()

if __name__ == "__main__":
    main() 