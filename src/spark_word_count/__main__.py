"""
Main entry point for the Spark Word Count application.
"""

import argparse
import logging

from spark_word_count.db.postgres import word_count_postgres
from spark_word_count.logging import setup_logging
from spark_word_count.webapp import run_app


def main() -> None:
    """Main entry point for the application."""
    parser = argparse.ArgumentParser(
        description="Count word frequencies in a text document using PySpark."
    )
    parser.add_argument("input_path", nargs="?", help="Path to the input text file")
    parser.add_argument(
        "--output",
        help="Path where the word count results will be saved (CSV format)",
    )
    parser.add_argument(
        "--postgres",
        action="store_true",
        help="Save results to PostgreSQL instead of CSV",
    )
    parser.add_argument(
        "--update",
        action="store_true",
        help="Update existing word counts in PostgreSQL instead of overwriting",
    )
    parser.add_argument(
        "--driver-memory",
        default="4g",
        help="Driver memory allocation (default: 4g)",
    )
    parser.add_argument(
        "--executor-memory",
        default="4g",
        help="Executor memory allocation (default: 4g)",
    )
    parser.add_argument(
        "--max-result-size",
        default="2g",
        help="Maximum size of result collection (default: 2g)",
    )
    parser.add_argument(
        "--web",
        action="store_true",
        help="Start the web server instead of processing files",
    )
    parser.add_argument(
        "--debug", action="store_true", help="Run in debug mode (for web server)"
    )
    parser.add_argument(
        "--host",
        default="0.0.0.0",
        help="Host to bind the web server to (default: 0.0.0.0)",
    )
    parser.add_argument(
        "--port", type=int, default=5000, help="Port to run the web server on (default: 5000)"
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
        help="Set the logging level",
    )

    args = parser.parse_args()

    # Set up logging
    setup_logging(level=args.log_level)
    logger = logging.getLogger(__name__)

    if args.web:
        logger.info("Starting web server")
        run_app(debug=args.debug, host=args.host, port=args.port)
    elif args.input_path:
        logger.info(f"Processing file: {args.input_path}")
        
        # Use PostgreSQL
        jdbc_url = "jdbc:postgresql://localhost:5432/wordcount"
        db_properties = {
            "user": "postgres",
            "password": "sparkdemo",
            "driver": "org.postgresql.Driver",
        }
        
        word_count_postgres(
            args.input_path,
            jdbc_url,
            db_properties,
            memory=args.driver_memory,
            executor_memory=args.executor_memory,
            max_result_size=args.max_result_size,
        )
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
