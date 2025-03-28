"""
Web application for displaying word count results.
"""

import logging
from typing import Any, Dict, List, Tuple

import psycopg2
from flask import Flask, jsonify, render_template, request
from flask_cors import CORS
from psycopg2.extras import RealDictCursor

from spark_word_count.config import get_db_config

logger = logging.getLogger(__name__)


def get_db_connection() -> psycopg2.extensions.connection:
    """Create a connection to the PostgreSQL database.

    Returns:
        psycopg2.extensions.connection: A connection to the PostgreSQL database
    """
    try:
        db_config = get_db_config()
        connection = psycopg2.connect(
            **db_config  # Use the config directly without trying to rename keys
        )
        return connection
    except Exception as e:
        logger.error(f"Error connecting to database: {e}")
        raise


def get_top_words(limit: int = 10) -> List[Dict[str, Any]]:
    """Get the top N words by frequency.

    Args:
        limit: Number of top words to return (default: 10)

    Returns:
        List[Dict[str, Any]]: List of word count records, each with 'word' and 'count' keys
    """
    try:
        connection = get_db_connection()
        cursor = connection.cursor(cursor_factory=RealDictCursor)
        cursor.execute(
            """
            SELECT word, count 
            FROM word_counts 
            ORDER BY count DESC 
            LIMIT %s
            """,
            (limit,),
        )
        results = cursor.fetchall()
        cursor.close()
        connection.close()

        # Convert RealDictRow to Dict
        return [dict(row) for row in results]
    except Exception as e:
        logger.error(f"Error fetching top words: {e}")
        return []


def get_word_stats() -> Dict[str, Any]:
    """Get statistics about the word counts.

    Returns:
        Dict[str, Any]: Dictionary containing word count statistics
    """
    try:
        connection = get_db_connection()
        cursor = connection.cursor(cursor_factory=RealDictCursor)

        # Get total words
        cursor.execute("SELECT SUM(count) as total_words FROM word_counts")
        result = cursor.fetchone()
        total_words = 0 if result is None else result["total_words"]

        # Get unique words count
        cursor.execute("SELECT COUNT(*) as unique_words FROM word_counts")
        result = cursor.fetchone()
        unique_words = 0 if result is None else result["unique_words"]

        # Get average frequency
        cursor.execute("SELECT AVG(count) as avg_frequency FROM word_counts")
        result = cursor.fetchone()
        avg_frequency = 0 if result is None else result["avg_frequency"]

        # Get median frequency
        cursor.execute(
            """
            SELECT PERCENTILE_CONT(0.5) WITHIN GROUP(ORDER BY count) as median_frequency 
            FROM word_counts
            """
        )
        result = cursor.fetchone()
        median_frequency = 0 if result is None else result["median_frequency"]

        cursor.close()
        connection.close()

        return {
            "total_words": total_words,
            "unique_words": unique_words,
            "avg_frequency": avg_frequency,
            "median_frequency": median_frequency,
        }
    except Exception as e:
        logger.error(f"Error fetching word stats: {e}")
        return {
            "total_words": 0,
            "unique_words": 0,
            "avg_frequency": 0,
            "median_frequency": 0,
        }


def get_word_frequency(word: str) -> int:
    """Get the frequency of a specific word.

    Args:
        word: The word to look up

    Returns:
        int: The frequency of the word, or 0 if not found
    """
    try:
        connection = get_db_connection()
        cursor = connection.cursor()
        cursor.execute(
            "SELECT count FROM word_counts WHERE word = %s",
            (word.lower(),),
        )
        result = cursor.fetchone()
        cursor.close()
        connection.close()

        if result is not None and len(result) > 0:
            return int(result[0])
        return 0
    except Exception as e:
        logger.error(f"Error fetching word frequency: {e}")
        return 0


def get_frequency_distribution() -> Dict[str, int]:
    """Get the distribution of word frequencies.

    Returns:
        Dict[str, int]: Dictionary with frequency ranges as keys and counts as values
    """
    try:
        connection = get_db_connection()
        cursor = connection.cursor()

        # Define frequency ranges
        ranges = [
            (1, 10),
            (11, 100),
            (101, 1000),
            (1001, 10000),
            (10001, 100000),
            (100001, float("inf")),
        ]

        distribution: Dict[str, int] = {}

        for start, end in ranges:
            if end == float("inf"):
                cursor.execute(
                    "SELECT COUNT(*) FROM word_counts WHERE count >= %s",
                    (start,),
                )
                range_name = f"{start}+"
            else:
                cursor.execute(
                    "SELECT COUNT(*) FROM word_counts WHERE count >= %s AND count <= %s",
                    (start, end),
                )
                range_name = f"{start}-{end}"

            result = cursor.fetchone()
            if result is not None and len(result) > 0:
                distribution[range_name] = int(result[0])
            else:
                distribution[range_name] = 0

        cursor.close()
        connection.close()
        return distribution
    except Exception as e:
        logger.error(f"Error fetching frequency distribution: {e}")
        return {}


def create_app() -> Flask:
    """Create the Flask application.

    Returns:
        Flask: The Flask application instance
    """
    app = Flask(__name__, static_folder="static", template_folder="templates")
    CORS(app)

    # Add custom filters
    @app.template_filter("format_number")
    def format_number(value):
        """Format a number with thousand separators."""
        try:
            return f"{int(value):,}"
        except (ValueError, TypeError):
            try:
                return f"{float(value):.2f}"
            except (ValueError, TypeError):
                return value

    @app.route("/")
    def index() -> str:
        """Render the main index page.

        Returns:
            str: Rendered HTML template
        """
        # Get query parameter for number of top words
        top_n = request.args.get("top", default=10, type=int)
        top_n = min(max(top_n, 5), 100)  # Limit between 5 and 100

        # Get top words
        top_words = get_top_words(top_n)

        # Get statistics
        stats = get_word_stats()

        return render_template(
            "index.html",
            top_words=[(item["word"], item["count"]) for item in top_words],
            stats=stats,
            top_n=top_n,
        )

    @app.route("/api/top_words")
    def api_top_words() -> Tuple[Any, int]:
        """API endpoint to get top words.

        Returns:
            Tuple[Any, int]: JSON response and HTTP status code
        """
        try:
            limit = request.args.get("limit", default=10, type=int)
            return jsonify(get_top_words(limit)), 200
        except Exception as e:
            logger.error(f"Error in top_words API: {e}")
            return jsonify({"error": str(e)}), 500

    @app.route("/api/stats")
    def api_stats() -> Tuple[Any, int]:
        """API endpoint to get word statistics.

        Returns:
            Tuple[Any, int]: JSON response and HTTP status code
        """
        try:
            return jsonify(get_word_stats()), 200
        except Exception as e:
            logger.error(f"Error in stats API: {e}")
            return jsonify({"error": str(e)}), 500

    @app.route("/api/word/<word>")
    def api_word(word: str) -> Tuple[Any, int]:
        """API endpoint to get frequency for a specific word.

        Args:
            word: The word to look up

        Returns:
            Tuple[Any, int]: JSON response and HTTP status code
        """
        try:
            frequency = get_word_frequency(word)
            return jsonify({"word": word, "frequency": frequency}), 200
        except Exception as e:
            logger.error(f"Error in word API: {e}")
            return jsonify({"error": str(e)}), 500

    @app.route("/api/distribution")
    def api_distribution() -> Tuple[Any, int]:
        """API endpoint to get frequency distribution.

        Returns:
            Tuple[Any, int]: JSON response and HTTP status code
        """
        try:
            distribution = get_frequency_distribution()
            return jsonify(distribution), 200
        except Exception as e:
            logger.error(f"Error in distribution API: {e}")
            return jsonify({"error": str(e)}), 500

    return app


def run_app(debug: bool = False, host: str = "0.0.0.0", port: int = 5000) -> None:
    """Run the Flask application.

    Args:
        debug: Enable debug mode (default: False)
        host: Host to bind the server to (default: 0.0.0.0)
        port: Port to run the server on (default: 5000)
    """
    app = create_app()
    app.run(debug=debug, host=host, port=port)


def main() -> None:
    """Command line interface for the web application."""
    import argparse

    parser = argparse.ArgumentParser(description="Word Count Web Application")
    parser.add_argument("--debug", action="store_true", help="Run in debug mode")
    parser.add_argument(
        "--host", default="0.0.0.0", help="Host to bind the server to (default: 0.0.0.0)"
    )
    parser.add_argument(
        "--port", type=int, default=5000, help="Port to run the server on (default: 5000)"
    )

    args = parser.parse_args()
    run_app(debug=args.debug, host=args.host, port=args.port)


if __name__ == "__main__":
    main()
