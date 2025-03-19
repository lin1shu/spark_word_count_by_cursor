"""
Web application to display word count statistics from PostgreSQL database.
"""

import os

import matplotlib
import pandas as pd
import psycopg2

matplotlib.use("Agg")  # Use non-interactive backend
import base64
from io import BytesIO

import matplotlib.pyplot as plt
from flask import Flask, jsonify, render_template, request

app = Flask(__name__, template_folder=os.path.join(os.path.dirname(__file__), "templates"))


# Add a format_number filter to Jinja2
@app.template_filter("format_number")
def format_number(value):
    """Format a number with commas as thousands separators."""
    if value is None:
        return "N/A"
    if isinstance(value, (int, float)):
        return f"{value:,}"
    return value


# Database connection parameters - configured for Docker container
DB_PARAMS = {
    "dbname": "wordcount",
    "user": "postgres",
    "password": "sparkdemo",
    "host": os.environ.get(
        "DB_HOST", "localhost"
    ),  # Use environment variable or default to localhost
    "port": "5432",
}


def get_db_connection():
    """Create a database connection."""
    try:
        conn = psycopg2.connect(**DB_PARAMS)
        return conn
    except Exception as e:
        print(f"Error connecting to database: {e}")
        return None


def get_top_words(limit=20):
    """Get the top N words by frequency."""
    conn = get_db_connection()
    if not conn:
        return None

    try:
        cur = conn.cursor()
        cur.execute("SELECT word, count FROM word_counts ORDER BY count DESC LIMIT %s", (limit,))
        rows = cur.fetchall()
        return rows
    except Exception as e:
        print(f"Error fetching top words: {e}")
        return None
    finally:
        conn.close()


def get_word_stats():
    """Get general statistics about the word counts."""
    conn = get_db_connection()
    if not conn:
        return None

    try:
        cur = conn.cursor()
        stats = {}

        # Total number of words
        cur.execute("SELECT SUM(count) FROM word_counts")
        stats["total_words"] = cur.fetchone()[0]

        # Number of unique words
        cur.execute("SELECT COUNT(*) FROM word_counts")
        stats["unique_words"] = cur.fetchone()[0]

        # Average word frequency
        cur.execute("SELECT AVG(count) FROM word_counts")
        stats["avg_frequency"] = round(cur.fetchone()[0], 2)

        # Median word frequency (more complex in PostgreSQL)
        cur.execute("SELECT PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY count) FROM word_counts")
        stats["median_frequency"] = round(cur.fetchone()[0], 2)

        return stats
    except Exception as e:
        print(f"Error fetching word stats: {e}")
        return None
    finally:
        conn.close()


def search_word(word):
    """Search for a specific word in the database."""
    conn = get_db_connection()
    if not conn:
        return None

    try:
        cur = conn.cursor()
        cur.execute("SELECT word, count FROM word_counts WHERE word ILIKE %s", (f"%{word}%",))
        rows = cur.fetchall()
        return rows
    except Exception as e:
        print(f"Error searching for word: {e}")
        return None
    finally:
        conn.close()


def generate_bar_chart(data, title="Top Words", xlabel="Words", ylabel="Frequency"):
    """Generate a bar chart for the word frequencies."""
    if not data:
        return None

    # Convert data to pandas DataFrame
    df = pd.DataFrame(data, columns=["word", "count"])

    # Create the bar chart
    plt.figure(figsize=(12, 6))
    bars = plt.bar(df["word"], df["count"], color="skyblue")
    plt.title(title, fontsize=16)
    plt.xlabel(xlabel, fontsize=12)
    plt.ylabel(ylabel, fontsize=12)
    plt.xticks(rotation=45, ha="right")
    plt.tight_layout()

    # Add value labels on top of each bar
    for bar in bars:
        height = bar.get_height()
        plt.text(
            bar.get_x() + bar.get_width() / 2.0,
            height,
            f"{height:,}",
            ha="center",
            va="bottom",
            rotation=0,
        )

    # Save the figure to a bytes buffer
    buffer = BytesIO()
    plt.savefig(buffer, format="png")
    buffer.seek(0)

    # Convert the buffer to a base64-encoded string
    image_base64 = base64.b64encode(buffer.getvalue()).decode("utf-8")
    plt.close()

    return image_base64


@app.route("/")
def index():
    """Home page displaying the word count statistics."""
    top_n = request.args.get("top", 20, type=int)
    top_words = get_top_words(top_n)
    stats = get_word_stats()

    # Generate the bar chart
    chart = generate_bar_chart(top_words, f"Top {top_n} Words by Frequency")

    return render_template("index.html", top_words=top_words, stats=stats, chart=chart, top_n=top_n)


@app.route("/search")
def search():
    """Search for words in the database."""
    query = request.args.get("q", "")
    if not query:
        return render_template("search.html", results=None)

    results = search_word(query)

    # Generate a chart if we have results
    chart = None
    if results and len(results) <= 20:  # Only generate chart for reasonable number of results
        chart = generate_bar_chart(results, f"Search Results for '{query}'")

    return render_template("search.html", results=results, query=query, chart=chart)


@app.route("/api/top_words")
def api_top_words():
    """API endpoint for getting top words."""
    limit = request.args.get("limit", 20, type=int)
    words = get_top_words(limit)
    if words:
        return jsonify([{"word": word, "count": count} for word, count in words])
    return jsonify([])


@app.route("/api/stats")
def api_stats():
    """API endpoint for getting word statistics."""
    stats = get_word_stats()
    if stats:
        return jsonify(stats)
    return jsonify({})


@app.route("/api/search")
def api_search():
    """API endpoint for searching words."""
    query = request.args.get("q", "")
    if not query:
        return jsonify([])

    results = search_word(query)
    if results:
        return jsonify([{"word": word, "count": count} for word, count in results])
    return jsonify([])


def main():
    """Run the Flask application."""
    app.run(debug=True, host="0.0.0.0", port=5000)


if __name__ == "__main__":
    main()
