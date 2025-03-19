# Spark Word Count

A Python application for counting word frequencies in text documents using Apache Spark and PostgreSQL, with a web-based visualization interface.

## Features

- Count the frequency of each word in a text document
- Store results in CSV format or PostgreSQL database
- Update existing word counts in PostgreSQL
- Web application to visualize and explore word frequencies
- RESTful API endpoints for accessing word count data
- Properly structured Python package

## Project Structure

```
spark_word_count/
├── data/                  # Sample text files for testing
├── jars/                  # Required Java library JAR files
├── venv/                  # Python virtual environment
├── src/                   # Source code
│   └── spark_word_count/  # Main package
│       ├── __init__.py    # Package initialization
│       ├── __main__.py    # Main entry point
│       ├── core.py        # Core word counting functionality
│       ├── webapp.py      # Flask web application
│       ├── templates/     # HTML templates for web interface
│       │   ├── base.html  # Base template with layout
│       │   ├── index.html # Homepage with statistics
│       │   └── search.html# Word search interface
│       └── db/            # Database integration
│           ├── __init__.py
│           └── postgres.py # PostgreSQL-specific code
├── requirements.txt       # Python dependencies
├── setup.py               # Package installation configuration
└── README.md              # Project documentation
```

## Prerequisites

- Python 3.6+
- Apache Spark 3.0+
- Java 8+ (required for Spark)
- PostgreSQL (optional, for database integration)
- Flask (for web application)

## Installation

1. Clone the repository:

```bash
git clone https://github.com/lin1shu/spark_word_count_by_cursor.git
cd spark_word_count_by_cursor
```

2. Create and activate a virtual environment:

```bash
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
```

3. Install the package:

```bash
pip install -e .
```

## Usage

### Command Line Interface

Count words and save to CSV:

```bash
spark-word-count data/sample_text.txt --output results
```

Count words and save to PostgreSQL:

```bash
spark-word-count data/sample_text.txt --postgres
```

Update existing word counts in PostgreSQL:

```bash
spark-word-count data/longer_sample.txt --postgres --update
```

Launch the web application:

```bash
export DB_HOST="localhost"  # Set your PostgreSQL host
spark-word-count web --port 5001
```

### Web Application

The web application provides a user-friendly interface to:
1. View overall word statistics (total words, unique words, avg/median frequency)
2. Visualize top words in a bar chart
3. Search for specific words and see their frequencies

Access the web application at: http://localhost:5001

### API Endpoints

- `GET /api/stats` - Get overall word count statistics
- `GET /api/top_words?limit=N` - Get top N most frequent words
- `GET /api/search?word=example` - Search for a specific word's frequency

### As a Library

```python
from spark_word_count.core import word_count
from spark_word_count.db.postgres import word_count_postgres, word_count_update

# Basic word count to CSV
word_count("data/sample_text.txt", "results")

# Word count to PostgreSQL
jdbc_url = "jdbc:postgresql://localhost:5432/wordcount"
db_properties = {
    "user": "postgres",
    "password": "sparkdemo",
    "driver": "org.postgresql.Driver"
}
word_count_postgres("data/sample_text.txt", jdbc_url, db_properties)

# Update existing word counts
word_count_update("data/longer_sample.txt", jdbc_url, db_properties)
```

## Docker Setup for PostgreSQL

```bash
docker run --name postgres-spark -e POSTGRES_PASSWORD=sparkdemo -e POSTGRES_DB=wordcount -p 5432:5432 -d postgres:13
```

## Performance

The application handles large text files (2GB+) efficiently using Apache Spark's distributed processing capabilities. For optimal performance with large files, adjust Spark memory settings:

```bash
spark-word-count data/large_sample.txt --postgres --driver-memory 8g --executor-memory 8g --max-result-size 4g
```

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## License

This project is licensed under the MIT License - see the LICENSE file for details. 