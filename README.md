# Spark Word Count

A Python application for counting word frequencies in text documents using Apache Spark and PostgreSQL.

## Features

- Count the frequency of each word in a text document
- Store results in CSV format or PostgreSQL database
- Update existing word counts in PostgreSQL
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

## Installation

1. Clone the repository:

```bash
git clone https://github.com/yourusername/spark_word_count.git
cd spark_word_count
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

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## License

This project is licensed under the MIT License - see the LICENSE file for details. 