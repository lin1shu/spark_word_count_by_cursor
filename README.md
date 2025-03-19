# Spark Word Count

A Python application for counting word frequencies in text documents using Apache Spark and PostgreSQL, with a web-based visualization interface.

[![CI Status](https://github.com/lin1shu/spark_word_count_by_cursor/workflows/Spark%20Word%20Count%20CI/badge.svg)](https://github.com/lin1shu/spark_word_count_by_cursor/actions)

## Features

- Count the frequency of each word in a text document
- Store results in CSV format or PostgreSQL database
- Update existing word counts in PostgreSQL
- Web application to visualize and explore word frequencies
- RESTful API endpoints for accessing word count data
- Properly structured Python package
- Comprehensive test suite
- Type-checked with mypy
- Configured with best practices for Python development

## Project Structure

```
spark_word_count/
├── .github/               # GitHub Actions workflows
├── data/                  # Sample text files for testing
├── docs/                  # Sphinx documentation
├── jars/                  # Required Java library JAR files
├── src/                   # Source code
│   └── spark_word_count/  # Main package
│       ├── __init__.py    # Package initialization
│       ├── __main__.py    # Main entry point
│       ├── config.py      # Configuration management
│       ├── core.py        # Core word counting functionality
│       ├── exceptions.py  # Custom exception classes
│       ├── logging.py     # Logging configuration
│       ├── webapp.py      # Flask web application
│       ├── templates/     # HTML templates for web interface
│       │   ├── base.html  # Base template with layout
│       │   ├── index.html # Homepage with statistics
│       │   └── search.html# Word search interface
│       └── db/            # Database integration
│           ├── __init__.py
│           └── postgres.py # PostgreSQL-specific code
├── tests/                 # Test suite
│   ├── unit/              # Unit tests
│   ├── integration/       # Integration tests
│   └── fixtures/          # Test fixtures
├── .flake8               # Flake8 configuration
├── .gitignore            # Git ignore rules
├── .isort.cfg            # isort configuration
├── .pre-commit-config.yaml # Pre-commit hooks
├── Makefile              # Common development tasks
├── pyproject.toml        # Black and mypy configuration
├── pytest.ini            # Pytest configuration
├── requirements.txt      # Python dependencies
├── setup.py              # Package installation configuration
└── README.md             # Project documentation
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

3. Install the package for development:

```bash
pip install -e ".[dev]"
```

## Development

This project uses several tools to ensure code quality:

- **pytest**: For running tests
- **flake8**: For linting
- **black**: For code formatting
- **isort**: For sorting imports
- **mypy**: For type checking
- **pre-commit**: For running checks before committing

To set up the development environment:

```bash
# Install development dependencies
pip install -e ".[dev]"

# Install pre-commit hooks
pre-commit install
```

Common development tasks can be run using the Makefile:

```bash
# Run tests
make test

# Run linting checks
make lint

# Format code
make format

# Generate documentation
make docs

# See all available commands
make help
```

## Usage

### Command Line Interface

Count words and save to CSV:

```bash
spark-word-count count data/sample_text.txt --output results
```

Count words and save to PostgreSQL:

```bash
spark-word-count count data/sample_text.txt --postgres
```

Update existing word counts in PostgreSQL:

```bash
spark-word-count count data/longer_sample.txt --postgres --update
```

Launch the web application:

```bash
# Set environment variables for configuration
export DB_HOST="localhost"
export WEB_PORT="5001"
export LOG_LEVEL="INFO"

# Run the web application
spark-word-count web --port 5001
# Or use the convenience make target
make run-web
```

### Environment Variables

The application can be configured using environment variables:

- `DB_NAME`: Database name (default: "wordcount")
- `DB_USER`: Database user (default: "postgres")
- `DB_PASSWORD`: Database password (default: "sparkdemo")
- `DB_HOST`: Database host (default: "localhost")
- `DB_PORT`: Database port (default: "5432")
- `WEB_HOST`: Web host to bind to (default: "0.0.0.0")
- `WEB_PORT`: Web port to listen on (default: 5001)
- `WEB_DEBUG`: Enable debug mode (default: false)
- `LOG_LEVEL`: Logging level (default: "INFO")
- `SPARK_DRIVER_MEMORY`: Spark driver memory (default: "4g")
- `SPARK_EXECUTOR_MEMORY`: Spark executor memory (default: "4g")

See `.env.example` for all available configuration options.

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
from spark_word_count.config import DatabaseConfig

# Configuration using the config system
db_config = DatabaseConfig(host="localhost", dbname="wordcount")
jdbc_url = db_config.get_jdbc_url()
jdbc_props = db_config.to_jdbc_properties()

# Basic word count to CSV
word_count("data/sample_text.txt", "results")

# Word count to PostgreSQL
word_count_postgres("data/sample_text.txt", jdbc_url, jdbc_props)

# Update existing word counts
word_count_update("data/longer_sample.txt", jdbc_url, jdbc_props)
```

## Docker Setup for PostgreSQL

```bash
docker run --name postgres-spark -e POSTGRES_PASSWORD=sparkdemo -e POSTGRES_DB=wordcount -p 5432:5432 -d postgres:13
```

## Performance

The application handles large text files (2GB+) efficiently using Apache Spark's distributed processing capabilities. For optimal performance with large files, adjust Spark memory settings:

```bash
spark-word-count count data/large_sample.txt --postgres --driver-memory 8g --executor-memory 8g --max-result-size 4g
```

## Documentation

To build and view the documentation:

```bash
# Install Sphinx and the ReadTheDocs theme
pip install sphinx sphinx_rtd_theme

# Generate documentation
make docs
```

The documentation will be available at `docs/_build/html/index.html`.

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

Before submitting your code, please ensure:
1. Tests are passing (`make test`)
2. Code is formatted (`make format`)
3. Linting checks pass (`make lint`)
4. Documentation is updated if needed (`make docs`)

## License

This project is licensed under the MIT License - see the LICENSE file for details. 