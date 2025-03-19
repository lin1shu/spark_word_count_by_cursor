"""
Test fixtures for integration tests.
"""

import os
import tempfile
from typing import Generator, Dict, Any, List, Tuple

import pytest


@pytest.fixture
def sample_text_file() -> Generator[str, None, None]:
    """Create a temporary file with sample text for testing."""
    with tempfile.NamedTemporaryFile(mode='w+', delete=False, suffix='.txt') as temp_file:
        # Write some sample text
        temp_file.write("""
        This is a sample text file for testing the Spark Word Count application.
        It contains some repeated words to create interesting word frequencies.
        The word 'the' appears multiple times, as does the word 'word'.
        This sample should be sufficient for testing basic word counting functionality.
        """)
        temp_path = temp_file.name
    
    yield temp_path
    
    # Clean up the file after the test
    os.unlink(temp_path)


@pytest.fixture
def mock_db_connection() -> Dict[str, Any]:
    """Mock database connection parameters for testing."""
    return {
        'dbname': 'test_wordcount',
        'user': 'test_user',
        'password': 'test_password',
        'host': 'localhost',
        'port': '5432'
    }


@pytest.fixture
def mock_word_counts() -> List[Tuple[str, int]]:
    """Mock word count data for testing."""
    return [
        ('the', 150),
        ('word', 120),
        ('and', 100),
        ('is', 80),
        ('test', 60),
        ('sample', 50),
        ('count', 40),
        ('spark', 30),
        ('application', 25),
        ('python', 20)
    ]


@pytest.fixture
def mock_word_stats() -> Dict[str, Any]:
    """Mock word statistics for testing."""
    return {
        'total_words': 1000,
        'unique_words': 100,
        'avg_frequency': 10.0,
        'median_frequency': 5.0
    } 