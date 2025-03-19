"""
Unit tests for the web application.
"""

import json
from unittest import mock

import pytest

from spark_word_count.webapp import create_app


@pytest.fixture
def app():
    """Create a test Flask application."""
    with mock.patch("spark_word_count.webapp.get_db_connection") as mock_db_conn:
        # Create a mock cursor
        mock_cursor = mock.MagicMock()
        mock_db_conn.return_value.cursor.return_value = mock_cursor

        # Set up mock responses for different queries
        def execute_side_effect(query, *args, **kwargs):
            if "SUM" in query and "total_words" in query:
                mock_cursor.fetchone.return_value = {"total_words": 1000000}
            elif "COUNT" in query and "unique_words" in query:
                mock_cursor.fetchone.return_value = {"unique_words": 10000}
            elif "AVG" in query:
                mock_cursor.fetchone.return_value = {"avg_frequency": 100.5}
            elif "PERCENTILE_CONT" in query:
                mock_cursor.fetchone.return_value = {"median_frequency": 20.0}
            elif "ORDER BY count DESC" in query:
                mock_cursor.fetchall.return_value = [
                    {"word": "the", "count": 50000},
                    {"word": "and", "count": 40000},
                    {"word": "to", "count": 30000},
                ]

        mock_cursor.execute.side_effect = execute_side_effect

        app = create_app()
        app.config["TESTING"] = True

        with app.test_client() as client:
            yield client


def test_index_route(app):
    """Test the index route."""
    response = app.get("/")
    assert response.status_code == 200


def test_api_top_words(app):
    """Test the top words API endpoint."""
    response = app.get("/api/top_words")
    assert response.status_code == 200

    data = json.loads(response.data)
    assert isinstance(data, list)
    assert len(data) > 0
    assert "word" in data[0]
    assert "count" in data[0]


def test_api_stats(app):
    """Test the stats API endpoint."""
    response = app.get("/api/stats")
    assert response.status_code == 200

    data = json.loads(response.data)
    assert "total_words" in data
    assert "unique_words" in data
    assert "avg_frequency" in data
    assert "median_frequency" in data
    assert data["total_words"] == 1000000
    assert data["unique_words"] == 10000
