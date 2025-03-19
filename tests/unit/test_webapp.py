"""
Unit tests for the webapp module.
"""

import json
from unittest import mock

import pytest
from flask import Flask

from spark_word_count.webapp import app


@pytest.fixture
def client():
    """Create a test client for the Flask app."""
    app.config['TESTING'] = True
    with app.test_client() as client:
        yield client


@mock.patch('spark_word_count.webapp.get_word_stats')
def test_stats_endpoint(mock_get_stats, client):
    """Test the stats endpoint returns the expected data."""
    mock_get_stats.return_value = {
        'total_words': 1000,
        'unique_words': 100,
        'avg_frequency': 10.0,
        'median_frequency': 5.0,
    }
    
    response = client.get('/api/stats')
    
    assert response.status_code == 200
    data = json.loads(response.data)
    assert 'total_words' in data
    assert data['total_words'] == 1000
    assert data['unique_words'] == 100
    assert data['avg_frequency'] == 10.0
    assert data['median_frequency'] == 5.0


@mock.patch('spark_word_count.webapp.get_top_words')
def test_top_words_endpoint(mock_get_top_words, client):
    """Test the top_words endpoint returns the expected data."""
    mock_get_top_words.return_value = [
        ('the', 150),
        ('and', 100),
        ('is', 80),
    ]
    
    response = client.get('/api/top_words?limit=3')
    
    assert response.status_code == 200
    data = json.loads(response.data)
    assert len(data) == 3
    assert data[0][0] == 'the'
    assert data[0][1] == 150
    assert data[1][0] == 'and'
    assert data[2][0] == 'is'


@mock.patch('spark_word_count.webapp.search_word')
def test_search_endpoint(mock_search_word, client):
    """Test the search endpoint returns the expected data."""
    mock_search_word.return_value = [
        ('test', 50),
        ('testing', 30),
    ]
    
    response = client.get('/api/search?word=test')
    
    assert response.status_code == 200
    data = json.loads(response.data)
    assert len(data) == 2
    assert data[0][0] == 'test'
    assert data[0][1] == 50
    assert data[1][0] == 'testing'
    assert data[1][1] == 30


def test_home_route(client):
    """Test the home route returns the index template."""
    with mock.patch('spark_word_count.webapp.get_word_stats') as mock_stats:
        with mock.patch('spark_word_count.webapp.get_top_words') as mock_top_words:
            mock_stats.return_value = {
                'total_words': 1000,
                'unique_words': 100,
                'avg_frequency': 10.0,
                'median_frequency': 5.0,
            }
            mock_top_words.return_value = [('the', 150), ('and', 100)]
            
            response = client.get('/')
            
            assert response.status_code == 200
            assert b'Spark Word Count Dashboard' in response.data 