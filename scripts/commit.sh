#!/bin/bash
# Helper script for committing code with pre-commit hooks

set -e

# Create the directory if it doesn't exist
mkdir -p $(dirname "$0")

# Check if pre-commit is installed
if ! command -v pre-commit &> /dev/null; then
    echo "Installing pre-commit..."
    pip install pre-commit
fi

# Install pre-commit hooks if not already installed
if [ ! -f .git/hooks/pre-commit ]; then
    echo "Installing pre-commit hooks..."
    pre-commit install
fi

# Run pre-commit on all files
echo "Running pre-commit on all files..."
pre-commit run --all-files

# Run tests
echo "Running tests..."
pytest tests/unit

# If we get here, tests passed, ready to commit
echo "All checks passed! Ready to commit."
echo "Use 'git add' and 'git commit' to commit your changes." 