from setuptools import setup, find_packages

setup(
    name="spark_word_count",
    version="0.1.0",
    description="A PySpark application for counting word frequencies in text files",
    author="Spark Word Count Team",
    author_email="example@example.com",
    url="https://github.com/lin1shu/spark_word_count_by_cursor",
    package_dir={"": "src"},
    packages=find_packages(where="src"),
    python_requires=">=3.6",
    install_requires=[
        "pyspark>=3.0.0",
        "psycopg2-binary>=2.8.0",
        "flask>=2.3.3",
        "matplotlib>=3.7.2",
        "pandas>=2.0.3",
        "python-dotenv>=1.0.0",
    ],
    extras_require={
        "dev": [
            "pytest>=7.0.0",
            "pytest-cov>=4.0.0",
            "mypy>=1.0.0",
            "black>=23.0.0",
            "flake8>=6.0.0",
            "isort>=5.0.0",
        ],
    },
    entry_points={
        "console_scripts": [
            "spark-word-count=spark_word_count.__main__:main",
        ],
    },
    classifiers=[
        "Development Status :: 4 - Beta",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: MIT License",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.6",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Topic :: Text Processing",
    ],
) 