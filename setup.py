from setuptools import setup, find_packages

setup(
    name="spark_word_count",
    version="0.1.0",
    description="A PySpark application for counting word frequencies in text files",
    author="Spark Word Count Team",
    package_dir={"": "src"},
    packages=find_packages(where="src"),
    python_requires=">=3.6",
    install_requires=[
        "pyspark>=3.0.0",
        "psycopg2-binary>=2.8.0",
        "flask>=2.3.3",
        "matplotlib>=3.7.2",
        "pandas>=2.0.3",
    ],
    entry_points={
        "console_scripts": [
            "spark-word-count=spark_word_count.__main__:main",
        ],
    },
) 