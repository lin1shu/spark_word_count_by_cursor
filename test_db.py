#!/usr/bin/env python3
import psycopg2

print("Connecting to PostgreSQL...")
try:
    conn = psycopg2.connect(
        host="localhost",
        dbname="wordcount",
        user="postgres",
        password="sparkdemo",
        port="5432",
    )
    print("Connection successful!")
    
    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(*) FROM word_counts")
    result = cursor.fetchone()
    print(f"Word count: {result[0]}")
    
    cursor.execute("SELECT word, count FROM word_counts ORDER BY count DESC LIMIT 5")
    results = cursor.fetchall()
    print("Top 5 words:")
    for word, count in results:
        print(f"  {word}: {count}")
    
    cursor.close()
    conn.close()
except Exception as e:
    print(f"Error: {e}") 