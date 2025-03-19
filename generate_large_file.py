#!/usr/bin/env python3
"""
Script to generate a large text file for testing Spark word count processing.
"""

import os
import random
import string
from pathlib import Path

# Define base paragraphs - similar to our sample texts
BASE_PARAGRAPHS = [
    "Machine learning is a subset of artificial intelligence that involves the development of algorithms and statistical models that enable computers to perform specific tasks without explicit instructions, relying instead on patterns and inference.",
    "The field of machine learning explores the study and construction of algorithms that can learn from and make predictions on data. Such algorithms operate by building a model from example inputs in order to make data-driven predictions or decisions, rather than following strictly static program instructions.",
    "Machine learning algorithms are used in a wide variety of applications, such as email filtering, detection of network intruders, optical character recognition, search engines, recommendation systems, and computer vision, where it is difficult or infeasible to develop conventional algorithms to perform the needed tasks.",
    "Data science is an interdisciplinary field that uses scientific methods, processes, algorithms, and systems to extract knowledge and insights from structured and unstructured data. Data science combines expertise from the fields of statistics, computer science, and domain knowledge to analyze large amounts of data.",
    "The term 'big data' often refers to data sets that are so large or complex that traditional data processing applications are inadequate to deal with them. Challenges include analysis, capture, data curation, search, sharing, storage, transfer, visualization, querying, updating, and information privacy.",
    "Apache Spark is an open-source, distributed computing system that provides an interface for programming entire clusters with implicit data parallelism and fault tolerance. Spark was developed at UC Berkeley in 2009 and later donated to the Apache Software Foundation, which has maintained it since.",
    "Python is a high-level, general-purpose programming language. Its design philosophy emphasizes code readability with its use of significant indentation. Python is dynamically typed and garbage-collected. It supports multiple programming paradigms, including structured, object-oriented, and functional programming.",
    "Database systems store and organize data to enable efficient retrieval, manipulation, and management. PostgreSQL is a powerful, open source object-relational database system with over 30 years of active development that has earned it a strong reputation for reliability, feature robustness, and performance.",
    "The integration of various technologies like Spark, Python, and PostgreSQL allows for powerful data processing pipelines that can handle large volumes of data efficiently. These technologies form the backbone of many modern data-driven applications and platforms across various industries.",
    "Artificial intelligence aims to create systems capable of performing tasks that would normally require human intelligence. These include visual perception, speech recognition, decision-making, and translation between languages.",
    "Deep learning is a subset of machine learning that uses neural networks with many layers (hence 'deep') to analyze various factors of data. Deep learning is making major advances in solving problems that have resisted the best attempts of the artificial intelligence community for many years.",
    "Natural language processing combines computational linguistics, machine learning, and deep learning models to enable computers to process human language in the form of text or voice data and to 'understand' its full meaning.",
    "Computer vision is a field of artificial intelligence that trains computers to interpret and understand the visual world. Using digital images from cameras and videos and deep learning models, machines can accurately identify and classify objects.",
    "Reinforcement learning is an area of machine learning concerned with how software agents ought to take actions in an environment in order to maximize some notion of cumulative reward.",
]

# Words to randomly substitute in paragraphs to create variations
TECH_WORDS = [
    "data", "algorithm", "learning", "model", "system", "processing", "artificial", 
    "intelligence", "neural", "network", "training", "prediction", "classification", 
    "regression", "clustering", "analytics", "big data", "cloud", "computing", 
    "programming", "software", "hardware", "database", "storage", "retrieval", 
    "query", "distributed", "parallel", "scalable", "efficient", "performance", 
    "optimization", "visualization", "analysis", "integration", "architecture", 
    "framework", "library", "API", "interface", "development", "deployment", 
    "production", "testing", "validation", "accuracy", "precision", "recall", 
    "feature", "parameter", "hyperparameter", "training set", "test set", 
    "validation set", "overfitting", "underfitting", "bias", "variance"
]

def generate_variant(paragraph):
    """Generate a variant of a paragraph by replacing some words."""
    words = paragraph.split()
    # Replace 10-20% of words with random tech words
    num_to_replace = random.randint(max(1, len(words) // 10), max(2, len(words) // 5))
    
    for _ in range(num_to_replace):
        idx = random.randint(0, len(words) - 1)
        # Don't replace very short words, punctuation, or the first/last word
        if len(words[idx]) > 3 and idx > 0 and idx < len(words) - 1:
            words[idx] = random.choice(TECH_WORDS)
    
    return ' '.join(words)

def add_sentences(paragraph):
    """Add 1-3 random sentences to a paragraph."""
    sentence_templates = [
        "This demonstrates the importance of {} in modern {} systems.",
        "Researchers continue to explore new approaches to improve {} and {} capabilities.",
        "The combination of {} and {} leads to significant advancements in the field.",
        "Organizations are increasingly adopting {} to enhance their {} processes.",
        "Future developments in {} will likely revolutionize how we approach {}.",
        "Effective implementation of {} requires careful consideration of {} factors."
    ]
    
    extra = ""
    num_sentences = random.randint(1, 3)
    
    for _ in range(num_sentences):
        template = random.choice(sentence_templates)
        word1 = random.choice(TECH_WORDS)
        word2 = random.choice(TECH_WORDS)
        extra += " " + template.format(word1, word2)
    
    return paragraph + extra

def generate_large_file(output_path, target_size_gb=2):
    """Generate a large text file (~2GB) by repeating and varying paragraphs."""
    target_size_bytes = target_size_gb * 1024 * 1024 * 1024
    output_file = Path(output_path)
    
    # Create directory if it doesn't exist
    output_file.parent.mkdir(parents=True, exist_ok=True)
    
    # Track current size
    current_size = 0
    
    # Counter for progress reporting
    counter = 0
    report_every = 100000  # Report progress every 100,000 paragraphs
    
    print(f"Generating ~{target_size_gb}GB text file at {output_path}...")
    
    with open(output_file, 'w') as f:
        while current_size < target_size_bytes:
            # Generate a variant of a random base paragraph
            base = random.choice(BASE_PARAGRAPHS)
            variant = generate_variant(base)
            
            # Sometimes add extra sentences
            if random.random() < 0.7:  # 70% chance
                variant = add_sentences(variant)
            
            # Add paragraph with newline
            paragraph = variant + "\n\n"
            f.write(paragraph)
            
            # Update size
            current_size += len(paragraph.encode('utf-8'))
            
            # Report progress
            counter += 1
            if counter % report_every == 0:
                progress = (current_size / target_size_bytes) * 100
                print(f"Progress: {progress:.2f}% - Generated {counter:,} paragraphs ({current_size/(1024*1024*1024):.2f}GB)")
    
    # Get final file size
    actual_size = os.path.getsize(output_file)
    print(f"Done! Generated file size: {actual_size/(1024*1024*1024):.2f}GB with {counter:,} paragraphs")

if __name__ == "__main__":
    output_path = "data/large_sample.txt"
    generate_large_file(output_path, 2)  # Generate 2GB file 