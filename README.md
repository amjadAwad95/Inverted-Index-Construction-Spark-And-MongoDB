# Spark-based Search Engine

A Spark and MongoDB implementation of an inverted index for document retrieval, supporting query processing with intersection of results.

## Features

- **Inverted Index Construction**: Builds an alphabetically sorted inverted index from a folder of documents.
- **MongoDB Integration**: Saves the inverted index into a MongoDB collection.
- **Query Processing**: Retrieves relevant documents for user queries by intersecting results from MongoDB.

## Requirements

- Apache Spark 3.5.3
- MongoDB 2.3.2
- Scala 2.12
- MongoDB Spark Connector
- MongoDB Scala Driver
