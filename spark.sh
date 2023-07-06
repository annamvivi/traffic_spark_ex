#!/bin/bash

# Install Dependencies
pip install pyspark
pip install sqlparse

# connect postgresql
python3 /mnt/c/linux/lalin_test/traffic_ex/postgres.py

# Extract Data from PostgreSQL with Spark
python3 /mnt/c/linux/lalin_test/traffic_ex/spark4.py