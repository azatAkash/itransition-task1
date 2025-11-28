#!/bin/bash

set -e
pip install psycopg2-binary

python main.py

PGPASSWORD=220021 psql -U postgres -d books_db -f queries.sql



