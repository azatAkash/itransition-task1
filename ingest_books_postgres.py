#!/usr/bin/env python3
import json
import re
from pathlib import Path

import psycopg2
from psycopg2.extras import execute_values

# Path to the data file (same folder as this script)
DATA_FILE = Path("task1_d.json")

# >>>>> FILL THESE WITH YOUR REAL POSTGRES SETTINGS <<<<<
DB_CONFIG = {
    "host": "localhost",
    "port": 5432,
    "dbname": "books_db",      # your database name
    "user": "postgres",        # your username
    "password": "220021",    # your password
}


def ruby_like_to_json(text: str) -> str:
    """
    Convert Ruby-style array of hashes:
      [{:id=>1, :title=>"A"}, {...}]
    into valid JSON:
      [{"id":1, "title":"A"}, {...}]
    """
    # Replace :key=> with "key":
    return re.sub(r':(\w+)=>', r'"\1":', text)


def main():
    # ---- Read and fix the JSON-ish file ----
    print(f"Reading data from {DATA_FILE}...")
    raw = DATA_FILE.read_text(encoding="utf-8")
    json_text = ruby_like_to_json(raw)
    books = json.loads(json_text)
    print(f"Loaded {len(books)} records from file.")

    # ---- Connect to PostgreSQL ----
    print("Connecting to PostgreSQL...")
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()

    # ---- Create table if not exists ----
    print("Creating table books_raw if it does not exist...")
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS books_raw (
            id        BIGINT PRIMARY KEY,
            title     TEXT NOT NULL,
            author    TEXT,
            genre     TEXT,
            publisher TEXT,
            year      INTEGER,
            price     TEXT
        );
        """
    )
    conn.commit()

    # ---- Insert all rows ----
    print("Inserting data into books_raw...")
    rows = [
        (
            b.get("id"),
            b.get("title"),
            b.get("author"),
            b.get("genre"),
            b.get("publisher"),
            b.get("year"),
            b.get("price"),
        )
        for b in books
    ]

    execute_values(
        cur,
        """
        INSERT INTO books_raw
            (id, title, author, genre, publisher, year, price)
        VALUES %s
        ON CONFLICT (id) DO UPDATE
        SET
            title     = EXCLUDED.title,
            author    = EXCLUDED.author,
            genre     = EXCLUDED.genre,
            publisher = EXCLUDED.publisher,
            year      = EXCLUDED.year,
            price     = EXCLUDED.price;
        """,
        rows,
    )
    conn.commit()

    # ---- Show row count ----
    cur.execute("SELECT COUNT(*) FROM books_raw;")
    count = cur.fetchone()[0]
    print(f"Row count in books_raw: {count}")

    cur.close()
    conn.close()
    print("Done.")


if __name__ == "__main__":
    main()
