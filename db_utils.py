import json
import re
from pathlib import Path

from psycopg2.extras import execute_values


def ruby_like_to_json(text: str) -> str:
    return re.sub(r':(\w+)=>', r'"\1":', text)


def load_books_from_file(path: Path) -> list[dict]:
    print(f"\nReading data from {path}...")
    raw = path.read_text(encoding="utf-8")
    json_text = ruby_like_to_json(raw)
    books = json.loads(json_text)
    print(f"Loaded {len(books)} records from file")
    return books


def recreate_books_table(cur):
    cur.execute("DROP TABLE IF EXISTS books_raw;")

    cur.execute(
        """
        CREATE TABLE books_raw (
            id        NUMERIC PRIMARY KEY,
            title     TEXT NOT NULL,
            author    TEXT,
            genre     TEXT,
            publisher TEXT,
            year      INTEGER,
            price     TEXT
        );
        """
    )


def insert_books(cur, books: list[dict]):
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
        VALUES %s;
        """,
        rows,
    )


