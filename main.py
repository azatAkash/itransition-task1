from pathlib import Path
import psycopg2

# Python (main.py + db_utils.py) → data ingestion
# SQL (queries.sql) → data transformation + analytics

from db_utils import (
    load_books_from_file,
    recreate_books_table,
    insert_books,
)


DB_CONFIG = {
    "host": "localhost",
    "port": 5432,
    "dbname": "books_db",
    "user": "postgres",
    "password": "220021",
}


def get_connection():
    return psycopg2.connect(**DB_CONFIG)


def main():
    books = load_books_from_file(Path("task1_d.json"))

    conn = get_connection()
    cur = conn.cursor()

    try:
        recreate_books_table(cur)
        insert_books(cur, books)
        conn.commit()

    finally:
        cur.close()
        conn.close()


if __name__ == "__main__":
    main()
