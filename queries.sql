DROP TABLE IF EXISTS book_summary;

CREATE TABLE book_summary AS
SELECT
    year AS publication_year,
    COUNT(*) AS book_count,
    ROUND(
        AVG(
            CASE
                WHEN LEFT(price, 1) = '€'
                    THEN CAST(SUBSTRING(price FROM 2) AS NUMERIC) * 1.2
                WHEN LEFT(price, 1) = '$'
                    THEN CAST(SUBSTRING(price FROM 2) AS NUMERIC)
                ELSE NULL
            END
        ),
        2
    ) AS average_price
FROM books_raw
GROUP BY year
ORDER BY year;



SELECT 'books_raw' AS table_name, COUNT(*) AS row_count FROM books_raw
UNION ALL
SELECT 'book_summary', COUNT(*) FROM book_summary;


SELECT
    publication_year,
    book_count,
    average_price
FROM book_summary
ORDER BY publication_year;
