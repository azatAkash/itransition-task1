DROP TABLE IF EXISTS book_summary;

CREATE TABLE book_summary AS
SELECT
    year AS publication_year,
    COUNT(*) AS book_count,
    ROUND(
        AVG(
            CASE
                WHEN LEFT(price, 1) = '€'
                    THEN CAST(SUBSTRING(price FROM 2) AS numeric) * 1.2  
                WHEN LEFT(price, 1) = '$'
                    THEN CAST(SUBSTRING(price FROM 2) AS numeric)        
                ELSE NULL                                                
            END
        ),
        2
    ) AS average_price
FROM books_raw
GROUP BY year
ORDER BY year;
