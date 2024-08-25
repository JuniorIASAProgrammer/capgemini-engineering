-- Task 1: Identify Authors with the Most Published Books
WITH cte_number_of_books_per_author AS (
	SELECT a.author_id, a.name, COUNT(*) AS n_books
	FROM books AS b
	JOIN authors AS a
		ON b.author_id=a.author_id
	GROUP BY a.author_id
)
SELECT name, n_books
FROM cte_number_of_books_per_author
WHERE n_books > 3;


-- Task 2: Identify Books with Titles Containing 'The' Using Regular Expressions
WITH cte_the_in_title AS (
	SELECT * FROM books
	WHERE title ~* '\mthe\M'
)
SELECT cte.title AS title, a.name AS author, g.genre_name AS genre, cte.published_date
FROM cte_the_in_title AS cte
LEFT JOIN authors AS a
	ON cte.author_id=a.author_id
LEFT JOIN genres AS g
	ON cte.genre_id=g.genre_id;
	
	
-- Task 3: Rank Books by Price within Each Genre Using the RANK() Window Function
SELECT 
	b.title,
	g.genre_name,
	b.price,
	RANK() OVER (PARTITION BY b.genre_id ORDER BY price) as rank
FROM books AS b
LEFT JOIN genres AS g
	ON b.genre_id=g.genre_id;
	

-- Task 4: Bulk Update Book Prices by Genre
CREATE OR REPLACE PROCEDURE sp_bulk_update_book_prices_by_genre(
	p_genre_id INTEGER,
	p_percentage_change NUMERIC(5, 2)
)
LANGUAGE plpgsql
AS $$
DECLARE 
	v_updated_count INTEGER;
BEGIN 
	UPDATE books
	SET price = price * (1 + p_percentage_change / 100)
	WHERE genre_id = p_genre_id;
	
	GET DIAGNOSTICS v_updated_count = ROW_COUNT;

    RAISE NOTICE 'Number of books updated: %', v_updated_count;
END
$$;

CALL sp_bulk_update_book_prices_by_genre(1, 20);


-- Task 5: Update Customer Join Date Based on First Purchase
CREATE OR REPLACE PROCEDURE sp_update_customer_join_date()
LANGUAGE plpgsql
AS $$
BEGIN
    UPDATE customers AS c
    SET join_date = subquery.first_purchase_date
    FROM (
        SELECT customer_id, MIN(sale_date) AS first_purchase_date
        FROM sales
        GROUP BY customer_id
    ) AS subquery
    WHERE c.customer_id = subquery.customer_id
    AND subquery.first_purchase_date < c.join_date;
 
END;
$$;

CALL sp_update_customer_join_date();

SELECT * FROM customers;


-- Task 6: Calculate Average Book Price by Genre
CREATE OR REPLACE FUNCTION fn_avg_price_by_genre(
	p_genre_id INTEGER
)
RETURNS NUMERIC(10, 2)
LANGUAGE plpgsql
AS $$
DECLARE
    v_avg_price NUMERIC(10, 2);
BEGIN 
	SELECT AVG(price)
    INTO v_avg_price
    FROM books
    WHERE genre_id = p_genre_id;
	
	RETURN COALESCE(v_avg_price, 0.00);
END
$$;

SELECT fn_avg_price_by_genre(1);

-- Task 7: Get Top N Best-Selling Books by Genre
CREATE OR REPLACE FUNCTION fn_get_top_n_books_by_genre(
	p_genre_id INTEGER,
	p_top_n INTEGER
)
RETURNS TABLE (
    book_id INTEGER,
    title TEXT,
    total_sales_revenue NUMERIC(10, 2)
)
LANGUAGE plpgsql
AS $$
BEGIN
	RETURN QUERY
	SELECT
        b.book_id,
        b.title::TEXT,
        SUM(s.quantity * b.price) AS total_sales_revenue
    FROM books AS b
    JOIN sales AS s 
		ON b.book_id = s.book_id
    WHERE b.genre_id = p_genre_id
	GROUP BY b.book_id, b.title
	ORDER BY total_sales_revenue DESC
	LIMIT p_top_n;
END;
$$;

SELECT * FROM fn_get_top_n_books_by_genre(1, 5);


-- Task 8: Log Changes to Sensitive Data
CREATE TABLE IF NOT EXISTS CustomersLog (
	log_id SERIAL PRIMARY KEY,
	column_name VARCHAR(50),
	old_value TEXT,
	new_value TEXT,
	changed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
	changed_by VARCHAR(50) -- This assumes you can track the user making the change
);

CREATE FUNCTION log_sensitive_data_changes()
	RETURNS TRIGGER 
	LANGUAGE plpgsql
	AS $$
	BEGIN
		IF NEW.first_name <> OLD.first_name THEN
			INSERT INTO CustomersLog (column_name, old_value, new_value, changed_by)
			VALUES ('first_name', OLD.first_name, NEW.first_name, CURRENT_USER);
    	END IF;

		IF NEW.last_name <> OLD.last_name THEN
			INSERT INTO CustomersLog (column_name, old_value, new_value, changed_by)
			VALUES ('last_name', OLD.last_name, NEW.last_name, CURRENT_USER);
		END IF;

		IF NEW.email <> OLD.email THEN
			INSERT INTO CustomersLog (column_name, old_value, new_value, changed_by)
			VALUES ('email', OLD.email, NEW.email, CURRENT_USER);
		END IF;	
		
		RETURN NEW;
	END
	$$;
	
CREATE TRIGGER tr_log_sensitive_data_changes
	AFTER UPDATE ON customers
	FOR EACH ROW 
	EXECUTE PROCEDURE log_sensitive_data_changes();
	
UPDATE customers
SET email = 'alice.williams1@example.com'
WHERE customer_id = 1;

SELECT * FROM CustomersLog;


-- Task 9: Automatically Adjust Book Prices Based on Sales Volume
CREATE OR REPLACE FUNCTION adjust_book_price()
	RETURNS TRIGGER 
	LANGUAGE plpgsql
	AS $$
	DECLARE
    	total_quantity_sold INTEGER;
	BEGIN
		SELECT SUM(quantity)
		INTO total_quantity_sold
		FROM sales
		WHERE book_id = OLD.book_id;
		
		IF total_quantity_sold >= 10 THEN
			UPDATE books
			SET price = price * 1.1
			WHERE book_id = OLD.book_id;
		END IF;
		RETURN NEW;
	END
	$$;

CREATE TRIGGER tr_adjust_book_price_10
	AFTER INSERT ON sales
	FOR EACH ROW 
	EXECUTE PROCEDURE adjust_book_price();
	
SELECT * FROM sales;
SELECT * FROM books;


-- Task 10: Archive Old Sales Records
CREATE TABLE IF NOT EXISTS SalesArchive AS
TABLE sales;

CREATE OR REPLACE PROCEDURE sp_archive_old_sales(p_cutoff_date DATE)
LANGUAGE plpgsql
AS $$
DECLARE
    cur_sales CURSOR FOR
        SELECT * FROM Sales WHERE sale_date < p_cutoff_date;
    rec_sales sales%ROWTYPE;
BEGIN
    OPEN cur_sales;

    LOOP
        FETCH cur_sales INTO rec_sales;
        EXIT WHEN NOT FOUND;

        INSERT INTO SalesArchive
        VALUES (rec_sales.*);

        DELETE FROM Sales WHERE CURRENT OF cur_sales;
    END LOOP;

    CLOSE cur_sales;
END;
$$;

CALL sp_archive_old_sales('2023-01-01');

SELECT * FROM SalesArchive;