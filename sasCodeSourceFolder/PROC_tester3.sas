PROC SQL;
SELECT price, qty
FROM sales_raw
WHERE price > 100;
QUIT;