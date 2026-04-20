PROC SQL;
SELECT a.id, b.name
FROM sales a
JOIN customers b
ON a.id = b.id
WHERE a.id > 10;
QUIT;