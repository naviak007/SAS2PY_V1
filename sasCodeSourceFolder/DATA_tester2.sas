DATA sales_clean;
    SET sales_raw;              * reading source dataset;
    total_amount = price * qty; /* calculating total */
    IF total_amount > 1000 THEN 
        category = 'High';
    ELSE 
        category = 'Normal';
RUN;


