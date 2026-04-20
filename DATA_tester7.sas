DATA sales_logic;
    SET sales_raw;

    total = price * qty;

    IF total > 1000 AND qty >= 5 THEN priority = 1;
    ELSE priority = 0;

RUN;