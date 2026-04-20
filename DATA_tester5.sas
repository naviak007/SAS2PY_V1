DATA sales_category;
    SET sales_raw;

    total = price * qty;

    IF total > 1000 THEN category = 'High';
    ELSE category = 'Normal';

RUN;