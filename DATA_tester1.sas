DATA sales_enriched;
    SET sales_raw;

    total_amount = price * qty;
    discount_amount = total_amount * 0.1;

    IF total_amount > 1000 THEN category = 'High';
    ELSE category = 'Normal';

    IF qty >= 10 THEN bulk_order = 1;
RUN;