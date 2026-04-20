DATA sales_calc;
    SET sales_raw;

    total = price * qty;
    avg_price = total / qty;

RUN;