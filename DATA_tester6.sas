DATA sales_math;
    SET sales_raw;

    total = price * qty;
    tax = total * 0.05;
    final_price = total + tax;

RUN;