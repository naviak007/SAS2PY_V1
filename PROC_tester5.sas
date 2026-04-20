PROC MEANS DATA=sales;
    CLASS region;
    VAR price qty;
    OUTPUT OUT=result
        MEAN=avg_price avg_qty
        SUM=sum_price sum_qty;
RUN;