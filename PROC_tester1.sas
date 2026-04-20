PROC SORT DATA=sales_raw OUT=sales_sorted;
    BY price DESCENDING qty;
RUN;