PROC MEANS DATA=sales MEAN SUM MAX;
CLASS region;
VAR price qty;
RUN;