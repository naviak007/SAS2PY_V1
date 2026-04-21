DATA sales_flags;
    SET sales_raw;

    IF price > 1000 THEN expensive = 1;
    IF qty >= 10 THEN bulk = 1;

RUN;