/*==============================================================*/
/* TEST SAS PROGRAM - Covers most SAS syntax for parser testing */
/*==============================================================*/

/*---------------------------*/
/* 1. GLOBAL OPTIONS         */
/*---------------------------*/
options nodate nonumber;
title "Comprehensive SAS Syntax Test";

/*---------------------------*/
/* 2. MACRO VARIABLES        */
/*---------------------------*/
%let threshold = 100;
%let today = %sysfunc(today(), date9.);

/*---------------------------*/
/* 3. LIBNAME STATEMENT      */
/*---------------------------*/
libname mylib "C:\data";

/*---------------------------*/
/* 4. DATA STEP WITH INPUT   */
/*---------------------------*/
data mylib.sales;
    infile datalines dlm=',' dsd;
    input region $ product $ amount quantity;
    total = amount * quantity;

    /* conditional logic */
    if total > &threshold then status = "HIGH";
    else status = "LOW";

    /* missing value handling */
    if amount = . then amount = 0;

    datalines;
North,A,100,2
South,B,200,3
East,C,150,1
West,A,.,5
;
run;

/*---------------------------*/
/* 5. ARRAY PROCESSING       */
/*---------------------------*/
data mylib.array_test;
    array nums[3] n1-n3 (10 20 30);

    do i = 1 to 3;
        nums[i] = nums[i] * 2;
    end;
run;

/*---------------------------*/
/* 6. RETAIN AND LAG         */
/*---------------------------*/
data mylib.lag_test;
    set mylib.sales;
    retain running_total 0;

    running_total + total;
    previous_total = lag(total);
run;

/*---------------------------*/
/* 7. MERGE DATASETS         */
/*---------------------------*/
data mylib.merge_test;
    merge mylib.sales mylib.lag_test;
    by region;
run;

/*---------------------------*/
/* 8. PROC SORT              */
/*---------------------------*/
proc sort data=mylib.sales out=mylib.sales_sorted;
    by region descending total;
run;

/*---------------------------*/
/* 9. PROC MEANS             */
/*---------------------------*/
proc means data=mylib.sales n mean sum max min;
    var total quantity;
run;

/*---------------------------*/
/* 10. PROC FREQ             */
/*---------------------------*/
proc freq data=mylib.sales;
    tables region*product / nocol nopercent;
run;

/*---------------------------*/
/* 11. PROC PRINT            */
/*---------------------------*/
proc print data=mylib.sales(obs=5);
run;

/*---------------------------*/
/* 12. PROC SQL              */
/*---------------------------*/
proc sql;
    create table mylib.sales_summary as
    select 
        region,
        product,
        sum(total) as total_sales,
        avg(total) as avg_sales
    from mylib.sales
    group by region, product
    having calculated total_sales > 100;
quit;

/*---------------------------*/
/* 13. SQL JOIN              */
/*---------------------------*/
proc sql;
    create table mylib.join_test as
    select a.region,
           a.product,
           a.total,
           b.running_total
    from mylib.sales as a
    left join mylib.lag_test as b
    on a.region = b.region;
quit;

/*---------------------------*/
/* 14. SQL SUBQUERY          */
/*---------------------------*/
proc sql;
    select *
    from mylib.sales
    where total > (
        select avg(total)
        from mylib.sales
    );
quit;

/*---------------------------*/
/* 15. MACRO DEFINITION      */
/*---------------------------*/
%macro report(dataset);
    proc means data=&dataset;
        var total quantity;
    run;
%mend;

/* Macro call */
%report(mylib.sales);

/*---------------------------*/
/* 16. CONDITIONAL MACRO     */
/*---------------------------*/
%macro check(val);
    %if &val > 50 %then %do;
        %put Value greater than 50;
    %end;
    %else %do;
        %put Value less than or equal to 50;
    %end;
%mend;

%check(75);

/*---------------------------*/
/* 17. FORMAT CREATION       */
/*---------------------------*/
proc format;
    value $regionfmt
        "North" = "Northern Zone"
        "South" = "Southern Zone"
        other   = "Other";
run;

/*---------------------------*/
/* 18. APPLY FORMAT          */
/*---------------------------*/
data mylib.formatted_sales;
    set mylib.sales;
    format region $regionfmt.;
run;

/*---------------------------*/
/* 19. EXPORT DATA           */
/*---------------------------*/
proc export data=mylib.sales
    outfile="C:\data\sales.csv"
    dbms=csv
    replace;
run;

/*---------------------------*/
/* 20. END OF PROGRAM        */
/*---------------------------*/
title;