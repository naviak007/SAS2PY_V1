/* Sample Comprehensive SAS Code for Testing */

options nodate nonumber;
libname mylib '/data/sas';

/* Global macro variables */
%let run_date = '01JAN2024'd;
%let threshold = 1000;

/* Custom format */
proc format;
value agegrp
low - 18 = 'Minor'
19 - 60 = 'Adult'
61 - high = 'Senior';
run;

/* Import CSV */
proc import datafile='/data/input/sales.csv'
out=mylib.sales
dbms=csv
replace;
guessingrows=1000;
run;

/* Data step with transformations */
data mylib.sales_clean;
set mylib.sales;

```
/* Line 1: Basic calculations */
revenue = price * quantity;

/* Line 2: Conditional logic */
if revenue > &threshold then category = 'High';
else category = 'Low';

/* Line 3: Date handling */
year = year(sale_date);
month = month(sale_date);

/* Line 4: Format assignment */
format age agegrp.;

/* Line 5: Drop unnecessary columns */
drop temp_col;
```

run;

/* Sorting */
proc sort data=mylib.sales_clean out=mylib.sales_sorted;
by customer_id descending revenue;
run;

/* Another dataset */
data mylib.customers;
infile '/data/input/customers.csv' dlm=',' firstobs=2;
input customer_id name $ age city $;
run;

/* Merge datasets */
data mylib.final_data;
merge mylib.sales_sorted(in=a)
mylib.customers(in=b);
by customer_id;

```
if a and b;

/* Derived column */
if age < 18 then segment = 'Youth';
else if age < 60 then segment = 'Working';
else segment = 'Senior';
```

run;

/* PROC SQL block */
proc sql;
create table mylib.summary as
select
city,
count(*) as total_customers,
sum(revenue) as total_revenue,
avg(revenue) as avg_revenue
from mylib.final_data
group by city
having calculated total_revenue > 5000;
quit;

/* Aggregation using PROC MEANS */
proc means data=mylib.final_data noprint;
class city;
var revenue;
output out=mylib.stats
mean=avg_rev
sum=total_rev
max=max_rev;
run;

/* Using arrays */
data mylib.array_example;
set mylib.sales_clean;

```
array nums[3] n1-n3;
do i = 1 to 3;
    nums[i] = revenue * i;
end;

drop i;
```

run;

/* Macro definition */
%macro process_data(input_ds, output_ds);

```
data &output_ds;
    set &input_ds;

    /* Macro transformation */
    adjusted_revenue = revenue * 1.1;

    if adjusted_revenue > 2000 then flag = 1;
    else flag = 0;

run;
```

%mend;

/* Macro execution */
%process_data(mylib.sales_clean, mylib.sales_enhanced);

/* Another PROC SQL with join */
proc sql;
create table mylib.joined as
select a.customer_id,
a.revenue,
b.city
from mylib.sales_clean a
left join mylib.customers b
on a.customer_id = b.customer_id;
quit;

/* Export dataset */
proc export data=mylib.joined
outfile='/data/output/joined.csv'
dbms=csv
replace;
run;

/* Final print */
proc print data=mylib.summary;
run;
