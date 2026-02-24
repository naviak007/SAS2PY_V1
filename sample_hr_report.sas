/* ============================================================
   Sample SAS Program for Accelerator Testing
   Covers DATA, PROC SORT, PROC MEANS, PROC SQL
   ============================================================ */

/* Macro variables */
%LET year_filter = 2024;
%LET min_salary  = 50000;

/* DATA STEP */
DATA work.filtered_employees;
    SET work.employees_raw;

    WHERE year = &year_filter AND salary >= &min_salary;

    KEEP emp_id first_name last_name department salary bonus;
    DROP temp_flag;

    RENAME first_name = fname
           last_name  = lname;

    total_comp = salary + bonus;

    IF department = 'IT' THEN high_value = 'Y';
RUN;


/* PROC SORT */
PROC SORT DATA=work.filtered_employees 
          OUT=work.sorted_employees
          NODUPKEY;
    BY DESCENDING salary department;
RUN;


/* PROC MEANS */
PROC MEANS DATA=work.sorted_employees NOPRINT;
    CLASS department;
    VAR salary bonus;

    OUTPUT OUT=work.department_summary
        MEAN = avg_salary avg_bonus
        MAX  = max_salary max_bonus
        MIN  = min_salary min_bonus;
RUN;


/* PROC SQL */
PROC SQL;
    CREATE TABLE work.top_departments AS
    SELECT department,
           COUNT(emp_id) AS employee_count,
           AVG(salary)   AS avg_salary
    FROM work.sorted_employees
    GROUP BY department
    HAVING AVG(salary) > 60000;
QUIT;