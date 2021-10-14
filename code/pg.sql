CREATE TABLE films (
    code        char(5),
    title       varchar(40),
    did         integer,
    date_prod   date,
    kind        varchar(10),
    len         interval hour to minute,
    CONSTRAINT code_title PRIMARY KEY(code,title)
);



CREATE TABLE total_sales_tbl AS 
SELECT c.country,
    c.city,
    sum(od.unit_price * od.quantity::double precision) AS sum
   FROM orders o
     JOIN order_details od ON o.order_id = od.order_id
     JOIN customers c ON o.customer_id = c.customer_id
  GROUP BY c.country, c.city;

  
CREATE TABLE total_sales_tbl AS 
SELECT c.country,
    c.city,
    sum(od.unit_price * od.quantity::double precision) AS sum
   FROM orders o
     JOIN order_details od ON o.order_id = od.order_id
     JOIN customers c ON o.customer_id = c.customer_id
  GROUP BY c.country, c.city
  WITH NO DATA;
  
  
CREATE TEMP TABLE total_sales_tbl AS 
SELECT c.country,
    c.city,
    sum(od.unit_price * od.quantity::double precision) AS sum
   FROM orders o
     JOIN order_details od ON o.order_id = od.order_id
     JOIN customers c ON o.customer_id = c.customer_id
  GROUP BY c.country, c.city;

  
CREATE OR REPLACE VIEW public.v_total_sales
AS SELECT c.country,
    c.city,
    sum(od.unit_price * od.quantity) AS sales
   FROM orders o
     JOIN order_details od ON o.order_id = od.order_id
     JOIN customers c ON o.customer_id = c.customer_id
  GROUP BY c.country, c.city;
  
  SELECT country, city, sales FROM v_total_sales vts
  
  
CREATE materialized VIEW public.vm_total_sales
AS SELECT c.country,
    c.city,
    sum(od.unit_price * od.quantity) AS sales
   FROM orders o
     JOIN order_details od ON o.order_id = od.order_id
     JOIN customers c ON o.customer_id = c.customer_id
  GROUP BY c.country, c.city;
  
  
  REFRESH MATERIALIZED VIEW vm_total_sales;
  
  
  TRUNCATE TABLE total_sales_tbl;
  
  DROP TABLE total_sales_tbl;
  
  
SELECT
    c.company_name,
    c.country
FROM
    customers c
JOIN orders o ON c.customer_id = o.customer_id
JOIN order_details od ON o.order_id = od.order_id
WHERE EXTRACT(YEAR FROM o.order_date) = 1996 
GROUP BY 1,2

EXCEPT

SELECT
    c.company_name,
    c.country
FROM
    customers c
JOIN orders o ON c.customer_id = o.customer_id
JOIN order_details od ON o.order_id = od.order_id
WHERE EXTRACT(YEAR FROM o.order_date) = 1997
GROUP BY 1,2

SELECT
    employee_id,
    last_name,
    first_name,
    hire_date
FROM
    employees e

SELECT max(hire_date) FROM employees;
  
-- 1994-11-15  

SELECT
    employee_id,
    last_name,
    first_name,
    hire_date
FROM
    employees e
WHERE hire_date = '1994-11-15'

SELECT
    employee_id,
    last_name,
    first_name,
    hire_date
FROM
    employees e
WHERE hire_date = (SELECT max(hire_date) FROM employees)


SELECT
    customer_id,
    company_name,
    country,
    address
FROM
    customers c
WHERE
    country IN (
        'USA', 'UK'
    )

    
--
SELECT ship_country, ship_city, count(DISTINCT order_id)
FROM orders o
WHERE customer_id IN (
    SELECT customer_id
    FROM customers c
    WHERE country IN ('USA', 'UK')
)
GROUP BY ship_country, ship_city


SELECT * FROM orders o 
WHERE (ship_country, ship_city) IN (
SELECT country, city -- можно не указывать DISTINCT. Дубли игнорируются
    FROM customers c
    WHERE country IN ('USA', 'UK')
)


SELECT customer_id, company_name 
FROM customers c
WHERE EXISTS(
    SELECT 1 FROM orders o
    WHERE o.customer_id = c.customer_id
    AND o.order_date >='1998-04-01')


SELECT 
    YEAR,
    MONTH,
    sales
FROM (
    SELECT 
        EXTRACT(YEAR FROM o.order_date) AS year,
        EXTRACT(MONTH FROM o.order_date) AS month,
        SUM(od.unit_price * od.quantity) AS sales
    FROM order_details od
    JOIN orders o ON o.order_id = od.order_id
    GROUP BY 1, 2
) as sales_data
WHERE YEAR = 1998



WITH sales_data AS (
SELECT 
    EXTRACT(YEAR FROM o.order_date) AS year,
    EXTRACT(MONTH FROM o.order_date) AS month,
    SUM(od.unit_price * od.quantity) AS sales
FROM order_details od
JOIN orders o ON o.order_id = od.order_id
GROUP BY 1, 2)
SELECT 
    YEAR,
    MONTH,
    sales
FROM sales_data
WHERE YEAR = 1998


SELECT
    EXTRACT(YEAR FROM o.order_date) AS year,
    EXTRACT(MONTH FROM o.order_date) AS month,
    EXTRACT(QUARTER FROM o.order_date) AS quarter,
    SUM(od.unit_price * od.quantity) AS sales,
    max(SUM(od.unit_price * od.quantity)) OVER () AS max_sales,
    max(SUM(od.unit_price * od.quantity)) OVER ( PARTITION BY EXTRACT(QUARTER FROM o.order_date) ) AS max_sales_quarter
FROM order_details od
JOIN orders o ON o.order_id = od.order_id
WHERE EXTRACT(YEAR FROM o.order_date) = 1997
GROUP BY 1, 2, 3

WITH grouped AS (
SELECT
    EXTRACT(YEAR FROM o.order_date) AS year,
    EXTRACT(MONTH FROM o.order_date) AS month,
    EXTRACT(QUARTER FROM o.order_date) AS quarter,
    SUM(od.unit_price * od.quantity) AS sales
FROM order_details od
JOIN orders o ON o.order_id = od.order_id
WHERE EXTRACT(YEAR FROM o.order_date) = 1997
GROUP BY 1, 2, 3)
SELECT 
    YEAR,
    MONTH,
    quarter,
    sales,
    max(sales) OVER () AS max_sales,
    max(sales) OVER ( PARTITION BY quarter) AS max_sales_quarter
FROM 
grouped


/* Рассчитаем ранги продаж. Для этого нам нужно будет указать порядок */
SELECT
    EXTRACT( QUARTER FROM o.order_date),
    EXTRACT( MONTH FROM o.order_date),
    SUM(od.unit_price*od.quantity) AS sales,
    max(SUM(od.unit_price*od.quantity)) OVER () AS max_sales,
    max(SUM(od.unit_price*od.quantity)) OVER (PARTITION BY EXTRACT( quarter FROM o.order_date) ) AS max_qtr_sales,
    rank() OVER (ORDER BY SUM(od.unit_price*od.quantity) DESC) AS sales_rank,
    rank() OVER (PARTITION BY EXTRACT( quarter FROM o.order_date) ORDER BY SUM(od.unit_price*od.quantity) DESC) AS sales_rank_qtr
FROM
    orders o
JOIN order_details od ON
    o.order_id = od.order_id
WHERE
    EXTRACT(YEAR FROM o.order_date)= 1996
GROUP BY 1,2
ORDER BY EXTRACT( MONTH FROM o.order_date)


SELECT 
    EXTRACT(MONTH FROM o.order_date) AS month,
    SUM(od.unit_price * od.quantity),
    SUM(CASE WHEN extract(YEAR FROM o.order_date) = 1997 then od.unit_price * od.quantity END) AS sales97,
    SUM(od.unit_price * od.quantity) FILTER(WHERE extract(YEAR FROM o.order_date) = 1997) AS sales_97
FROM orders o
JOIN order_details od ON o.order_id = od.order_id
GROUP BY 1
ORDER BY 1


WITH sales_data AS (
SELECT UNNEST AS sales FROM unnest(ARRAY[5, 10,20,30,25,10])
)
SELECT 
    sales,
    ROW_NUMBER() OVER(ORDER BY sales desc) AS rn_sales_desc,
    RANK() OVER(ORDER BY sales desc) AS rnk_sales_desc,
    DENSE_RANK() OVER(ORDER BY sales desc) AS dense_rnk_sales_desc
FROM sales_data

WITH sales_data AS (
SELECT UNNEST AS sales FROM unnest(ARRAY[5, 10,20,30,25,10])
)
SELECT 
    sales,
    ROW_NUMBER() OVER(ORDER BY sales ) AS rn_sales_desc,
    RANK() OVER(ORDER BY sales ) AS rnk_sales_desc,
    DENSE_RANK() OVER(ORDER BY sales ) AS dense_rnk_sales_desc
FROM sales_data




WITH sales_data AS (
    SELECT 
        ship_country, 
        ship_city, 
        sum(od.quantity * od.unit_price) AS sales
    FROM orders o
    JOIN order_details od ON o.order_id = od.order_id
    GROUP BY 1,2),
sales_ranks AS (
SELECT 
    ship_country, 
    ship_city,
    sales,
    RANK() OVER(ORDER BY sales desc) AS rnk_sales_total,
    RANK() OVER(PARTITION BY ship_country ORDER BY sales DESC) AS rnk_sales_by_country
FROM sales_data)
SELECT * 
FROM sales_ranks
WHERE rnk_sales_by_country = 1

CREATE TABLE total_sales_tbl AS 
SELECT c.country,
    c.city,
    sum(od.unit_price * od.quantity::double precision) AS sales
   FROM orders o
     JOIN order_details od ON o.order_id = od.order_id
     JOIN customers c ON o.customer_id = c.customer_id
  GROUP BY c.country, c.city;


SELECT  
    country,
    city,
    sales,
    sum(sales) over() AS grand_total,
    sum(sales) over(PARTITION BY country) AS country_total,
    round((sales / sum(sales) over()*100.0)::numeric, 2) AS pct_of_total
FROM total_sales_tbl
ORDER BY sales DESC;

SELECT *
FROM 
    unnest(ARRAY[5,10,20,30,25,10]) t1, 
    unnest(ARRAY[5,10,20,30,25,10]) t2



SELECT  
    country,
    city,
    sales,
    CASE sales
        WHEN max(sales) OVER () THEN 'Лучший результат'
        WHEN min(sales) OVER () THEN 'Худший результат'
        ELSE 'Средний результат'
    END AS result
FROM total_sales_tbl
ORDER BY sales DESC;


WITH sales_data AS (
SELECT 
    EXTRACT(YEAR FROM o.order_date) AS year,
    EXTRACT(MONTH FROM o.order_date) AS month,
    SUM(od.unit_price * od.quantity) AS sales
FROM order_details od
JOIN orders o ON o.order_id = od.order_id
GROUP BY 1, 2)
SELECT 
    YEAR,
    MONTH,
    sales,
    sum(sales) over(PARTITION BY year ORDER BY month) AS rolling_sum,
    avg(sales) over(ORDER BY YEAR, MONTH ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) AS rolling_3mon_avg_sales
FROM sales_data


WITH sales_data AS (
SELECT 
    EXTRACT(YEAR FROM o.order_date) AS year,
    EXTRACT(MONTH FROM o.order_date) AS month,
    SUM(od.unit_price * od.quantity) AS sales
FROM order_details od
JOIN orders o ON o.order_id = od.order_id
GROUP BY 1, 2),
fill_data AS (
  SELECT 
    t1 AS mon,
    t2 AS yr,
    0 AS sales
    FROM 
        unnest(ARRAY[1,2,3,4,5,6]) t1, 
        unnest(ARRAY[1995, 1996,1997]) t2  
),
unioned AS (
    SELECT * FROM sales_data
    UNION ALL
    SELECT * FROM fill_data
)
SELECT 
    YEAR,
    MONTH,
    sales,
    lag(sales, 1) over(ORDER BY YEAR, month) AS prev_month_sales,
    lag(sales, 12) over(ORDER BY YEAR, month) AS prev_year_month_sales,
    sales - lag(sales, 12) over(ORDER BY YEAR, month) AS sales_diff,
    lead(sales, 1) over(ORDER BY YEAR, month) AS next_month_sales
FROM unioned

SELECT 
    t1 AS mon,
    t2 AS yr,
    0 AS sales
FROM 
    unnest(ARRAY[1,2,3,4,5,6]) t1, 
    unnest(ARRAY[1996,1997]) t2

    
SELECT country, city, SUM(sales)
FROM v_total_sales vts
GROUP BY ROLLUP(country, city)


SELECT country, city, SUM(sales)
FROM v_total_sales vts
GROUP BY CUBE(country, city)


SELECT 
    country,
    city,
    SUM(sales)
FROM v_total_sales
GROUP BY GROUPING SETS((country),(city), ())
