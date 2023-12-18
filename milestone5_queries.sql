
-- Task 1: How many stores in each country
-- count country code, aggregate by country code
-- order by no. of stores desc
SELECT 
	country_code, COUNT(country_code) AS total_no_stores
    FROM 
        dim_store_details
	GROUP BY 
        country_code
	ORDER BY 
        total_no_stores DESC;


-- Task 2: which locations have the most stores,
-- Count locality, aggregate by locality, 
-- Order by count desc
SELECT 
    locality, COUNT(locality) 
    FROM 
        dim_store_details
    GROUP BY 
        locality
    ORDER BY 
        COUNT DESC 
    LIMIT 7;



-- Task 3: Which months have highest amount of sales
-- Need total_sales which is product_quantity * product price
-- need to join onto date_time to get months
-- aggregate by month
SELECT 	
    ROUND(CAST(SUM(ord.product_quantity * dp.product_price) AS NUMERIC), 2) 
    AS sales, ddt.month	
    FROM 
        orders_table ord
    INNER JOIN 
        dim_date_times ddt 
    ON 
        ddt.date_uuid = ord.date_uuid
    INNER JOIN 
        dim_products dp
    ON 
        dp.product_code = ord.product_code
    GROUP BY 
        (ddt.month)
    ORDER BY sales DESC
    LIMIT 6;


-- Task 4: How many sales from online
-- Count all rows, for sum of sales
-- sum of product quantity for total quantity
SELECT 	
    COUNT(*) AS number_of_sales,
    SUM(ord.product_quantity)
    AS product_quantity_count,
		CASE 
            WHEN 
                dsd.store_type = 'Web Portal' 
            THEN
                'WEB'
            ELSE
                'offline'
		END
    FROM 
        orders_table ord
    LEFT JOIN 
        dim_store_details dsd 
    ON 
        ord.store_code = dsd.store_code
    LEFT JOIN
        dim_products dp 
    ON 
        dp.product_code = ord.product_code
    GROUP BY 
            CASE 
                WHEN 
                    dsd.store_type = 'Web Portal' 
                THEN
                    'WEB'
                ELSE
                    'offline'
            END

-- Alternate

-- select
-- 	count(*) as number_of_sales,
-- 	sum(product_quantity),
-- 	case 
-- 		when store_code = 'WEB-1388012W' then 'Web'
-- 		else 'Offline'
-- 	end as location
-- from orders_table
-- group by location



-- Task 5: percentage of sales through each store
-- Need store type, product quantity and product price (orders, products & store_details tables)
-- 
SELECT 
    dsd.store_type, 
    ROUND(CAST(SUM(ord.product_quantity * dp.product_price) as NUMERIC), 2)
    AS total_sales,
    ROUND(COUNT( * ) / CAST((SELECT COUNT( * ) FROM orders_table) AS NUMERIC), 2) * 100 as "percentage_total(%)"
    FROM 
        orders_table ord
    LEFT JOIN 
        dim_store_details dsd
    ON 
        ord.store_code = dsd.store_code
    LEFT JOIN
        dim_products dp
    ON 
        dp.product_code = ord.product_code
    GROUP BY 
        dsd.store_type 
    ORDER BY
        total_sales DESC


-- Task 6: Sales by month

SELECT 
    ROUND(CAST(SUM(dim_products.product_price * orders_table.product_quantity) AS NUMERIC), 2) as total_sales, dim_date_times.year, dim_date_times.month 
    FROM 
        orders_table

    LEFT JOIN 
        dim_date_times ON orders_table.date_uuid  = dim_date_times.date_uuid
    LEFT JOIN 
        dim_products ON orders_table.product_code = dim_products.product_code
    GROUP BY 
        dim_date_times.year, dim_date_times.month
    ORDER BY 
        total_sales DESC LIMIT 10


-- Task 7: Staff by Stores
SELECT 
    SUM(CAST(dim_store_details.staff_numbers AS NUMERIC)) as staff_numbers, dim_store_details.country_code 
    FROM 
        dim_store_details
    GROUP BY 
        country_code
    ORDER BY 
        staff_numbers DESC

-- Task 8: German Store sales
SELECT 
    ROUND(CAST(SUM(dim_products.product_price * orders_table.product_quantity) AS NUMERIC), 2) as total_sales, 
    dim_store_details.store_type, 
    dim_store_details.country_code 
    FROM 
        orders_table
    INNER JOIN 
        dim_products on orders_table.product_code = dim_products.product_code 
    INNER JOIN 
        dim_store_details on orders_table.store_code = dim_store_details.store_code 
    WHERE 
        dim_store_details.country_code = 'DE'
    GROUP BY 
        store_type, country_code
    ORDER BY 
        total_sales 

-- Task 9

WITH date_times AS (
SELECT
year,
month,
day,
timestamp,
TO_TIMESTAMP(CONCAT(year, '/', month, '/', day, '/', timestamp), 'YYYY/MM/DD/HH24:MI:ss') as times

			   FROM dim_date_times d
					 JOIN orders_table o
					 ON d.date_uuid = o.date_uuid
					 JOIN dim_store_details s
					 ON o.store_code = s.store_code
			   ORDER BY times DESC),		   	


next_times AS(
SELECT year,
timestamp,
times,
LEAD(times) OVER(ORDER BY times DESC) AS next_times
FROM date_times),

avg_times AS(
SELECT year,
(AVG(times - next_times)) AS avg_times
FROM next_times
GROUP BY year
ORDER BY avg_times DESC)

SELECT year,
-- concat('hours: ', cast(round(avg(EXTRACT(HOUR FROM avg_times))) as text),
-- 	   ', minutes: ', cast(round(avg(EXTRACT(MINUTE FROM avg_times))) as text),
-- 	   ', seconds: ', cast(round(avg(EXTRACT(SECOND FROM avg_times))) as text))
-- 	   as actual_time_taken

	CONCAT('"Hours": ', (EXTRACT(HOUR FROM avg_times)),','
	' "minutes" :', (EXTRACT(MINUTE FROM avg_times)),','
    ' "seconds" :', ROUND(EXTRACT(SECOND FROM avg_times)),','
     ' "milliseconds" :', ROUND((EXTRACT( SECOND FROM avg_times)- FLOOR(EXTRACT(SECOND FROM avg_times)))*100))
	
   as actual_time_taken


FROM avg_times
GROUP BY year, avg_times
ORDER BY avg_times DESC
LIMIT 5;


WITH cte AS(
    SELECT TO_TIMESTAMP(CONCAT(year, '-', month, '-', day, ' ', timestamp),
    'YYYY-MM-DD HH24:MI:SS') as datetimes, year FROM dim_date_times
    ORDER BY datetimes DESC
), cte2 AS(
    SELECT 
        year, 
        datetimes, 
        LEAD(datetimes, 1) OVER (ORDER BY datetimes DESC) as time_difference 
        FROM cte
) SELECT year, AVG((datetimes - time_difference)) as actual_time_taken FROM cte2
GROUP BY year
ORDER BY actual_time_taken DESC
LIMIT 5;