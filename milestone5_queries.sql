SELECT 
	country_code,COUNT(country_code) AS total_no_stores
    FROM 
        dim_store_details
	GROUP BY 
        country_code
	ORDER BY 
        total_no_stores DESC;


SELECT 
    locality, COUNT(locality) 
    FROM 
        dim_store_details
    GROUP BY 
        locality
    ORDER BY 
        count DESC 
    LIMIT 7;


SELECT 	
    ROUND(CAST(SUM(orders_table.product_quantity*dim_products.product_price)AS NUMERIC),2) 
    AS sales,dim_date_times.month	
    FROM 
        orders_table
    INNER JOIN 
        dim_date_times ON dim_date_times.date_uuid = orders_table.date_uuid
    INNER JOIN 
        dim_products ON dim_products.product_code = orders_table.product_code
    GROUP BY 
        (dim_date_times.month)
    ORDER BY sales DESC
    LIMIT 6;


SELECT 	
    COUNT(*) AS number_of_sales,
    SUM(orders_table.product_quantity)
    AS product_quantity_count,
		CASE 
		WHEN 
            dim_store_details.store_type = 'Web Portal' 
        THEN
		'WEB'
		ELSE'offline'
		END
FROM 
    orders_table
LEFT JOIN 
    dim_store_details ON orders_table.store_code = dim_store_details.store_code
LEFT JOIN
    dim_products ON dim_products.product_code = orders_table.product_code
GROUP BY 
		CASE 
		WHEN dim_store_details.store_type = 'Web Portal' THEN
		'WEB'
		ELSE'offline'
		END


SELECT 
    dim_store_details.store_type, 
    ROUND(CAST(SUM(orders_table.product_quantity*dim_products.product_price) as NUMERIC), 2)
    AS total_sales,
    ROUND(COUNT( * ) / CAST((SELECT COUNT( * ) FROM orders_table) AS NUMERIC), 2) * 100 as "percentage_total(%)"
    FROM 
        orders_table
    LEFT JOIN 
        dim_store_details ON orders_table.store_code = dim_store_details.store_code
    LEFT JOIN
        dim_products ON dim_products.product_code = orders_table.product_code
    GROUP BY 
        dim_store_details.store_type 



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


SELECT 
    SUM(CAST(dim_store_details.staff_numbers AS NUMERIC)) as staff_numbers, dim_store_details.country_code 
    FROM 
        dim_store_details
    GROUP BY 
        country_code
    ORDER BY 
        staff_numbers DESC


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