--MRDC Milestone 3

--Task 1: Cast types to orders_table

-- ALTER TABLE orders_table 
-- ALTER COLUMN product_quantity TYPE SMALLINT,
-- ALTER COLUMN date_uuid TYPE UUID USING date_uuid::UUID,
-- ALTER COLUMN user_uuid TYPE UUID USING user_uuid::UUID,
-- ALTER COLUMN card_number TYPE VARCHAR(19),
-- ALTER COLUMN store_code TYPE VARCHAR(12),
-- ALTER COLUMN product_code TYPE VARCHAR(11);

-- SELECT * FROM orders_table
-- LIMIT 10;

-- Cast types to dim_users_table

-- ALTER TABLE dim_users
-- ALTER COLUMN first_name TYPE VARCHAR(225),
-- ALTER COLUMN last_name TYPE VARCHAR(225),
-- ALTER COLUMN date_of_birth TYPE DATE,
-- ALTER COLUMN country_code TYPE VARCHAR(2),
-- ALTER COLUMN user_uuid TYPE UUID USING user_uuid::UUID,
-- ALTER COLUMN join_date TYPE DATE;

-- SELECT * FROM dim_users
-- LIMIT 10;

-- Adapt Null & concat lat to remove dups dim_store_details



-- UPDATE dim_stores_details
-- SET latitude = NULL
-- WHERE latitude = 'N/A';
-- UPDATE dim_stores_details
-- SET longitude = NULL
-- WHERE longitude = 'N/A';
-- -- --Drop/Merge latitude column
-- UPDATE dim_stores_details
-- SET latitude = concat(lat, latitude);
-- ALTER TABLE dim_stores_details
-- DROP COLUMN lat;

-- SELECT * FROM dim_stores_details
-- LIMIT 10;

-- -- Cast types to dim_store_details

-- ALTER TABLE dim_stores_details
-- ALTER COLUMN longitude TYPE FLOAT USING longitude::DOUBLE PRECISION,       
-- ALTER COLUMN locality  TYPE VARCHAR(255),           
-- ALTER COLUMN store_code TYPE VARCHAR(12),             
-- ALTER COLUMN staff_numbers TYPE SMALLINT USING staff_numbers::SMALLINT,     
-- ALTER COLUMN opening_date TYPE DATE,      
-- ALTER COLUMN store_type TYPE VARCHAR(255),  
-- ALTER COLUMN latitude TYPE FLOAT USING latitude::DOUBLE PRECISION,   
-- ALTER COLUMN country_code TYPE VARCHAR(2),  
-- ALTER COLUMN continent TYPE VARCHAR(255);   

--Add new column weight_class

-- SELECT * FROM dim_products
-- LIMIT 10;


-- ALTER TABLE dim_products
-- -- ALTER COLUMN weight FLOAT(64)
-- ADD COLUMN weight_class VARCHAR(15)
-- UPDATE dim_products
-- SET weight_class =

-- 	CASE  
-- 		WHEN weight < 2 THEN '<2'
-- -- 		WHEN weight BETWEEN 2 AND 39 THEN '>=2 - < 40'
-- -- 		WHEN weight BETWEEN 40 AND 139 THEN '>= 40 - < 140'
-- -- 		WHEN weight >= 140 THEN '=> 140'
-- 	END


--Task 5: Update the dim_products with the required data types

-- ALTER TABLE dim_products
-- 	RENAME COLUMN removed TO still_available;

SELECT * FROM dim_products
LIMIT 10;

-- ALTER TABLE dim_products
-- ALTER COLUMN product_price TYPE FLOAT USING product_price::DOUBLE PRECISION,
-- ALTER COLUMN weight TYPE FLOAT USING weight::DOUBLE PRECISION, 
-- ALTER COLUMN category TYPE VARCHAR(18),
-- ALTER COLUMN "EAN" TYPE VARCHAR(17),
-- ALTER COLUMN date_added TYPE DATE,
-- ALTER COLUMN uuid TYPE UUID USING uuid::UUID, 
-- ALTER COLUMN still_available TYPE BOOL USING (still_available='still_available'),
-- ALTER COLUMN product_code TYPE VARCHAR(11);