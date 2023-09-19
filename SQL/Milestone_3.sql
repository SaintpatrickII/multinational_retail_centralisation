--MRDC Milestone 3

--Task 1: Cast types to orders_table

ALTER TABLE orders_table 
ALTER COLUMN product_quantity TYPE SMALLINT,
ALTER COLUMN date_uuid TYPE UUID USING date_uuid::UUID,
ALTER COLUMN user_uuid TYPE UUID USING user_uuid::UUID,
ALTER COLUMN card_number TYPE VARCHAR(19),
ALTER COLUMN store_code TYPE VARCHAR(12),
ALTER COLUMN product_code TYPE VARCHAR(11);

SELECT * FROM orders_table
LIMIT 10;

-- Cast types to dim_users_table

ALTER TABLE dim_users
ALTER COLUMN first_name TYPE VARCHAR(225),
ALTER COLUMN last_name TYPE VARCHAR(225),
ALTER COLUMN date_of_birth TYPE DATE,
ALTER COLUMN country_code TYPE VARCHAR(2),
ALTER COLUMN user_uuid TYPE UUID USING user_uuid::UUID,
ALTER COLUMN join_date TYPE DATE;

SELECT * FROM dim_users
LIMIT 10;

-- Adapt Null & concat lat to remove dups dim_store_details



UPDATE dim_store_details
SET latitude = NULL
WHERE latitude = 'N/A';
UPDATE dim_store_details
SET longitude = NULL
WHERE longitude = 'N/A';
-- --Drop/Merge latitude column
UPDATE dim_store_details
SET latitude = concat(lat, latitude);
ALTER TABLE dim_store_details
DROP COLUMN lat;

SELECT * FROM dim_store_details
WHERE longitude = '2YBZ1440V6';

-- -- Cast types to dim_store_details

ALTER TABLE dim_store_details
ALTER COLUMN longitude TYPE FLOAT USING longitude::DOUBLE PRECISION,       
ALTER COLUMN locality  TYPE VARCHAR(255),           
ALTER COLUMN store_code TYPE VARCHAR(12),             
ALTER COLUMN staff_numbers TYPE SMALLINT USING staff_numbers::SMALLINT,     
ALTER COLUMN opening_date TYPE DATE,      
ALTER COLUMN store_type TYPE VARCHAR(255),  
ALTER COLUMN latitude TYPE FLOAT USING latitude::DOUBLE PRECISION,   
ALTER COLUMN country_code TYPE VARCHAR(2),  
ALTER COLUMN continent TYPE VARCHAR(255);   

SELECT * FROM dim_store_details
LIMIT 10;

--Add new column weight_class

-- SELECT * FROM dim_products
-- LIMIT 10;


ALTER TABLE dim_products
ADD COLUMN weight_class VARCHAR(15);

UPDATE dim_products
SET weight_class =

	CASE  
		WHEN weight < 2 THEN 'Light'
		WHEN weight BETWEEN 2 AND 39 THEN 'Mid_Sized'
		WHEN weight BETWEEN 40 AND 139 THEN 'Heavy'
		WHEN weight >= 140 THEN 'Truck_Required'
	END;

SELECT * FROM dim_products
LIMIT 10;

--Task 5: Update the dim_products with the required data types

-- ALTER TABLE dim_products
-- 	RENAME COLUMN removed TO still_available;

-- SELECT * FROM dim_products
-- LIMIT 10;

ALTER TABLE dim_products
	RENAME COLUMN removed TO still_available;

ALTER TABLE dim_products
ALTER COLUMN product_price TYPE FLOAT USING product_price::DOUBLE PRECISION,
ALTER COLUMN weight TYPE FLOAT USING weight::DOUBLE PRECISION, 
ALTER COLUMN category TYPE VARCHAR(18),
ALTER COLUMN "EAN" TYPE VARCHAR(17),
ALTER COLUMN date_added TYPE DATE,
ALTER COLUMN uuid TYPE UUID USING uuid::UUID, 
ALTER COLUMN still_available TYPE BOOL USING (still_available='still_available'),
ALTER COLUMN product_code TYPE VARCHAR(11);

SELECT * FROM dim_products
LIMIT 10;


--Task 6: Update dim_date_times table

-- SELECT * FROM dim_date_times
-- LIMIT 10;

ALTER TABLE dim_date_times
ALTER COLUMN month TYPE VARCHAR(2),
ALTER COLUMN year TYPE VARCHAR(4),
ALTER COLUMN day TYPE VARCHAR(2),
ALTER COLUMN time_period TYPE VARCHAR(10),
ALTER COLUMN date_uuid TYPE UUID USING date_uuid::UUID;

SELECT * FROM dim_date_times
LIMIT 10;


-- Task 7: Update dim_card_details table

-- SELECT * FROM dim_card_details
-- LIMIT 10;

ALTER TABLE dim_card_details
ALTER COLUMN card_number TYPE VARCHAR(19),
ALTER COLUMN expiry_date TYPE VARCHAR(19),
ALTER COLUMN date_payment_confirmed TYPE DATE;

SELECT * FROM dim_card_details
LIMIT 10;

--Task 8: Set a PRIMARY KEY in the dimension tables

ALTER TABLE dim_card_details 
ADD PRIMARY KEY (card_number);
ALTER TABLE dim_date_times
ADD PRIMARY KEY (date_uuid);
ALTER TABLE dim_products
ADD PRIMARY KEY (product_code);
ALTER TABLE dim_store_details
ADD PRIMARY KEY (store_code);
ALTER TABLE dim_users
ADD PRIMARY KEY (user_uuid);

--Task 9: Finalise the star-based schema and add foreign keys to the orders table

SELECT * FROM orders_table
LIMIT 10;

ALTER TABLE orders_table ADD CONSTRAINT fk_dim_date_times FOREIGN KEY (date_uuid) REFERENCES dim_date_times (date_uuid);
ALTER TABLE orders_table ADD CONSTRAINT fk_product_code FOREIGN KEY (product_code) REFERENCES dim_products (product_code); --needs fix
ALTER TABLE orders_table ADD CONSTRAINT fk_store_code FOREIGN KEY (store_code) REFERENCES dim_store_details (store_code); -- needs fix
ALTER TABLE orders_table ADD CONSTRAINT fk_card_number FOREIGN KEY (card_number) REFERENCES dim_card_details (card_number); 
ALTER TABLE orders_table ADD CONSTRAINT fk_user_uuid FOREIGN KEY (user_uuid) REFERENCES dim_users (user_uuid); 


SELECT *
FROM dim_products prod
LEFT JOIN orders_table ord ON prod.product_code = ord.product_code;
-- WHERE prod.product_code IS NULL;

SELECT sto.store_code
FROM dim_store_details sto
LEFT JOIN orders_table ord ON sto.store_code = ord.store_code
WHERE sto.store_code IS NULL;



SELECT DISTINCT(ord.product_code)
FROM orders_table ord
WHERE NOT EXISTS 
	(SELECT * FROM dim_products prod
	WHERE prod.product_code = ord.product_code);

SELECT * FROM orders_table
WHERE product_code = 'c7-2549027x';


SELECT DISTINCT(ord.store_code)
FROM orders_table ord
WHERE NOT EXISTS 
	(SELECT * FROM dim_store_details sto
	WHERE sto.store_code = ord.store_code);

SELECT * FROM orders_table
LIMIT 10;

SELECT * FROM dim_products
LIMIT 10;

SELECT * FROM dim_store_details
LIMIT 10;

SELECT * FROM orders_table
LEFT JOIN 