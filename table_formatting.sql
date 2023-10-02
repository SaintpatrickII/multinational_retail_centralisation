--MRDC Milestone 3

--Task 1: Cast types to orders_table

ALTER TABLE orders_table
ALTER COLUMN product_quantity TYPE SMALLINT,
ALTER COLUMN date_uuid TYPE UUID USING date_uuid::UUID,
ALTER COLUMN user_uuid TYPE UUID USING user_uuid::UUID,
ALTER COLUMN card_number TYPE VARCHAR(19),
ALTER COLUMN store_code TYPE VARCHAR(12),
ALTER COLUMN product_code TYPE VARCHAR(11);

--Task 2: Cast types to dim_users

ALTER TABLE dim_users
ALTER COLUMN first_name TYPE VARCHAR(225),
ALTER COLUMN last_name TYPE VARCHAR(225),
ALTER COLUMN date_of_birth TYPE DATE,
ALTER COLUMN country_code TYPE VARCHAR(2),
ALTER COLUMN user_uuid TYPE UUID USING user_uuid::UUID,
ALTER COLUMN  join_date TYPE DATE ;

-- --Task 3: Modify dim_store_details  

-- --Update webstore row where there is a NULL

UPDATE dim_store_details
SET latitude = NULL
WHERE latitude = 'N/A';
UPDATE dim_store_details
SET longitude = NULL
WHERE longitude = 'N/A';
--Drop/Merge latitude column
ALTER TABLE dim_store_details
DROP COLUMN lat;
UPDATE dim_store_details
SET latitude = concat(lat, latitude);

-- --Cast types to dim_store_details - to do this, ensure that web portal row is 'N/A' for VARCHAR and NULL for INT/FLOAT

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

-- --Task 4: Make changes to the dim_products for delivery team

-- --Remove £ sign from product_price

-- UPDATE dim_products SET product_price = REPLACE(product_price, '£', '');

-- --Add new column weight_class

ALTER TABLE dim_products
ADD COLUMN weight_class VARCHAR(15)
UPDATE dim_products
SET weight_class =

	CASE  
		WHEN weight < 2 THEN '<2'
		WHEN weight BETWEEN 2 AND 39 THEN '>=2 - < 40'
		WHEN weight BETWEEN 40 AND 139 THEN '>= 40 - < 140'
		WHEN weight >= 140 THEN '=> 140'
	END

-- --Task 5: Update the dim_products with the required data types

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

-- --Task 6: Update dim_date_times table

ALTER TABLE dim_date_times
ALTER COLUMN month TYPE VARCHAR(2),
ALTER COLUMN year TYPE VARCHAR(4),
ALTER COLUMN day TYPE VARCHAR(2),
ALTER COLUMN time_period TYPE VARCHAR(10),
ALTER COLUMN date_uuid TYPE UUID USING date_uuid::UUID

-- --Task 7: Update dim_card_details

ALTER TABLE dim_card_details
ALTER COLUMN card_number TYPE VARCHAR(19),
ALTER COLUMN expiry_date TYPE VARCHAR(19),
ALTER COLUMN date_payment_confirmed TYPE DATE

-- --Task 8: Set a PRIMARY KEY in the dimension tables

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

-- --Task 9: Finalise the star-based schema and add foreign keys to the orders table

ALTER TABLE orders_table ADD CONSTRAINT fk_dim_date_times FOREIGN KEY (date_uuid) REFERENCES dim_date_times (date_uuid);
ALTER TABLE orders_table ADD CONSTRAINT fk_product_code FOREIGN KEY (product_code) REFERENCES dim_products (product_code);
ALTER TABLE orders_table ADD CONSTRAINT fk_store_code FOREIGN KEY (store_code) REFERENCES dim_store_details (store_code);
ALTER TABLE orders_table ADD CONSTRAINT fk_card_number FOREIGN KEY (card_number) REFERENCES dim_card_details (card_number); 
ALTER TABLE orders_table ADD CONSTRAINT fk_user_uuid FOREIGN KEY (user_uuid) REFERENCES dim_users (user_uuid); 