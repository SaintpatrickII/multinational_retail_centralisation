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
WHERE weight = NULL
LIMIT 10;


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

SELECT * FROM dim_products;
DELETE FROM dim_products WHERE weight IS NULL;


ALTER TABLE orders_table 
ALTER COLUMN product_quantity TYPE SMALLINT,
ALTER COLUMN date_uuid TYPE UUID USING date_uuid::UUID,
ALTER COLUMN user_uuid TYPE UUID USING user_uuid::UUID,
ALTER COLUMN card_number TYPE VARCHAR(19),
ALTER COLUMN store_code TYPE VARCHAR(12),
ALTER COLUMN product_code TYPE VARCHAR(11);

SELECT * FROM orders_table
LIMIT 10;


SELECT * FROM dim_products
WHERE product_code= 'c7-2549027x';


-- -look at missing rows in order & products:
WITH join_prod_ord AS (SELECT *
FROM orders_table ord
LEFT JOIN  dim_products prod
ON  prod.product_code = ord.product_code
WHERE ord.product_code = 'c7-2549027x')

SELECT * FROM join_prod_ord;

WITH join_prod_ord AS (SELECT *
FROM orders_table ord
LEFT JOIN  dim_products prod
ON  prod.product_code = ord.product_code
WHERE ord.product_code = 'A8-4686892S')

SELECT * FROM join_prod_ord;



WITH join_prod_ord AS (SELECT *
FROM orders_table ord
LEFT JOIN  dim_products prod
ON  prod.product_code = ord.product_code)

SELECT * FROM join_prod_ord;

SELECT * FROM dim_products
WHERE product_name = 'Skin Techniques Gold Hydrogel Collagen Eye Mask'
LIMIT 10;


SELECT * FROM dim_products
WHERE product_code = 'R7-3126933h';


-- 2 x 200g example
SELECT * FROM dim_products
WHERE product_code = 'M5-8164943v';

--kg example
SELECT * FROM dim_products
WHERE product_code = 'U3-5148457q';

--ml example V3-7914798Q
SELECT * FROM dim_products
WHERE product_code = 'V3-7914798Q';

--g example A3-7619070S
SELECT * FROM dim_products
WHERE product_code = 'A3-7619070S';

-- weight barcode check XCD69KUI0K
SELECT * FROM dim_products
WHERE product_code = 'w5-6777421C';


SELECT * FROM orders_table
WHERE product_code = 'w5-6777421C';


SELECT DISTINCT(ord.product_code)
FROM orders_table ord
WHERE NOT EXISTS 
	(SELECT * FROM dim_products prod
	WHERE prod.product_code = ord.product_code);


SELECT * FROM dim_card_details
WHERE card_number = '4305628477334070000';

DROP TABLE dim_products;



ALTER TABLE orders_table ADD CONSTRAINT fk_product_code FOREIGN KEY (product_code) REFERENCES dim_products (product_code);