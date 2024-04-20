-- Databricks notebook source
-- MAGIC %md-sandbox
-- MAGIC
-- MAGIC <div  style="text-align: center; line-height: 0; padding-top: 9px;">
-- MAGIC   <img src="https://dalhussein.blob.core.windows.net/course-resources/bookstore_schema.png" alt="Databricks Learning" style="width: 600">
-- MAGIC </div>

-- COMMAND ----------

-- MAGIC %run ../Includes/Copy-Datasets

-- COMMAND ----------

Describe history customers

-- COMMAND ----------

SELECT * FROM customers limit 5

-- COMMAND ----------

DESCRIBE customers

-- COMMAND ----------

SELECT customer_id, profile:first_name, profile:address:country 
FROM customers
Limit 5

-- COMMAND ----------

SELECT customer_id, profile:first_name, profile:address 
FROM customers
Limit 5

-- COMMAND ----------

SELECT from_json(profile) AS profile_struct
  FROM customers;

-- COMMAND ----------

SELECT profile 
FROM customers 
LIMIT 1

-- COMMAND ----------


--using 'schema_of_json' junction we can pass a sample column and extract schema from it 
--using 'from_json' we can change a json field as struct so we can access the fields in '.' format rather than ':' as in json format.

CREATE OR REPLACE TEMP VIEW parsed_customers AS
  SELECT customer_id, from_json(profile, schema_of_json('{"first_name":"Thomas","last_name":"Lane","gender":"Male","address":{"street":"06 Boulevard Victor Hugo","city":"Paris","country":"France"}}')) AS profile_struct
  FROM customers;
  
SELECT * FROM parsed_customers
limit 4

-- COMMAND ----------

DESCRIBE parsed_customers

-- COMMAND ----------

SELECT customer_id, profile_struct.first_name, profile_struct.address.country
FROM parsed_customers
limit 4

-- COMMAND ----------

-- once json field is changed into strct we can query it using * operator
CREATE OR REPLACE TEMP VIEW customers_final AS
  SELECT customer_id, profile_struct.*
  FROM parsed_customers;
  
SELECT * FROM customers_final
limit 3

-- COMMAND ----------

SELECT order_id, customer_id, books
FROM orders
where customer_id = 'C00002'
limit 5

-- COMMAND ----------

-- use 'explode' function to create seperate rows for nested fields
SELECT order_id, customer_id, explode(books) AS book 
FROM orders
where customer_id = 'C00002'

-- COMMAND ----------

SELECT customer_id,
  order_id AS orders_set,
  books AS books_set
FROM orders
where customer_id in ('C00001','C00002','C00004')


-- COMMAND ----------

-- use 'collect_set' is a aggregation function to collect all the values for a grouped column and removes duplicates
SELECT customer_id,
  collect_set(order_id) AS orders_set,
  collect_set(books.book_id) AS books_set
FROM orders
where customer_id in ('C00001','C00002','C00004')
GROUP BY customer_id


-- COMMAND ----------

-- 'flatten' func flattens the nested array 
-- 'array_distinct' selects only the distict array values
SELECT customer_id,
  collect_set(books.book_id) As before_flatten,
  array_distinct(flatten(collect_set(books.book_id))) AS after_flatten
FROM orders
where customer_id in ('C00001','C00002','C00004')
GROUP BY customer_id

-- COMMAND ----------

-- inner join
CREATE OR REPLACE VIEW orders_enriched AS
SELECT *
FROM (
  SELECT *, explode(books) AS book 
  FROM orders) o
INNER JOIN books b
ON o.book.book_id = b.book_id;

SELECT * FROM orders_enriched
limit 3

-- COMMAND ----------

-- union
CREATE OR REPLACE TEMP VIEW orders_updates
AS SELECT * FROM parquet.`${dataset.bookstore}/orders-new`;

SELECT * FROM orders 
UNION 
SELECT * FROM orders_updates 
limit 3

-- COMMAND ----------

--returns common rows
SELECT * FROM orders 
INTERSECT 
SELECT * FROM orders_updates


-- COMMAND ----------

SELECT * FROM orders 
MINUS 
SELECT * FROM orders_updates 

-- COMMAND ----------

--PIVOT
CREATE OR REPLACE TABLE transactions AS

SELECT * FROM (
  SELECT
    customer_id,
    book.book_id AS book_id,
    book.quantity AS quantity
  FROM orders_enriched
) PIVOT (
  sum(quantity) FOR book_id in (
    'B01', 'B02', 'B03', 'B04', 'B05', 'B06',
    'B07', 'B08', 'B09', 'B10', 'B11', 'B12'
  )
);

SELECT * FROM transactions
limit 4

-- COMMAND ----------

-- higher order functions
-- here we use 'FILTER' function to filter books with quantity >= 2 in a new col many_books
-- implementing filter func using lambda 
SELECT
  order_id,
  books,
  FILTER (books, i -> i.quantity >= 2) AS many_books
FROM orders

-- COMMAND ----------

SELECT order_id, many_books
FROM (
  SELECT
    order_id,
    FILTER (books, i -> i.quantity >= 2) AS many_books
  FROM orders)
WHERE size(many_books) > 0
limit 5;

-- COMMAND ----------

--'TRANSFORM' func
-- used to apply atransformation on all the elements of the array and extract tranformed values
SELECT
  order_id,
  books,
  TRANSFORM (
    books,
    k -> CAST(k.subtotal * 0.8 AS INT)
  ) AS subtotal_after_discount
FROM orders
limit 5;

-- COMMAND ----------


