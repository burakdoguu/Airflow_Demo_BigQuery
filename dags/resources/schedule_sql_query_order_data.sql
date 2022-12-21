CREATE OR REPLACE TABLE `precise-blend-371110.airflow_demo.top_10_product_category` (productid STRING, total_bought INTEGER,categoryid STRING)
OPTIONS(
  description="Top ten words per Shakespeare corpus"
) AS
SELECT productid, total_bought, categoryid FROM (
  SELECT productid, total_bought, categoryid,CAST(regexp_extract(categoryid, '[0-9]+') as INT64)
FROM (
  SELECT 
  (ROW_NUMBER() OVER (PARTITION BY categoryid ORDER BY SUM(quantity) DESC)) as RN,
  productid, SUM(quantity) as total_bought,categoryid
  FROM (
    SELECT o.productid as productid,o.quantity as quantity,o.event,o.messageid,o.userid,o.orderid,c.categoryid as categoryid
    FROM `precise-blend-371110.airflow_demo.orders_data` o
    INNER JOIN `airflow_demo.product-category-map` c 
    ON o.productid = c.productid)
  GROUP BY productid, categoryid
)
WHERE RN <=10
ORDER BY 4
)