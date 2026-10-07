-- Q05 top 10 customers by lifetime spend (full scan + join + top-N)
SELECT c.c_custkey, c.c_name, c.c_mktsegment, sum(o.o_totalprice) AS spend, count(*) AS orders
FROM lakehouse.shop.orders o
JOIN lakehouse.shop.customer c ON c.c_custkey = o.o_custkey
GROUP BY 1, 2, 3
ORDER BY spend DESC
LIMIT 10;
