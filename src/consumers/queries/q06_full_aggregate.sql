-- Q06 full-table aggregate
SELECT count(*) AS orders, sum(o_totalprice) AS revenue, avg(o_totalprice) AS avg_order,
       count(DISTINCT o_custkey) AS customers
FROM lakehouse.shop.orders;
