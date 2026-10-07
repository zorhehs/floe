-- Q03 quarterly revenue by order status over three years
SELECT year(o_orderdate) AS yr, quarter(o_orderdate) AS qtr, o_orderstatus,
       count(*) AS orders, sum(o_totalprice) AS revenue
FROM lakehouse.shop.orders
WHERE o_orderdate >= DATE '1994-01-01' AND o_orderdate < DATE '1997-01-01'
GROUP BY 1, 2, 3
ORDER BY 1, 2, 3;
