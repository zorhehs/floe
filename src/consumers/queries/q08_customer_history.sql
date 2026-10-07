-- Q08 one customer's order history (selective filter on non-key column)
SELECT o_orderkey, o_orderdate, o_orderstatus, o_totalprice
FROM lakehouse.shop.orders
WHERE o_custkey = 74431
ORDER BY o_orderdate;
