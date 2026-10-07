-- Q07 market segment x priority for urgent orders in one quarter
SELECT c.c_mktsegment, o.o_orderpriority, count(*) AS orders, avg(o.o_totalprice) AS avg_price
FROM lakehouse.shop.orders o
JOIN lakehouse.shop.customer c ON c.c_custkey = o.o_custkey
WHERE o.o_orderdate BETWEEN DATE '1995-01-01' AND DATE '1995-03-31'
  AND o.o_orderpriority IN ('1-URGENT', '2-HIGH')
GROUP BY 1, 2
ORDER BY 1, 2;
