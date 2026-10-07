-- Q02 selective date-range scan (one month): partition pruning target
SELECT count(*) AS orders, sum(o_totalprice) AS revenue
FROM lakehouse.shop.orders
WHERE o_orderdate BETWEEN DATE '1995-03-01' AND DATE '1995-03-31';
