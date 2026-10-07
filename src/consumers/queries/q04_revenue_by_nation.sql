-- Q04 three-way join: revenue per nation/region for one year
SELECT r.r_name AS region, n.n_name AS nation, count(*) AS orders, sum(o.o_totalprice) AS revenue
FROM lakehouse.shop.orders o
JOIN lakehouse.shop.customer c ON c.c_custkey = o.o_custkey
JOIN lakehouse.shop.nation n   ON n.n_nationkey = c.c_nationkey
JOIN lakehouse.shop.region r   ON r.r_regionkey = n.n_regionkey
WHERE o.o_orderdate >= DATE '1996-01-01' AND o.o_orderdate < DATE '1997-01-01'
GROUP BY 1, 2
ORDER BY revenue DESC;
