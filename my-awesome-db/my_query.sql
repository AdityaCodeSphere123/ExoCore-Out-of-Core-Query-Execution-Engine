SELECT *,''
FROM customer
CROSS JOIN orders
WHERE c_custkey = o_custkey ORDER BY c_custkey;