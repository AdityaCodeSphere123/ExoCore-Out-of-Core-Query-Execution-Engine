SELECT n_name AS country, n_regionkey AS region, ''
FROM nation
WHERE n_regionkey >= 3
  AND n_nationkey < 23
  AND n_name <> 'IRAQ';