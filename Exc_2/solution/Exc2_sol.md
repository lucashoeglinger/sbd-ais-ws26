```sql
DROP TABLE IF EXISTS orders_big;

CREATE TABLE orders_big (
  id SERIAL PRIMARY KEY,
  customer_name TEXT,
  product_category TEXT,
  quantity INTEGER,
  price_per_unit FLOAT,
  order_date DATE,
  country TEXT
);

\COPY orders_big(customer_name,product_category,quantity,price_per_unit, order_date, country) FROM '/data/orders_1M.csv' DELIMITER ',' CSV HEADER;
```


```sql

SELECT customer_name, product_category, quantity, price_per_unit, order_date, country
FROM orders_big
WHERE price_per_unit = (SELECT MAX(price_per_unit) FROM orders_big);   
```


customer_name | product_category | quantity | price_per_unit | order_date | country 
--------+---------------+------------------+----------+----------------+------------+---------
 Emma Brown    | Automotive       |        3 |           2000 | 2024-10-11 | Italy
(1 row)

```sql

SELECT o.product_category, SUM(o.quantity) FROM orders_big as o ORDER BY o.quantity GROUP BY o.product_category DESC LIMIT 3;

```

 product_category | quantity 
------------------+----------
 Fashion          |        5
 Grocery          |        5
 Health & Beauty  |        5
(3 rows)

```sql

SELECT product_category, ROUND(SUM(quantity * price_per_unit)::numeric, 2) AS revenue FROM orders_big GROUP BY product_category;

```

 product_category |   revenue
------------------+--------------
 Automotive       | 306589798.86
 Books            |  12731976.04
 Electronics      | 241525009.45
 Fashion          |  31566368.22
 Grocery          |  15268355.66
 Health & Beauty  |  46599817.89
 Home & Garden    |  78023780.09
 Office Supplies  |  38276061.64
 Sports           |  61848990.83
 Toys             |  23271039.02
(10 rows)

```sql

SELECT customer_name, ROUND(SUM(quantity * price_per_unit)::numeric, 2) AS total_spend FROM orders_big 
GROUP BY customer_name
ORDER BY total_spend DESC LIMIT 3;
```

 customer_name  | total_spend 
----------------+-------------
 Carol Taylor   |   991179.18
 Nina Lopez     |   975444.95
 Daniel Jackson |   959344.48
(3 rows)

```sql

SELECT customer_name, ROUND(SUM(quantity * price_per_unit)::numeric, 2) AS total_spend, COUNT(customer_name) AS count FROM orders_big 
GROUP BY customer_name
ORDER BY total_spend DESC LIMIT 3;
```


 customer_name  | total_spend | count 
----------------+-------------+-------
 Carol Taylor   |   991179.18 |  1028
 Nina Lopez     |   975444.95 |   980
 Daniel Jackson |   959344.48 |  1033
(3 rows)


Run the self-join (with `\timing on`) on each of the three tables and fill in:

| rows in table | join result (`COUNT(*)`) | time |
|---|---|---|
| 50 000 | 27501822 | 1149.892 ms|
| 100 000 | 109946508 | 4401.405 ms|
| 200 000 | 439395606 | 27963.736 ms|


when the input doubles, the time grows by $\approx 4 (3.81)$. For the whole 1M rows it would take roughly by this estimate. $$2.25 iterations$$ this would mean the time would take approximately (699,093 ms (~11.6 min)) 



CREATE INDEX idx_country ON people_100k (country)


```sql
SELECT SUM(n * n)
FROM (
    SELECT country, COUNT(*) AS n
    FROM people_big
    GROUP BY country
) sub;   
```


Rewrite vs. hardware/index.
The rewrite changes the algorithm from O(n²) to O(n). No amount of RAM, CPU, or indexing can make the join version scale as it is bounded by the output size. The rewrite eliminates the quadratic term entirely. An index is a constant-factor optimization; a rewrite is a complexity-class change. In a real system, algorithmic improvement always wins over hardware.

If the business needs the actual pairs.
The output is Θ(n²) in the worst case (all same country) so we must materialize that many rows. A bigger machine helps only linearly (more RAM to buffer, more cores for parallel scan). A cluster partitioned by country is the right approach: each node handles only its own partition's pairs, so the per-node work is O(n/k)² where k = number of nodes. But total work across the cluster is still proportional to the number of pairs. You cannot avoid the I/O of writing O(n²) rows to disk/network. In practice this means: stream results, don't materialize, and push the computation as close to the data as possible (MapReduce / Spark / dbt + warehouse).

OLTP limits in a cloud environment.
An OLTP database (Postgres, MySQL) is tuned for short, low-latency transactions (point lookups, small updates). A query that scans 1M rows, holds O(n²) intermediate results, and runs for 10+ minutes will:

- exhaust shared buffers / work_mem and spill to disk,
- hold locks or block checkpoints,
- degrade p99 latency for all other transactions on the same instance.