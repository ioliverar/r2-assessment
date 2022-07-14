-- Reporting Outputs

-- Output 1:

-- Total processed transactions last 40 batch-days
SELECT batch_date, count(1)
FROM transaction_sales ts
WHERE ts.batch_date <= date_add(now(), INTERVAL -40 day)
GROUP BY batch_date
ORDER BY batch_date DESC;

-- Total processed transactions last 40 processed-days
SELECT date_format(ts.created_datetime,'%Y-%m-%d') AS processing_date, count(1)
FROM transaction_sales ts
WHERE ts.created_datetime > date_add(now(), INTERVAL -40 day)
GROUP BY date_format(ts.created_datetime,'%Y-%m-%d')
ORDER BY date_format(ts.created_datetime,'%Y-%m-%d') DESC;


-- Output 2:

-- Total stores by date, total amount by date, and average for the last 40 days
SELECT date_format(transaction_datetime,'%Y-%m-%d') AS transaction_date, 
	   count(store_token) AS total_stores,
	   sum(amount) AS total_sales,
	   round(avg(amount), 3) AS average
FROM transaction_sales ts
WHERE ts.transaction_datetime < date_add(now(), INTERVAL -40 day)
GROUP BY date_format(transaction_datetime,'%Y-%m-%d')
ORDER BY date_format(transaction_datetime,'%Y-%m-%d') DESC;

-- Total accumulated sales amount by transaction's moth
SELECT date_format(transaction_datetime,'%Y-%m') AS transaction_moth, 
	   sum(amount) AS total_sales
FROM transaction_sales ts
WHERE ts.transaction_datetime < date_add(now(), INTERVAL -40 day)
GROUP BY date_format(transaction_datetime,'%Y-%m')
ORDER BY date_format(transaction_datetime,'%Y-%m') DESC;

-- Store token for one of the top sales stores
SELECT *
FROM (
SELECT store_token, 
	   count(1),
	   sum(amount) AS total_sales
FROM transaction_sales ts
WHERE ts.transaction_datetime < date_add(now(), INTERVAL -40 day)
GROUP BY store_token
ORDER BY sum(amount) DESC
) s
LIMIT 1;

-- Output 3:

--  Top 5 sales stores for the las 10 days

SELECT ts.*
FROM (
	SELECT date_format(ts.transaction_datetime,'%Y-%m-%d') AS transaction_date, 
		   sum(ts.amount) AS total_sales,
		   ts.store_token,
		   s.store_name,
		   DENSE_RANK() OVER(ORDER BY sum(ts.amount) DESC) rank_id
	FROM transaction_sales ts
	LEFT JOIN stores s
		ON s.store_token = substr(ts.store_token,1,36)
	WHERE ts.transaction_datetime < date_add(now(), INTERVAL -10 day)
	GROUP BY date_format(ts.transaction_datetime,'%Y-%m-%d'), ts.store_token, s.store_name
) ts
WHERE ts.rank_id <= 5;
