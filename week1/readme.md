# Q1
```bash
# start  the container
docker run -it  python:3.13  bash
# get the version
pip  --version
# pip 25.3
```  

# Q2

postgres:5432  

# Q3

```SQL
SELECT COUNT(*)
FROM  taxi_data
WHERE CAST(lpep_pickup_datetime AS DATE)>='2025-11-01'
      AND  CAST(lpep_pickup_datetime AS DATE)<'2025-12-01'
      AND trip_distance<=1.0
```

Answer:  8007

# Q4

```SQL
SELECT MAX(trip_distance) as trip_distance_max,
       CAST(lpep_pickup_datetime AS DATE) as date
FROM  taxi_data 
WHERE trip_distance<=100.0
GROUP BY CAST(lpep_pickup_datetime AS DATE)
ORDER  BY trip_distance_max DESC
```

Answer: 2025-11-14

# Q5

```SQL
SELECT SUM(total_amount) as  total_amount_sum,
       l."Zone"
FROM  taxi_data t
LEFT JOIN loc_data l  ON t."PULocationID" =l."LocationID"
WHERE CAST(lpep_pickup_datetime AS DATE)  =  '2025-11-18'
GROUP  BY l."Zone"
ORDER BY total_amount_sum DESC
LIMIT 10
```

answer: East Harlem North

# Q6

```SQL
SELECT MAX(tip_amount) as max_tip_amount,
       l2."Zone"
FROM  taxi_data t
LEFT JOIN loc_data l1  ON t."PULocationID" =l1."LocationID"
LEFT JOIN loc_data l2  ON t."DOLocationID" =l2."LocationID"
WHERE CAST(t.lpep_pickup_datetime AS DATE)>='2025-11-01'
      AND  CAST(t.lpep_pickup_datetime AS DATE)<'2025-12-01'
	  AND l1."Zone"='East Harlem North'
GROUP BY l2."Zone"
ORDER BY max_tip_amount DESC

```

Answer:  Yorkville West


