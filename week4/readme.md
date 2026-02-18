Here are the SQL queries for Q4 and Q5.
I also added the stg_fhv_tripdata.sql for question 6.

# Q4

```SQL
SELECT pickup_zone, 
       SUM(revenue_monthly_total_amount) as sum_revenue_monthly_total_amount,
       service_type
FROM prod.fct_monthly_zone_revenue
WHERE  YEAR(revenue_month)=2020 and service_type='Green'
GROUP BY pickup_zone,service_type
ORDER BY sum_revenue_monthly_total_amount DESC
LIMIT 1
```

# Q5

```SQL
SELECT SUM(total_monthly_trips)
FROM prod.fct_monthly_zone_revenue
WHERE  YEAR(revenue_month)=2019 AND MONTH(revenue_month)=10 and service_type='Green'
```