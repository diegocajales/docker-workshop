# docker-workshop

## Homework 1: Docker, SQL and Terraform

### Question 1 - Cheking PIP version

First we create the image

```
docker run -it \               
 --rm \
--entrypoint=bash \
python:3.13
```

Then we check the version

```
pip -V

pip 25.3 from /usr/local/lib/python3.13/site-packages/pip (python 3.13)
```

### Question 2 - Hostname and port that pgadmin should use to connect to the postgres database

```
db:5432
```

### Question 3 - Query: For the trips in November 2025, how many trips had a trip_distance of less than or equal to 1 mile

```sql
SELECT 
	COUNT(1)
FROM
	green_tripdata gt
WHERE 
	(gt.lpep_pickup_datetime >= '2025-11-01' AND gt.lpep_pickup_datetime < '2025-12-01') AND
	gt.trip_distance <= 1;
```

### Question 4 - Query: Which was the pick up day with the longest trip distance? Only consider trips with trip_distance less than 100 miles

```sql
SELECT 
	gt.lpep_pickup_datetime,
	gt.trip_distance
FROM
	green_tripdata gt
WHERE
	gt.trip_distance < 100
ORDER BY
	gt.trip_distance desc
LIMIT 1;
```

### Question 5 - Query: Which was the pickup zone with the largest total_amount (sum of all trips) on November 18th, 2025?

```sql
SELECT 
	tzl."Zone",
	sum(gt.total_amount)
FROM
	green_tripdata gt
LEFT JOIN
	taxi_zone_lookup tzl ON gt.PULocationID = tzl.LocationID 
WHERE
	gt.lpep_pickup_datetime >= '2025-11-18 00:00:00' AND gt.lpep_pickup_datetime < '2025-11-19 00:00:00'
GROUP BY
	tzl."Zone"
ORDER BY
	SUM(gt.total_amount) DESC
LIMIT 1;
```

### Question 6 - Query: For the passengers picked up in the zone named "East Harlem North" in November 2025, which was the drop off zone that had the largest tip?

```sql
SELECT 
	tzl_do."Zone",
	gt.tip_amount 
FROM
	green_tripdata gt
LEFT JOIN
	taxi_zone_lookup tzl_pu ON gt.PULocationID = tzl_pu.LocationID 
LEFT JOIN
	taxi_zone_lookup tzl_do ON gt.DOLocationID = tzl_do.LocationID 
WHERE
	(gt.lpep_pickup_datetime >= '2025-11-01' AND gt.lpep_pickup_datetime <= '2025-11-31') AND
	tzl_pu."Zone" = "East Harlem North"
ORDER BY
	gt.tip_amount DESC
LIMIT 1;
```

### Question 7 - Concepts order for: 1) Downloading plugins and setting up backend, 2) Generating and executing changes, 3) Removing all resources?

```
terraform init, terraform apply -auto-approve, terraform destroy
```

### Learning in public

> [Link to post](https://www.linkedin.com/posts/diego-alonso-cajales-albornoz_github-diegocajalesdocker-workshop-activity-7421686347641319424-P30C?utm_source=share&utm_medium=member_desktop&rcm=ACoAABcVuFcBWjLCkqaDiYk8Ieiegt_4omF8LZg)
