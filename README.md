# docker-workshop

## Question 1 - Cheking PIP version
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

## Question 2 - hostname and port

```
db:5432
```

## Question 3 - Query

```sql
SELECT 
	COUNT(1)
FROM
	green_tripdata gt
WHERE 
	(gt.lpep_pickup_datetime >= '2025-11-01' AND gt.lpep_pickup_datetime < '2025-12-01') AND
	gt.trip_distance <= 1;
```

## Question 4 - Query

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

## Question 5 - Query

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

## Question 6 - Query

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

## Question 7 - Concepts order

terraform init, terraform apply -auto-approve, terraform destroy

	
