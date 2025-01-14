# Module 1 Homework: Docker & SQL

## Question 1. Understanding docker first run 

Run docker with the `python:3.12.8` image in an interactive mode, use the entrypoint `bash`.

What's the version of `pip` in the image?

### Answer 1. Understanding docker first run

**CLI command: `docker run -it python:3.12.8 /bin/bash`**

**Answer: 24.3.1**


## Question 2. Understanding Docker networking and docker-compose

Given the following `docker-compose.yaml`, what is the `hostname` and `port` that **pgadmin** should use to connect to the postgres database?

```yaml
services:
  db:
    container_name: postgres
    image: postgres:17-alpine
    environment:
      POSTGRES_USER: 'postgres'
      POSTGRES_PASSWORD: 'postgres'
      POSTGRES_DB: 'ny_taxi'
    ports:
      - '5433:5432'
    volumes:
      - vol-pgdata:/var/lib/postgresql/data

  pgadmin:
    container_name: pgadmin
    image: dpage/pgadmin4:latest
    environment:
      PGADMIN_DEFAULT_EMAIL: "pgadmin@pgadmin.com"
      PGADMIN_DEFAULT_PASSWORD: "pgadmin"
    ports:
      - "8080:80"
    volumes:
      - vol-pgadmin_data:/var/lib/pgadmin  

volumes:
  vol-pgdata:
    name: vol-pgdata
  vol-pgadmin_data:
    name: vol-pgadmin_data
```

### Answer 2. Understanding Docker networking and docker-compose

**Answer: db:5432**


##  Prepare Postgres

Run Postgres and load data as shown in the videos
We'll use the green taxi trips from October 2019:

```bash
wget https://github.com/DataTalksClub/nyc-tlc-data/releases/download/green/green_tripdata_2019-10.csv.gz
```

You will also need the dataset with zones:

```bash
wget https://github.com/DataTalksClub/nyc-tlc-data/releases/download/misc/taxi_zone_lookup.csv
```

Download this data and put it into Postgres.

You can use the code from the course. It's up to you whether
you want to use Jupyter or a python script.

## Question 3. Trip Segmentation Count

During the period of October 1st 2019 (inclusive) and November 1st 2019 (exclusive), how many trips, **respectively**, happened:
1. Up to 1 mile
2. In between 1 (exclusive) and 3 miles (inclusive),
3. In between 3 (exclusive) and 7 miles (inclusive),
4. In between 7 (exclusive) and 10 miles (inclusive),
5. Over 10 miles 

### Answer 3. Trip Segmentation Count

**SQL Query**
```
SELECT
	COUNT(CASE WHEN g.trip_distance <= 1 THEN 1 END) as under_1,
	COUNT(CASE WHEN g.trip_distance > 1 AND g.trip_distance <= 3 THEN 1 END) as btw_1_and_3,
	COUNT(CASE WHEN g.trip_distance > 3 AND g.trip_distance <= 7 THEN 1 END) as btw_3_and_7,
	COUNT(CASE WHEN g.trip_distance > 7 AND g.trip_distance <= 10 THEN 1 END) as btw_7_and_10,
	COUNT(CASE WHEN g.trip_distance > 10 THEN 1 END) as over_10
FROM green_taxi_trips g
WHERE g.lpep_pickup_datetime::DATE BETWEEN '2019-10-01' AND '2019-10-31'
```

**Answer: 104,838;  199,013;  109,645;  27,688;  35,202**


## Question 4. Longest trip for each day

Which was the pick up day with the longest trip distance?
Use the pick up time for your calculations.

Tip: For every day, we only care about one single trip with the longest distance. 

- 2019-10-11
- 2019-10-24
- 2019-10-26
- 2019-10-31

### Answer 4. Longest trip for each day

**SQL Query**
```
SELECT
	g.lpep_pickup_datetime::DATE as date,
	MAX(g.trip_distance) as longest_distance
FROM green_taxi_trips g
WHERE g.lpep_pickup_datetime::DATE IN ('2019-10-11','2019-10-24', '2019-10-26', '2019-10-31')
GROUP BY 1
ORDER BY 2 DESC
```

**Answer: 2019-10-31**


## Question 5. Three biggest pickup zones

Which were the top pickup locations with over 13,000 in
`total_amount` (across all trips) for 2019-10-18?

Consider only `lpep_pickup_datetime` when filtering by date.

### Answer 5. Three biggest pickup zones

**SQL Query**
```
WITH base_table AS (

	SELECT *
	FROM green_taxi_trips g
	INNER JOIN taxi_zones z ON z."LocationID" = g."PULocationID"
	WHERE g.lpep_pickup_datetime::DATE = '2019-10-18'
)

SELECT
	bt."Zone",
	SUM(bt."total_amount")
FROM base_table bt
GROUP BY 1
HAVING SUM(bt."total_amount") > 13000
```

**Answer: East Harlem North, East Harlem South, Morningside Heights**


## Question 6. Largest tip

For the passengers picked up in Ocrober 2019 in the zone
name "East Harlem North" which was the drop off zone that had
the largest tip?

Note: it's `tip` , not `trip`

We need the name of the zone, not the ID.

### Answer 6. Largest tip

**SQL Query**
```
WITH base_table AS (

	SELECT
		g."index",
		g."tip_amount",
		zpu."Zone" as pickup_zone,
		zdo."Zone" as dropoff_zone
	FROM green_taxi_trips g
	INNER JOIN taxi_zones zpu ON zpu."LocationID" = g."PULocationID"
	INNER JOIN taxi_zones zdo ON zdo."LocationID" = g."DOLocationID"
	WHERE zpu."Zone" = 'East Harlem North'

)

SELECT
	bt."dropoff_zone",
	MAX(bt."tip_amount")
FROM base_table bt
GROUP BY 1
ORDER BY 2 desc
```

**Answer: JFK Airport**


## Terraform

In this section homework we'll prepare the environment by creating resources in GCP with Terraform.

In your VM on GCP/Laptop/GitHub Codespace install Terraform. 
Copy the files from the course repo
[here](../../../01-docker-terraform/1_terraform_gcp/terraform) to your VM/Laptop/GitHub Codespace.

Modify the files as necessary to create a GCP Bucket and Big Query Dataset.


## Question 7. Terraform Workflow

Which of the following sequences, **respectively**, describes the workflow for: 
1. Downloading the provider plugins and setting up backend,
2. Generating proposed changes and auto-executing the plan
3. Remove all resources managed by terraform`

### Answer 7. Terraform Workflow

**Answer: terraform init, terraform apply -auto-aprove, terraform destroy**
