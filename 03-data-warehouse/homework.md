# Module 3 Homework

For this homework we will be using the Yellow Taxi Trip Records for **January 2024 - June 2024 NOT the entire year of data**

Parquet Files from the New York
City Taxi Data found here: https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page

If you are using orchestration such as Kestra, Mage, Airflow or Prefect etc. do not load the data into Big Query using the orchestrator. Stop with loading the files into a bucket.

**Load Script:** You can manually download the parquet files and upload them to your GCS Bucket or you can use the linked script [here](./load_yellow_taxi_data.py).
You will simply need to generate a Service Account with GCS Admin Priveleges or be authenticated with the Google SDK and update the bucket name in the script to the name of your bucket. Nothing is fool proof so make sure that all 6 files show in your GCS Bucket before begining.

**NOTE:** You will need to use the PARQUET option files when creating an External Table.

## BIG QUERY SETUP
**Create an external table using the Yellow Taxi Trip Records.**
SQL Query
```
CREATE OR REPLACE EXTERNAL TABLE `dtc-de-zoomcamp-446201.zoomcamp.external_yellow_tripdata`
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://yellow_taxi_trips_2024/yellow_tripdata_2024-*.parquet']
);
```

**Create a (regular/materialized) table in BQ using the Yellow Taxi Trip Records (do not partition or cluster this table).**
SQL Query
```
CREATE OR REPLACE TABLE `dtc-de-zoomcamp-446201.zoomcamp.yellow_tripdata_non_partitoned` AS
SELECT * FROM `dtc-de-zoomcamp-446201.zoomcamp.external_yellow_tripdata`;
```


## Question 1:
Question 1: What is count of records for the 2024 Yellow Taxi Data?
- 65,623
- 840,402
- 20,332,093
- 85,431,289

**Answer 1: 20,332,093**

![answer1](03-data-warehouse/images/Answer1.pngraw=true)


## Question 2:
Write a query to count the distinct number of PULocationIDs for the entire dataset on both the tables.</br> 
What is the **estimated amount** of data that will be read when this query is executed on the External Table and the Table?

- 18.82 MB for the External Table and 47.60 MB for the Materialized Table
- 0 MB for the External Table and 155.12 MB for the Materialized Table
- 2.14 GB for the External Table and 0MB for the Materialized Table
- 0 MB for the External Table and 0MB for the Materialized Table

**Answer 2: 0 MB for the External Table and 155.12 MB for the Materialized Table**
![answer_2_bq_materialized] (/images/Answer2a.png)
![answer_2_bq_external] (/images/Answer2b.png)

## Question 3:
Write a query to retrieve the PULocationID from the table (not the external table) in BigQuery. Now write a query to retrieve the PULocationID and DOLocationID on the same table. Why are the estimated number of Bytes different?
- BigQuery is a columnar database, and it only scans the specific columns requested in the query. Querying two columns (PULocationID, DOLocationID) requires reading more data than querying one column (PULocationID), leading to a higher estimated number of bytes processed.
- BigQuery duplicates data across multiple storage partitions, so selecting two columns instead of one requires scanning the table twice, doubling the estimated bytes processed.
- BigQuery automatically caches the first queried column, so adding a second column increases processing time but does not affect the estimated bytes scanned.
- When selecting multiple columns, BigQuery performs an implicit join operation between them, increasing the estimated bytes processed

**Answer 3: BigQuery is a columnar database, and it only scans the specific columns requested in the query. Querying two columns (PULocationID, DOLocationID) requires reading more data than querying one column (PULocationID), leading to a higher estimated number of bytes processed.**
![answer_3_bq_1col] (/images/Answer3a.png)
![answer_3_bq_2col] (/images/Answer3b.png)

## Question 4:
How many records have a fare_amount of 0?
- 128,210
- 546,578
- 20,188,016
- 8,333

**Answer 4: 8,333**
SQL Query
```
SELECT COUNT(*)
FROM `dtc-de-zoomcamp-446201.zoomcamp.external_yellow_tripdata`
WHERE fare_amount = 0
```


## Question 5:
What is the best strategy to make an optimized table in Big Query if your query will always filter based on tpep_dropoff_timedate and order the results by VendorID (Create a new table with this strategy)
- Partition by tpep_dropoff_timedate and Cluster on VendorID
- Cluster on by tpep_dropoff_timedate and Cluster on VendorID
- Cluster on tpep_dropoff_timedate Partition by VendorID
- Partition by tpep_dropoff_timedate and Partition by VendorID

**Answer 5: Partition by tpep_dropoff_timedate and Cluster on VendorID**
SQL Query
```
CREATE OR REPLACE TABLE `dtc-de-zoomcamp-446201.zoomcamp.yellow_tripdata_partitoned_clustered`
PARTITION BY DATE(tpep_dropoff_datetime)
CLUSTER BY VendorID AS
SELECT * FROM `dtc-de-zoomcamp-446201.zoomcamp.external_yellow_tripdata`;
```


## Question 6:
Write a query to retrieve the distinct VendorIDs between tpep_dropoff_timedate
03/01/2024 and 03/15/2024 (inclusive)</br>

Use the materialized table you created earlier in your from clause and note the estimated bytes. Now change the table in the from clause to the partitioned table you created for question 4 and note the estimated bytes processed. What are these values? </br>

Choose the answer which most closely matches.</br> 

- 12.47 MB for non-partitioned table and 326.42 MB for the partitioned table
- 310.24 MB for non-partitioned table and 26.84 MB for the partitioned table
- 5.87 MB for non-partitioned table and 0 MB for the partitioned table
- 310.31 MB for non-partitioned table and 285.64 MB for the partitioned table

**Answer 6: 310.24 MB for non-partitioned table and 26.84 MB for the partitioned table**
SQL Queries
```
SELECT COUNT(DISTINCT VendorID)
FROM `dtc-de-zoomcamp-446201.zoomcamp.yellow_tripdata_partitoned_clustered`
WHERE DATE(tpep_dropoff_datetime) BETWEEN '2024-03-01' AND '2024-03-15'

SELECT COUNT(DISTINCT VendorID)
FROM `dtc-de-zoomcamp-446201.zoomcamp.yellow_tripdata_non_partitoned`
WHERE DATE(tpep_dropoff_datetime) BETWEEN '2024-03-01' AND '2024-03-15'
```
![answer_6a_bq] (/images/Answer6a.png)
![answer_6b_bq] (/images/Answer6b.png)

## Question 7: 
Where is the data stored in the External Table you created?

- Big Query
- Container Registry
- GCP Bucket
- Big Table

**Answer 7: GCP Bucket**


## Question 8:
It is best practice in Big Query to always cluster your data:
- True
- False

**Answer 8: False**


## (Bonus: Not worth points) Question 9:
No Points: Write a `SELECT count(*)` query FROM the materialized table you created. How many bytes does it estimate will be read? Why?

**Answer 9: 0 bytes is estimated to be read because the materialized table is stored in BigQuery. The number of rows is in the metadata (Details > Storage info > Number of rows) so BigQuery does not actually need to run the query.**
