# Module 2 Homework

## Quiz Question 1

1) Within the execution for `Yellow` Taxi data for the year `2020` and month `12`: what is the uncompressed file size (i.e. the output file `yellow_tripdata_2020-12.csv` of the `extract` task)?
- 128.3 MB
- 134.5 MB
- 364.7 MB
- 692.6 MB

**Answer 1: 128.3 MB**


## Quiz Question 2

2) What is the value of the variable `file` when the inputs `taxi` is set to `green`, `year` is set to `2020`, and `month` is set to `04` during execution?
- `{{inputs.taxi}}_tripdata_{{inputs.year}}-{{inputs.month}}.csv` 
- `green_tripdata_2020-04.csv`
- `green_tripdata_04_2020.csv`
- `green_tripdata_2020.csv`

**Answer 2: `green_tripdata_2020-04.csv`**


## Quiz Question 3

3) How many rows are there for the `Yellow` Taxi data for the year 2020?
- 13,537.299
- 24,648,499
- 18,324,219
- 29,430,127

```
SELECT COUNT(*) AS num_rows
FROM `dtc-de-zoomcamp-446201.zoomcamp.yellow_tripdata` 
WHERE filename LIKE '%2020%'
```

**Answer 3: 24,648,499**


## Quiz Question 4

4) How many rows are there for the `Green` Taxi data for the year 2020?
- 5,327,301
- 936,199
- 1,734,051
- 1,342,034

```
SELECT COUNT(*) AS num_rows
FROM `dtc-de-zoomcamp-446201.zoomcamp.green_tripdata` 
WHERE filename LIKE '%2020%'
```

**Answer 4: 1,734,051**


## Quiz Question 5

5) How many rows are there for the `Yellow` Taxi data for the March 2021 CSV file?
- 1,428,092
- 706,911
- 1,925,152
- 2,561,031

```
SQL Query

SELECT COUNT(*) AS num_rows
FROM `dtc-de-zoomcamp-446201.zoomcamp.yellow_tripdata` 
WHERE filename = 'yellow_tripdata_2021-03.csv'
```

**Answer 5: 1,925,152**


## Quiz Question 6

6) How would you configure the timezone to New York in a Schedule trigger?
- Add a `timezone` property set to `EST` in the `Schedule` trigger configuration  
- Add a `timezone` property set to `America/New_York` in the `Schedule` trigger configuration
- Add a `timezone` property set to `UTC-5` in the `Schedule` trigger configuration
- Add a `location` property set to `New_York` in the `Schedule` trigger configuration  

**Answer 6: Add a `timezone` property set to `America/New_York` in the `Schedule` trigger configuration**