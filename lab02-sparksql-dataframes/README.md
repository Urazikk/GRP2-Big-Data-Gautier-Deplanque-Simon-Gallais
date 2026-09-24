# Lab 2 - Structured data analysis with DataFrames and Spark SQL

Exploratory analysis of the NYC yellow taxi trips (January 2019, about 7.7 million rows) with
PySpark DataFrames and Spark SQL, enriched with the taxi zone lookup and compared with January 2026.

**Notebook:** [`lab_sparksql_and_dataframes.ipynb`](lab_sparksql_and_dataframes.ipynb) - already
executed, outputs are visible directly on GitHub. Every result has a short markdown explanation
above it.

## What is covered

| Part | Content |
|------|---------|
| 1. Trips | Unique trip key, highest passenger count, average passengers, shortest / longest trips (distance and time), busiest / slowest day, hour and day of the week, effect of distance and passengers on the tip, highest "extra" charge, outliers. |
| 2. Zones | Join with the zone lookup (pickup and dropoff borough): pickups / dropoffs, busy hours and days, average distance and fare, highest / lowest fares by borough. Comparison with the most recent January (2026), overall and by borough. |
| 3. SQL | Three questions redone in pure Spark SQL (one with a join): average trips per weekday, highest "extra" charge, trips / distance / fare by borough. Same results as the DataFrame version. |

Not done from the "where to go from here" section: visualizations, full year 2019 and seasons.

## Main results

- The raw file is dirty: 537 pickups outside January 2019, 7,129 negative fares, 55,089 trips with a
  distance of 0, a fare of 623,259.86 dollars and trips lasting 30 days. Averages are computed on a
  cleaned dataset (same rules for 2019 and 2026).
- Busiest day: Friday January 25 (292,499 trips); slowest: January 1 (189,432 trips), then the
  MLK Day holiday (January 21). Peak hour is 6 pm, slowest is 4 am. Friday is the busiest weekday
  on average and Sunday the slowest.
- The tip is correlated with the distance (0.71) but not with the number of passengers (0.01).
- The highest "extra" charge (535.38 dollars) belongs to a trip with an erroneous fare of 355,676.98 dollars.
- About 91% of the pickups are in Manhattan (2.24 miles and 10.63 dollars on average); trips from Queens,
  Staten Island and EWR are much longer and more expensive.
- January 2026 vs January 2019: -54% trips (7.63M to 3.52M), +71% average fare, +22% average distance;
  Brooklyn +64% and the Bronx x2 in trips, Manhattan -56%.

## Run it

Requirements: Docker.

```bash
cd lab02-sparksql-dataframes
./run_docker.sh          # starts the container and prints the Jupyter link with its token
```

Open the link and run the notebook from the `work/` folder. It downloads the datasets itself (parquet
files and the zone lookup); they are not stored in the repository. Stop the container with
`docker stop pyspark_notebook`.

Note: the `trip_id` values quoted in the comments come from a single-partition run, so they differ
if the notebook is executed again in Docker (the ids are unique but depend on the partitioning).
