# Lab 4 - Spark Structured Streaming (Wikimedia event stream)

The Wikimedia `recentchange` stream is sent to Kafka by a Python producer, then read and aggregated
by Spark Structured Streaming in a Jupyter notebook. Kafka and Jupyter/PySpark run in Docker Compose,
the producer runs on the host.

```
stream.wikimedia.org --> wikistream_producer.py --> Kafka topic "wikistreams" --> Spark (notebook / job)
       (SSE)               (filters, JSON)              (docker: kafka)             (docker: pyspark_notebook)
```

**Notebook:** [`notebooks/wikistream_pyspark.ipynb`](notebooks/wikistream_pyspark.ipynb). Each query
has a short markdown explanation above it.

## What is covered

| Step | Content | File |
|------|---------|------|
| 1-4. Setup | `data`, `jobs`, `notebooks` folders, Kafka + Jupyter with Docker Compose. Only change to the course file: `PYTHONPATH` (same fix as Lab 1 and 2). | `compose.yaml` |
| 6. Topic | Topic `wikistreams` (1 partition, replication factor 1), the script waits for the creation. | `admin.py` |
| 7. Producer | Events of `recentchange` sent as JSON to `wikistreams`, key = wiki (`server_name`), delivery callback and `flush` at the end. | `wikistream_producer.py` |
| 8-9. Demo | Edits by bots / humans per 1 hour window, console sink (docker logs). | notebook, part 1 |
| 10. Aggregations | Edits, bot share, bytes added / removed, average and max size of an edit; distinct users; most active users; big edits (more than 2 000 bytes). | notebook, parts 2 to 5 |
| 11. Producer filters | Command line options: wiki(s), change types, namespace, page titles, humans only / bots only, duration. | `wikistream_producer.py` |
| 12. Windows | **Tumbling**: 1 minute windows (part 2). **Overlapping**: 10 minutes every 2 minutes (part 3) and 5 minutes every minute (part 4). | notebook |
| Extra | Same tumbling aggregation as a `spark-submit` job writing parquet files, with a checkpoint (restart without losing or reading twice an event). | `jobs/wikistream_job.py` |

## Notebook queries

| Query | Window | Watermark | Output mode | Sink |
|-------|--------|-----------|-------------|------|
| Demo: edits by bot / human | tumbling 1 h | none | complete | console |
| Activity per minute | tumbling 1 min | 2 min | update / complete | console + memory `tumbling_1min` |
| Bots vs humans | sliding 10 min / 2 min | 10 min | update / complete | console + memory `sliding_10min` |
| Most active users | sliding 5 min / 1 min | 5 min | complete | memory `users_5min` (top 10 in SQL) |
| Big edits | none (filter) | none | append | console + memory `big_edits` |

Choices made:

- `event_time` is built with `timestamp_seconds(timestamp)`: the `from_unixtime` of the demo returns a
  string, not a timestamp.
- `size_delta = length.new - length.old`, with `coalesce` because a new page has no `old` size.
- `countDistinct`, `limit` and `orderBy` are not supported on a streaming aggregation (outside
  complete mode): `approx_count_distinct` is used for the users, and the top 10 is computed with SQL
  on the memory table.
- The Kafka connector version is taken from `pyspark.__version__` so it always matches the Spark of
  the image.
- `spark.sql.shuffle.partitions = 4` instead of 200: a micro-batch holds a few hundred events, 200
  tasks per batch would mostly be overhead.

## Producer filters

```bash
python wikistream_producer.py                                           # demo: edits on fr.wikipedia.org, 10 min
python wikistream_producer.py --wiki en.wikipedia.org --minutes 5        # another language
python wikistream_producer.py --wiki fr.wikipedia.org de.wikipedia.org --types edit new
python wikistream_producer.py --humans-only --namespace 0                # articles, human edits only
python wikistream_producer.py --title "Paris" "Lyon" --minutes 60        # follow specific pages
python wikistream_producer.py --bots-only --quiet
```

The filters use `EventStreams.register_filter` from pywikibot: `all` filters must all match,
the `none` filter (`--humans-only`) drops the events where `bot` is true. The course producer also
subscribed to `revision-create`, but those events have no `server_name` / `type` fields so the filter
dropped all of them; only `recentchange` is kept.

## Run it

Requirements: Docker (with Compose), Python 3.

```bash
cd lab04-structured-streaming
docker compose up -d
docker logs pyspark_notebook 2>&1 | grep "token=" | head -1    # Jupyter link

python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
python admin.py
python wikistream_producer.py --minutes 15
```

In Jupyter, open `work/wikistream_pyspark.ipynb` and run all the cells. The console sinks are visible
with `docker logs -f pyspark_notebook`, the memory tables are read at the end of the notebook. Run the
last cell to stop the queries.

Parquet job (in another terminal, while the producer runs):

```bash
docker exec pyspark_notebook bash -c 'spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.13:$(python -c "import pyspark; print(pyspark.__version__)") \
  /home/jovyan/jobs/wikistream_job.py'
```

The results are written to `data/edits_per_minute` (a window is written about 2 minutes after its end,
when the watermark has passed it). Stop everything with `docker compose down`.
