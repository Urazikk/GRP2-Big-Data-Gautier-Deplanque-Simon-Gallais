# Lab 3 - Kafka overview (producer / topic / consumer)

A Kafka broker in Docker, a topic, a Python producer and a Python consumer with the
`confluent_kafka` library. First the demo given in the course (a "timer" topic), then a pipeline
that streams *Around the World in 80 Days* (Project Gutenberg #103) line by line through Kafka and
cleans the text on the consumer side.

## What is covered

| Step | Content | File |
|------|---------|------|
| Demo | Topic `timer`, producer sending "The time is now HH:MM:SS" every second, consumer printing the messages. | `admin.py`, `producer.py`, `consumer.py` |
| 2. New topic | Topic `book` (1 partition, replication factor 1). The script waits for the creation before listing the topics. | `admin_book.py` |
| 3. Book | *Around the World in 80 Days*, same book as Lab 1. | downloaded, not stored |
| 4. Producer | Reads the book line by line and sends each line to `book`. The Gutenberg header and license (outside the `*** START` / `*** END` markers) are skipped. | `producer_book.py` |
| 5. Consumer | Subscribes to `book` with its own consumer group (`book-reader`), reads from the beginning of the topic. | `consumer_book.py` |
| 6. Cleaning | Same steps as the Lab 1 word count: lower case, punctuation and blank tokens removed, stop words removed. Cleaned lines go to `cleaned_book.txt`, word frequencies (sorted descending) to `word_counts.txt`. | `consumer_book.py` |

## Main results

- Demo: every message sent by the producer is received by the consumer, in order; the consumer
  stops after 10 seconds without new messages.
- Book: 7 934 lines sent, 7 934 messages received. After cleaning, 6 069 non-empty lines and
  6 634 distinct words.
- Top words: `fogg` (646), `passepartout` (424), `mr` (391), `phileas` (256), `fix` (255),
  `one` (172), `aouda` (136), `master` (129), `time` (126), `train` (119). Close to Lab 1, the
  small differences come from the tokenization (here a regex keeping only letters, so
  `Fogg's` gives `fogg`).
- The consumer group keeps its offset: running `consumer_book.py` a second time reads nothing new.
  To read the topic again, change the `group.id`.

## Run it

Requirements: Docker, Python 3.

```bash
cd lab03-kafka
./run_kafka.sh                       # Kafka broker on localhost:9092
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
curl -L -o around_the_world_in_80_days.txt https://www.gutenberg.org/cache/epub/103/pg103.txt
```

Demo (consumer in a second terminal, with the environment activated):

```bash
python admin.py
python producer.py        # runs for 5 minutes
python consumer.py
```

Book pipeline:

```bash
python admin_book.py
python producer_book.py
python consumer_book.py   # stops after 10 s without new messages
```

Stop the broker with `docker stop kafka`.
