# %%
import socket
from confluent_kafka import Producer

# %%
conf = {'bootstrap.servers': 'localhost:9092',
        'client.id': socket.gethostname()}

producer = Producer(conf)

# %%
topic='book'
book_path='around_the_world_in_80_days.txt'

# %% Read the book line by line and send each line to the topic
# Only the text between the Gutenberg START/END markers is sent
in_book = False
sent = 0

with open(book_path, encoding='utf-8') as book:
  for line in book:
    if line.startswith('*** START OF'):
      in_book = True
      continue
    if line.startswith('*** END OF'):
      break
    if not in_book:
      continue

    producer.produce(topic=topic, value=line.rstrip('\n'))
    producer.poll(0)  # serve delivery callbacks, frees the local queue
    sent += 1

producer.flush()
print(f"{sent} lines sent to topic '{topic}'")
