# %%
import re
from collections import Counter
from confluent_kafka import Consumer

# %%
conf = {'bootstrap.servers': 'localhost:9092',
        'group.id': 'book-reader',
        'auto.offset.reset': 'smallest'}

consumer = Consumer(conf)

# %%
topic='book'
consumer.subscribe([topic])

# %% Text cleaning, inspired by the word count lab
STOP_WORDS = {
  'a', 'about', 'above', 'after', 'again', 'against', 'all', 'am', 'an',
  'and', 'any', 'are', 'as', 'at', 'be', 'because', 'been', 'before',
  'being', 'below', 'between', 'both', 'but', 'by', 'can', 'could', 'did',
  'do', 'does', 'doing', 'down', 'during', 'each', 'few', 'for', 'from',
  'further', 'had', 'has', 'have', 'having', 'he', 'her', 'here', 'hers',
  'herself', 'him', 'himself', 'his', 'how', 'i', 'if', 'in', 'into', 'is',
  'it', 'its', 'itself', 'just', 'me', 'more', 'most', 'my', 'myself', 'no',
  'nor', 'not', 'now', 'of', 'off', 'on', 'once', 'only', 'or', 'other',
  'our', 'ours', 'ourselves', 'out', 'over', 'own', 'same', 'she', 'should',
  'so', 'some', 'such', 'than', 'that', 'the', 'their', 'theirs', 'them',
  'themselves', 'then', 'there', 'these', 'they', 'this', 'those', 'through',
  'to', 'too', 'under', 'until', 'up', 'very', 'was', 'we', 'were', 'what',
  'when', 'where', 'which', 'while', 'who', 'whom', 'why', 'will', 'with',
  'would', 'you', 'your', 'yours', 'yourself', 'yourselves', 'said', 'upon',
  's', 't', 'd', 'll', 'm', 've', 're',
}


def clean_line(line):
  """Lower case, remove punctuation/blanks, remove stop words."""
  words = re.findall(r'[a-z]+', line.lower())  # lower + strip punctuation
  return [w for w in words if w not in STOP_WORDS]


# %%
MAX_EMPTY_POLLS = 10  # Ends after ~10 seconds of silence
MAX_ERRORS = 5        # Ends after 5 consecutive errors
empty_polls = 0
error_count = 0

word_counts = Counter()
received = 0

with open('cleaned_book.txt', 'w', encoding='utf-8') as out:
  while True:
    msg = consumer.poll(1.0)

    if msg is None:
      empty_polls += 1
      if empty_polls >= MAX_EMPTY_POLLS:
        print("Closing: No new messages received.")
        break
      continue

    if msg.error():
      error_count += 1
      print(f"Consumer error: {msg.error()}")
      if error_count >= MAX_ERRORS:
        print("Closing: Too many consecutive errors.")
        break
      continue

    empty_polls = 0
    error_count = 0
    received += 1

    words = clean_line(msg.value().decode('utf-8'))
    if not words:  # blank line or only stop words
      continue
    word_counts.update(words)
    out.write(' '.join(words) + '\n')

consumer.close()

# %% Word frequencies, sorted descending
with open('word_counts.txt', 'w', encoding='utf-8') as out:
  for word, count in word_counts.most_common():
    out.write(f"{word}\t{count}\n")

print(f"{received} messages received, {len(word_counts)} distinct words")
print("Top 10:", word_counts.most_common(10))
