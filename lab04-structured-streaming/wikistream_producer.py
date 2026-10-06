# %% Dependencies
import os
import argparse
import socket
import json
import time
from datetime import datetime, timedelta

# pywikibot asks for a user-config.py file, not needed to read the public streams
os.environ.setdefault('PYWIKIBOT_NO_USER_CONFIG', '1')

from confluent_kafka import Producer
from pywikibot.comms.eventstreams import EventStreams

# %% Filters given on the command line (step 11 of the lab)
# Examples:
#   python wikistream_producer.py                                   # demo: edits on fr.wikipedia.org
#   python wikistream_producer.py --wiki en.wikipedia.org --minutes 5
#   python wikistream_producer.py --wiki fr.wikipedia.org de.wikipedia.org --types edit new
#   python wikistream_producer.py --humans-only --namespace 0       # articles edited by humans
#   python wikistream_producer.py --title "Paris" "Lyon"            # follow specific pages
parser = argparse.ArgumentParser(description='Send Wikimedia recent changes to Kafka')
parser.add_argument('--wiki', nargs='+', default=['fr.wikipedia.org'],
                    help='server_name of the wikis to follow')
parser.add_argument('--types', nargs='+', default=['edit'],
                    help='change types to keep: edit, new, log, categorize')
parser.add_argument('--namespace', type=int, nargs='+', default=None,
                    help='namespaces to keep (0 = articles, 1 = talk pages, 2 = user pages...)')
parser.add_argument('--title', nargs='+', default=None,
                    help='only follow these page titles')
bots = parser.add_mutually_exclusive_group()
bots.add_argument('--humans-only', action='store_true', help='drop the bot edits')
bots.add_argument('--bots-only', action='store_true', help='keep only the bot edits')
parser.add_argument('--minutes', type=float, default=10, help='streaming duration')
parser.add_argument('--topic', default='wikistreams')
parser.add_argument('--quiet', action='store_true', help='do not print every event')
# parse_known_args so the cells also run in an interactive window (ipykernel adds its own args)
args, _ = parser.parse_known_args()

# %% Helper Functions
# Serializer function to change message from python dict to json
value_serializer = lambda val: json.dumps(val).encode('utf-8')

sent = 0
failed = 0


def delivery_report(err, msg):
  """Called once per message by producer.poll() / flush()."""
  global sent, failed
  if err is not None:
    failed += 1
    print(f'Delivery failed: {err}')
  else:
    sent += 1


# %% Producer Instantiation
conf = {'bootstrap.servers': 'localhost:9092',
        'client.id': socket.gethostname(),
        'compression.type': 'lz4'
}

producer = Producer(conf)

# %% Create wikistreams query
# Only 'recentchange': the 'revision-create' events of the demo have no server_name / type
# fields, so the filter below dropped all of them anyway.
# No 'since': we read the live events and not a replay from a fixed date
stream = EventStreams(streams=['recentchange'])

# 'all' filters: every condition must match
stream.register_filter(server_name=args.wiki, type=args.types)
if args.namespace is not None:
  stream.register_filter(namespace=args.namespace)
if args.title is not None:
  stream.register_filter(title=args.title)
if args.bots_only:
  stream.register_filter(bot=True)
# 'none' filter: skip the event if it matches
if args.humans_only:
  stream.register_filter(ftype='none', bot=True)

print(f'Following {args.wiki}, types={args.types}, namespace={args.namespace}, '
      f'title={args.title}, humans_only={args.humans_only}, bots_only={args.bots_only}')

# %% Query EventStream
# Run a single query and inspect raw and example formatted output
change = next(stream)
print('Raw Message: ' + str(change))
print(
  '\n'
  'Formatted Message: {type} on page "{title}" by "{user}" at {meta[dt]}.'
  .format(**change)
)

# %% Streaming Query
start_time = datetime.now()
stop_time = start_time + timedelta(minutes=args.minutes)

while datetime.now() < stop_time:
  change = next(stream)
  producer.produce(
    topic=args.topic,
    key=change.get('server_name', '').encode('utf-8'),  # same wiki -> same partition
    value=value_serializer(change),
    callback=delivery_report
  )
  producer.poll(0)  # serve the delivery callbacks
  if not args.quiet:
    length = change.get('length') or {}  # no 'old' size for a new page, no length for a log
    delta = (length.get('new') or 0) - (length.get('old') or 0)
    print(f"{change['meta']['dt']} | {change['server_name']} | bot={change.get('bot')} | "
          f"{delta:+d} | {change['title']}")

# Close producer after run duration
print('\n flushing the last messages and closing the producer')
producer.flush(60)
producer.close()
print(f'{sent} messages sent, {failed} failed')
