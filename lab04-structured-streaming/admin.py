# %%
from confluent_kafka.admin import AdminClient, NewTopic

# %%
config = {
  'bootstrap.servers': 'localhost:9092',
}

admin_client = AdminClient(config)

# %% Same as the Kafka lab, only the topic name changes
topic = 'wikistreams'
futures = admin_client.create_topics(
  [NewTopic(topic, num_partitions=1, replication_factor=1)]
)

# Wait for the creation, otherwise list_topics can run before the topic exists
for name, f in futures.items():
  try:
    f.result()
    print(f"Topic '{name}' created")
  except Exception as e:  # e.g. TOPIC_ALREADY_EXISTS when the script is run twice
    print(f"Topic '{name}': {e}")

# %%
x = admin_client.list_topics()
for t in x.topics.keys():
  print(t)

# %%
#admin_client.delete_topics([topic])
