# %%
from confluent_kafka.admin import AdminClient, NewTopic

# %%
config =  {
  'bootstrap.servers': 'localhost:9092',
}

admin_client = AdminClient(config)

# %%
topic='book'
futures = admin_client.create_topics(
  [NewTopic(topic, num_partitions=1, replication_factor=1)]
)

# Wait for the creation to finish before listing the topics
for t, f in futures.items():
  try:
    f.result()
    print(f"Topic '{t}' created")
  except Exception as e:
    print(f"Topic '{t}' not created: {e}")

# %%
x = admin_client.list_topics()
for  t in x.topics.keys():
  print(t)

# %%
#admin_client.delete_topics([topic])

# %%
