# Fiche 04 - Stream processing avec Kafka (Lab 3)

## Définition du streaming

1. Jeux de données **non bornés** (ils ne finissent jamais).
2. Traitement **non borné dans le temps**.
3. Faible latence, résultats parfois approximatifs ou spéculatifs.

Deux familles d'outils :

- **Systèmes de messagerie distribués** : publier / consommer des messages, tolérants aux pannes (Kafka).
- **Moteurs de stream processing** : traitement exactly-once, agrégations, fenêtres en event time
  (Spark Structured Streaming, Flink, Kafka Streams).

## Apache Kafka

Plateforme de streaming distribuée qui permet de :

- **publier et s'abonner** à des flux de messages (comme une file de messages),
- **stocker** ces flux de façon durable et tolérante aux pannes,
- **traiter** les flux au fil de l'eau (Kafka Streams).

## Vocabulaire

| Terme | Définition |
|-------|------------|
| Record | Message = (clé, valeur, timestamp). |
| Topic | Catégorie de messages. Un record appartient à un seul topic. |
| Partition | Un topic est découpé en 1 à N partitions. L'**ordre est garanti dans une partition**, pas entre partitions. |
| Offset | Position d'un message dans une partition. |
| Réplication | Chaque partition est copiée 1 à M fois. |
| Rétention | Les messages sont gardés pendant une durée donnée, même après lecture. |
| Broker | Un serveur Kafka. |
| Leader / followers | Par partition, 1 leader gère lectures et écritures, les followers répliquent. |
| Producer | Écrit dans un topic. Il choisit la partition (round-robin, ou hash de la clé). |
| Consumer | Lit un topic à partir de l'offset de son choix. |
| Consumer group | Chaque message est livré à **un seul consumer du groupe**, 1 partition à 1 consumer. |

Kafka est "bête" : pas de routage, c'est le producteur qui choisit topic et partition.

## Pourquoi Kafka est performant

- Performance **indépendante du volume stocké** (écriture séquentielle sur disque).
- Scalabilité des traitements : on ajoute des consumers dans le groupe (jusqu'au nombre de partitions).
- Ordre conservé par partition.
- Plusieurs applications indépendantes lisent le même topic, chacune avec son offset (son groupe).
- Peut servir de stockage de logs (écrit sur disque et répliqué, plutôt CP).

## Problématiques du stream processing

- **Event time** (moment où l'événement s'est produit) contre **processing time** (moment où il est
  traité). Les deux diffèrent à cause de la latence réseau, des pannes, des retards.
- **Fenêtres** : nécessaires pour agréger un flux infini.
- **Watermark** : à un processing time P, on considère que toutes les données d'event time antérieur
  à E ont été reçues.
- **Trigger** : moment où les résultats sont produits.

## Modèle Dataflow (4 questions)

1. **What** : quels résultats ? (agrégation, ex : somme par clé)
2. **Where** en event time ? (fenêtres, ex : fenêtre fixe d'1 h)
3. **When** en processing time ? (triggers : watermark, toutes les 10 min, nombre d'éléments)
4. **How** les résultats successifs d'une fenêtre se combinent ? (discarding, accumulating, ou
   accumulating + retraction)

Moteurs : Flink, Spark, Storm, Kafka Streams, Google Dataflow, Samza. Deux approches : streaming pur
(événement par événement) ou micro-batch (Spark).

## Code Python (confluent_kafka)

```python
from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka import Producer, Consumer

# Création du topic
admin = AdminClient({'bootstrap.servers': 'localhost:9092'})
admin.create_topics([NewTopic('book', num_partitions=1, replication_factor=1)])

# Producteur
producer = Producer({'bootstrap.servers': 'localhost:9092'})
producer.produce('book', value=line.encode('utf-8'))
producer.flush()

# Consommateur
consumer = Consumer({'bootstrap.servers': 'localhost:9092',
                     'group.id': 'book-reader',
                     'auto.offset.reset': 'smallest'})  # depuis le début si pas d'offset
consumer.subscribe(['book'])
msg = consumer.poll(1.0)   # None si rien reçu pendant 1 s
```

Broker local : `docker run -d --name kafka -p 9092:9092 apache/kafka-native:4.1.1` (mode KRaft, sans Zookeeper).

## Ce qu'on a fait au Lab 3

1. Démo du cours : topic `timer`, un producteur envoie l'heure chaque seconde, un consommateur l'affiche.
2. Nouveau topic `book` (1 partition, réplication 1), le script attend la création.
3. Producteur qui lit *Around the World in 80 Days* ligne par ligne (en ignorant l'en-tête et la
   licence Gutenberg) et envoie chaque ligne.
4. Consommateur (groupe `book-reader`) qui nettoie chaque ligne comme au Lab 1 : minuscules,
   ponctuation, stop words. Sorties : `cleaned_book.txt` et `word_counts.txt`.

Résultats : 7 934 lignes envoyées et reçues, 6 069 lignes non vides après nettoyage, 6 634 mots
distincts. Top : `fogg` (646), `passepartout` (424), `mr` (391). Relancer le consommateur ne relit rien :
l'offset du groupe est sauvegardé. Pour relire, il faut changer de `group.id`.

## A retenir

- Topic, partition, offset, consumer group.
- Ordre garanti seulement dans une partition.
- Un message est lu une fois par groupe, et par autant de groupes qu'on veut.
- Event time contre processing time, fenêtres, watermark, trigger.
