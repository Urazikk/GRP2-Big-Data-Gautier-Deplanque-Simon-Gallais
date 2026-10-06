# Fiche 05 - Spark Structured Streaming (Lab 4 : Wikimedia)

## Le streaming

- Données générées en continu, par de nombreuses sources en même temps, petits messages (ko).
- Exemples : IoT, bourse, recommandation géolocalisée, consommation électrique, modifications Wikipédia.
- Données **non bornées**, traitement **non borné** dans le temps.
- Spark Structured Streaming : traitement **tolérant aux pannes** et **exactly-once**.

## Batch contre stream

| Batch | Stream |
|-------|--------|
| Données finies, traitées en une fois | Données infinies, traitées au fil de l'eau |
| Latence de minutes à heures | Latence de millisecondes à secondes |
| Résultat final | Résultat mis à jour en continu |

## Modèles de traitement

| Modèle | Latence | Coût |
|--------|---------|------|
| Traditionnel (événement par événement) | Très faible (ms) | Redondance coûteuse pour la tolérance aux pannes |
| **Micro-batch** (Spark) | Environ 100 ms | Tolérance aux pannes simple, permet l'exactly-once |

## Sémantiques de traitement

- **At-most-once** : un message est traité 0 ou 1 fois. Pertes possibles, pas de doublon.
- **At-least-once** : traité 1 fois ou plus. Pas de perte, doublons possibles.
- **Exactly-once** : traité exactement 1 fois. Spark l'obtient avec une source rejouable (Kafka et ses
  offsets) + un **checkpoint** (offsets et état sauvegardés) + un sink idempotent (fichiers).

## Modèle de programmation

Le flux est vu comme une **table qui grandit sans fin** (unbounded table). Chaque nouveau message est
une nouvelle ligne. On écrit une requête comme sur un DataFrame statique, Spark l'exécute de façon
incrémentale à chaque trigger et met à jour la **table de résultat**.

### Modes de sortie

| Mode | Écrit | Usage |
|------|-------|-------|
| **Complete** | Toute la table de résultat à chaque trigger | Agrégations (petites tables) |
| **Append** | Seulement les nouvelles lignes, jamais modifiées ensuite | Filtres, ou agrégations avec watermark (fenêtre écrite une fois finie) |
| **Update** | Seulement les lignes modifiées depuis le dernier trigger | Agrégations, affichage console |

## Opérations

Même syntaxe que les DataFrames statiques :

```python
df.select("device").where("signal > 10")
df.groupBy("deviceType").count()

df.createOrReplaceTempView("updates")
spark.sql("select count(*) from updates")   # renvoie un autre DataFrame streaming
```

**Non supporté** : `distinct`, `take(n)`, `limit`, agrégations enchaînées, certaines jointures,
`orderBy` hors mode complete. Alternatives : `approx_count_distinct`, sink mémoire puis SQL.

## Event time et processing time

- **Event time** : moment où l'événement a eu lieu (champ `timestamp` dans la donnée).
- **Processing time** : moment où Spark le reçoit.
- On agrège en event time pour avoir des résultats corrects même si les données arrivent en retard.

## Fenêtres

```python
from pyspark.sql.functions import window
df.groupBy(window("event_time", "1 minute"))                 # tumbling
df.groupBy(window("event_time", "10 minutes", "2 minutes"))  # sliding (overlapping)
```

- **Tumbling** (fixe) : taille = pas, pas de chevauchement, chaque événement dans **une seule** fenêtre.
- **Sliding** (glissante, chevauchante) : pas plus petit que la taille, chaque événement dans
  **plusieurs** fenêtres (taille / pas). Ex : 10 min tous les 2 min, chaque événement compté 5 fois.
  Effet "moyenne mobile", courbe plus lisse.

## Données en retard et watermark

```python
df.withWatermark("event_time", "2 minutes").groupBy(window("event_time", "1 minute")).count()
```

- Le watermark = event time max vu moins le seuil (ici 2 min).
- Un événement plus ancien que le watermark est **ignoré**.
- Les fenêtres dont la fin est avant le watermark sont **finalisées** et supprimées de l'état.
- Sans watermark, l'état garde toutes les fenêtres et grossit indéfiniment.
- En mode append, une fenêtre n'est écrite qu'une fois le watermark passé.

## Vocabulaire

| Terme | Sens |
|-------|------|
| Event time | Quand l'événement a eu lieu |
| Processing time | Quand on reçoit la donnée |
| Trigger | À quelle fréquence on traite (`processingTime="30 seconds"`) |
| Window | Sur quelle durée on agrège |
| Watermark | À partir de quand une donnée est trop en retard |

## Lecture de Kafka avec Spark

```python
raw = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:19092") \
    .option("subscribe", "wikistreams") \
    .option("startingOffsets", "earliest") \
    .load()                                   # colonnes key, value (binaire), topic, partition, offset...

events = raw.select(from_json(col("value").cast("string"), schema).alias("d")).select("d.*")

query = agg.writeStream.format("console").outputMode("update") \
    .trigger(processingTime="30 seconds").start()
```

- Connecteur à ajouter : `spark.jars.packages = org.apache.spark:spark-sql-kafka-0-10_2.13:<version de Spark>`.
- Schéma obligatoire (`StructType`) : pas d'inférence sur un flux.
- Sinks : `console` (logs du conteneur), `memory` (table interrogeable en SQL, pour les tests),
  `parquet` / fichiers (avec `checkpointLocation`), `kafka`.
- Une requête ne s'arrête jamais seule : `query.stop()`, ou `spark.streams.active` pour toutes les lister.

## Ce qu'on a fait au Lab 4

Architecture : flux Wikimedia `recentchange` (SSE) vers `wikistream_producer.py` (pywikibot,
filtres) vers topic Kafka `wikistreams` vers Spark dans Jupyter. Kafka et Jupyter lancés avec
Docker Compose (réseau commun, Spark lit `kafka:19092`, le producteur sur l'hôte écrit sur `localhost:9092`).

1. **Démo** : nombre d'éditions bots / humains par fenêtre d'1 h, mode complete, console.
   Correction : `timestamp_seconds` au lieu de `from_unixtime` (qui renvoie une chaîne).
2. **Tumbling 1 min** (watermark 2 min) : éditions, part de bots, octets ajoutés / retirés, taille
   moyenne et max d'une édition.
3. **Sliding 10 min / 2 min** : bots contre humains, nombre d'éditions, utilisateurs distincts
   (`approx_count_distinct`), taille moyenne.
4. **Sliding 5 min / 1 min** : utilisateurs les plus actifs, top 10 calculé en SQL sur la table mémoire.
5. **Filtre sans agrégation** (append) : grosses éditions de plus de 2 000 octets.
6. **Filtres côté producteur** (options en ligne de commande) : wiki, type de changement, namespace,
   titres de pages, humains seulement ou bots seulement, durée.
7. **Job spark-submit** : agrégation par minute écrite en Parquet avec checkpoint (reprise sans perte
   ni doublon après un arrêt).

## A retenir

- Un flux = une table infinie, même API que les DataFrames.
- Micro-batch, exactly-once grâce à source rejouable + checkpoint + sink idempotent.
- Complete / append / update.
- Tumbling = 1 fenêtre par événement, sliding = plusieurs.
- Watermark = gestion du retard + nettoyage de l'état.
