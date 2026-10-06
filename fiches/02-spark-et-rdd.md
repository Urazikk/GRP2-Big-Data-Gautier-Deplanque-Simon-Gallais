# Fiche 02 - Spark et RDD (Lab 1 : word count)

## Apache Spark

- Moteur de calcul **distribué** et **en mémoire**, généraliste, open source (Apache).
- Écrit en **Scala** (tourne dans la JVM), utilisable en Scala, Python (PySpark), R, SQL, Java.
- Lié à l'écosystème Hadoop (lit HDFS, tourne sur YARN).
- Cas d'usage : ETL sur gros volumes, streaming quasi temps réel, graphes, ML.
- Modules : Spark Core (RDD), Spark SQL (DataFrames), Structured Streaming, MLlib, GraphX.

## Fonctionnement interne

Gestionnaires de cluster possibles : YARN, Mesos, Kubernetes, Spark standalone.

À la soumission d'un programme :

1. Spark demande des ressources pour créer le **driver** et les **executors**.
2. Le code est découpé en **tâches**.
3. Le driver envoie les tâches aux executors.
4. Les executors renvoient leur statut au driver.

## Transformations et actions

| Transformations (lazy) | Actions (déclenchent le calcul) |
|------------------------|---------------------------------|
| `map`, `flatMap`, `filter`, `groupBy`, `reduceByKey`, `join`, `orderBy`, `select` | `count`, `collect`, `take(n)`, `show`, `save` |

**Lazy evaluation** : les transformations ne font rien tant qu'aucune action n'est appelée. Spark
construit alors un **DAG** (graphe orienté acyclique) et l'optimise.

## RDD (Resilient Distributed Dataset)

- Collection d'éléments **tolérante aux pannes**, **partitionnée** sur les nœuds du cluster.
- **Immuable** : chaque transformation crée un nouveau RDD.
- Tolérance aux pannes grâce au **lineage** : Spark sait recalculer une partition perdue.
- Peut être **persisté** en mémoire (`cache()` / `persist()`) pour éviter de recalculer.
- Partitions : 1 partition = 1 bloc HDFS (128 Mo) = **1 tâche**. Par défaut 1 partition par cœur.

## DAG, stages et shuffle

- Une action crée un **job**, découpé en **stages**, chaque stage contient une tâche par partition.
- Une stage se termine à chaque **shuffle** (redistribution des données entre nœuds).
- **Transformation narrow** : chaque partition de sortie dépend d'une seule partition d'entrée
  (`map`, `filter`). Pas de shuffle.
- **Transformation wide** : une partition de sortie dépend de plusieurs partitions (`groupByKey`,
  `reduceByKey`, `join`). Shuffle, donc coûteux.

`reduceByKey` est préférable à `groupByKey` : il agrège d'abord localement sur chaque partition
(combiner) avant le shuffle, donc moins de données échangées.

## Fonctions clés en PySpark

```python
rdd = sc.textFile("book.txt")
counts = rdd.flatMap(lambda line: line.split()) \
            .map(lambda w: (w.lower(), 1)) \
            .filter(lambda kv: kv[0] not in stop_words) \
            .reduceByKey(lambda a, b: a + b) \
            .sortBy(lambda kv: kv[1], ascending=False)
counts.take(10)
```

- `map` : 1 élément en entrée donne 1 élément en sortie.
- `flatMap` : 1 élément donne 0 à n éléments (une ligne devient une liste de mots aplatie).
- `reduceByKey` : agrège les valeurs par clé sur des paires `(clé, valeur)`.
- `lambda` : fonction anonyme passée aux transformations.

## Ce qu'on a fait au Lab 1

Livre : *Around the World in 80 Days* (Gutenberg #103), puis comparaison avec la version française.

1. Word count pas à pas : comptage, minuscules, stop words, tri alphabétique, tri par fréquence,
   suppression de la ponctuation. Tout est ensuite chaîné dans `word_count_pipeline`.
2. Commentaires ligne par ligne d'un exemple `map` / `reduceByKey` (âge moyen).
3. Mesure du temps (warm-up + médiane sur 5 exécutions) en changeant l'ordre des opérations.
4. Comparaison EN / FR : mots uniques, top 20, mots communs.

Résultats : top mots EN `fogg` (602), `passepartout` (404), `mr` (391). 7 292 mots uniques en anglais
contre 9 800 en français. Mots communs aux deux top 20 : les noms propres (`fogg`, `passepartout`,
`phileas`, `fix`, `aouda`) et `mr`. Filtrer avant ou après le comptage change peu ici car
`reduceByKey` combine déjà localement et le fichier est petit.

## Environnement

Image Docker `quay.io/jupyter/pyspark-notebook` (Spark + Jupyter), volume monté sur
`/home/jovyan/work`, lien Jupyter récupéré avec `docker logs pyspark_notebook | grep token=`.
Spark UI sur le port 4040.

## A retenir

- Transformations lazy, actions qui déclenchent le DAG.
- Narrow sans shuffle, wide avec shuffle.
- `reduceByKey` plutôt que `groupByKey`.
- RDD = bas niveau, flexible mais les lambdas sont opaques pour l'optimiseur.
