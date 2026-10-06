# Fiche 03 - Spark SQL et DataFrames (Lab 2 : taxis de New York)

## Limites des RDD

- Avantage : contrôle bas niveau de l'exécution.
- Inconvénients : code complexe et peu lisible, et les **lambdas sont opaques** pour Spark, qui ne
  peut donc pas les optimiser.

## DataFrame

- Table distribuée en mémoire, avec des **colonnes nommées et typées** (un **schéma**).
- Collection d'objets `Row`.
- Sources : fichiers structurés (Parquet, CSV, JSON), tables Hive, bases SQL (JDBC), RDD.
- API haut niveau, même performance en Python et en Scala (le code est traduit en plan d'exécution
  JVM, alors qu'un RDD Python passe par des workers Python, plus lents).

## Optimiseur Catalyst

Requête (DataFrame ou SQL) puis plan logique, plan logique optimisé (filtres poussés au plus tôt,
colonnes inutiles retirées), plans physiques, choix selon un modèle de coût, génération de code.
C'est pour cela qu'un DataFrame est plus rapide qu'un RDD équivalent.

Voir le plan : `df.explain()`.

## Deux façons d'écrire

```python
from pyspark.sql import functions as F

# API DataFrame (chaînage)
df.filter(F.col("trip_distance") > 0) \
  .groupBy("PULocationID") \
  .agg(F.count("*").alias("trips"), F.avg("fare_amount").alias("avg_fare")) \
  .orderBy(F.desc("trips"))

# SQL
df.createOrReplaceTempView("trips")
spark.sql("""
    SELECT PULocationID, COUNT(*) AS trips, AVG(fare_amount) AS avg_fare
    FROM trips WHERE trip_distance > 0
    GROUP BY PULocationID ORDER BY trips DESC
""")
```

Les deux donnent le même plan d'exécution.

## Fonctions utiles

| Besoin | Fonction |
|--------|----------|
| Colonnes | `select`, `withColumn`, `withColumnRenamed`, `drop` |
| Filtrer | `filter` / `where` |
| Agréger | `groupBy(...).agg(count, sum, avg, min, max)` |
| Trier | `orderBy`, `F.desc` |
| Dates | `F.hour`, `F.dayofweek`, `F.to_date`, `F.unix_timestamp` |
| Conditions | `F.when(...).otherwise(...)` |
| Jointure | `df.join(other, on=..., how="left")` |
| Statistiques | `describe`, `summary`, `corr` |
| Identifiant | `F.monotonically_increasing_id()` (unique, mais dépend du partitionnement) |

## Schéma

- Parquet est **auto-descriptif** : le schéma est dans le fichier.
- CSV / JSON : schéma à fournir (`StructType`) ou à inférer (`inferSchema`, `samplingRatio`).
  En production, toujours un schéma explicite.

## Ce qu'on a fait au Lab 2

Données : taxis jaunes de NYC, janvier 2019 (environ 7,7 millions de lignes, Parquet) + table des zones
(CSV), comparaison avec janvier 2026.

1. **Trajets** : clé unique, passagers (max, moyenne), trajets les plus courts / longs, jours et heures
   les plus chargés, effet de la distance et des passagers sur le pourboire, valeurs aberrantes.
2. **Zones** : jointure avec la table des zones (arrondissement de départ et d'arrivée), activité,
   distance et prix moyens par arrondissement, comparaison 2019 / 2026.
3. **SQL** : trois questions refaites en Spark SQL (dont une avec jointure), mêmes résultats.

Résultats marquants :

- Données sales : 537 départs hors janvier 2019, 7 129 prix négatifs, 55 089 trajets de distance 0,
  un prix de 623 259,86 dollars. Les moyennes sont calculées sur un jeu nettoyé.
- Jour le plus chargé : vendredi 25 janvier (292 499 trajets), le moins chargé : le 1er janvier.
  Pic à 18 h, creux à 4 h.
- Pourboire corrélé à la distance (0,71), pas au nombre de passagers (0,01).
- Environ 91 % des départs à Manhattan.
- Janvier 2026 contre janvier 2019 : -54 % de trajets, +71 % de prix moyen, +22 % de distance.

## A retenir

- DataFrame = données + schéma, optimisé par Catalyst, plus rapide et plus lisible qu'un RDD.
- API DataFrame et SQL sont équivalentes.
- Toujours nettoyer avant de calculer des moyennes (valeurs aberrantes).
- Schéma explicite en production, Parquet pour la performance (format colonne, compressé).
