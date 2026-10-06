# Fiche 01 - Introduction au Big Data

Cours 1 (pas de lab associé). Objectif : poser le vocabulaire et le contexte des systèmes distribués.

## Système d'information

Un SI sert à **collecter**, **traiter**, **stocker** et **distribuer** la donnée.

## Système distribué

Groupe d'ordinateurs qui apparaît comme **un seul système cohérent** pour l'utilisateur.

| Avantages | Inconvénients |
|-----------|---------------|
| Scalabilité | Plus dur à concevoir |
| Disponibilité | Plus dur à utiliser |
| Flexibilité | Plus dur à maintenir |

## Théorème CAP

Un système de stockage distribué ne peut garantir que **2 propriétés sur 3** :

- **C**onsistency : chaque nœud renvoie la valeur la plus récente (ou une erreur).
- **A**vailability : chaque requête reçoit une réponse rapide, pas forcément la plus récente.
- **P**artition tolerance : le système continue de fonctionner si des nœuds sont déconnectés.

En pratique la partition réseau arrive toujours, donc le vrai choix est **CP** (ex : HBase, Kafka pour
le stockage de logs) ou **AP** (ex : Cassandra).

## Scalabilité

- **Verticale** (scale up) : machine plus puissante (RAM, CPU, disque). Limitée et chère.
- **Horizontale** (scale out) : plus de machines. C'est le modèle des systèmes distribués.

## Types de données

| Type | Exemples |
|------|----------|
| Structurée | Tables SQL, CSV, Excel |
| Semi-structurée | JSON, XML, HTML |
| Non structurée | Texte libre, images, son |

Environ 80 % des données sont non structurées.

## Historique

- **70s à 2000** : SGBDR, données typées, utilisées par des techniciens.
- **2000 à 2005** : Internet, données semi-structurées et non structurées, débuts du NoSQL.
- **2005 à aujourd'hui** : réseaux sociaux, explosion des volumes, fin de la loi de Moore (on ne peut
  plus compter sur une machine plus rapide, il faut distribuer).

## Les 3 V

- **Volume** : ne tient pas dans un SGBDR, ne peut pas être traité par une seule machine (To, Po).
- **Vélocité** : données produites en continu, résultats attendus en quasi temps réel.
- **Variété** : tous les formats.
- D'autres V s'ajoutent souvent : Véracité, Valeur.

## Cluster Big Data

- **Cluster** : ensemble de machines connectées vues comme un seul système.
- Modèle **maître / esclaves (workers)**.
- Exemples de stacks : Hadoop, Elasticsearch, Cassandra.
- **Data Lake** : réservoir central qui rassemble les données de toute l'entreprise pour les analyser plus tard.

## Écosystème Hadoop

Créé en 2006 chez Yahoo, open source (Apache), Java, Linux.

| Rôle | Composant |
|------|-----------|
| Système de fichiers distribué | **HDFS** |
| Gestionnaire de ressources | **YARN** |
| Moteurs d'exécution | MapReduce, Tez, **Spark** |
| Entrepôt / SQL | Hive |
| Base NoSQL | HBase |

## Métiers de la data

| Métier | Rôle | Outils |
|--------|------|--------|
| Data Analyst | Interpréter et visualiser pour aider à la décision | SQL, outils BI |
| Data Scientist | ML, statistiques, modèles prédictifs | Python, R |
| Data Engineer | Construire les pipelines d'ingestion, stockage, traitement | Spark, Hive, HDFS |
| Data Architect | Concevoir la plateforme, la sécurité, la gouvernance | Toute la stack |

Pyramide de la data science (de la base vers le haut) : collecter, déplacer et stocker, explorer et
transformer, agréger et labelliser, apprendre et optimiser. Pas de data science sans data engineering.

## A retenir

- CAP : 2 propriétés sur 3, la partition est inévitable.
- Big Data = Volume, Vélocité, Variété.
- Scale out plutôt que scale up.
- Hadoop = HDFS (stockage) + YARN (ressources) + moteur (Spark).
