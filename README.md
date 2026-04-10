# TP3 Immobilier - Pipeline DVF (Airflow + HDFS + PostgreSQL)

Ce projet met en place un pipeline ETL autour des donnees DVF (Demandes de Valeurs Foncieres) :

- ingestion depuis data.gouv.fr
- stockage raw dans HDFS (avec partitionnement Hive-style)
- transformation et agregation avec pandas
- chargement dans PostgreSQL (zone curated)
- orchestration complete avec Apache Airflow

## 1. Architecture du projet

- `docker-compose.yaml` : stack complete (Airflow, PostgreSQL, NameNode, 2 DataNodes)
- `dags/dag_dvf.py` : DAG principal `pipeline_dvf_immobilier`
- `dags/helpers/webhdfs_client.py` : client helper WebHDFS
- `sql/init_dvf.sql` : schema PostgreSQL (`dvf`, `dvf_raw`, `prix_m2_arrondissement`, `stats_marche`)

## 2. Prerequis

- Docker + Docker Compose v2
- Ports libres : `8080`, `9870`, `9864`, `9865`, `9000`, `5432`

## 3. Demarrage de la stack

Depuis le dossier parent (`Apache_Airflow`) :

```bash
docker compose -f TP3-Immobilier/docker-compose.yaml up -d
docker compose -f TP3-Immobilier/docker-compose.yaml ps
```

Tous les services doivent passer en `healthy`.

## 4. Acces interfaces

- Airflow Web UI : [http://localhost:8080](http://localhost:8080)  
  Login : `admin` / `admin`
- HDFS NameNode UI : [http://localhost:9870](http://localhost:9870)
- HDFS DataNode 1 UI : [http://localhost:9864](http://localhost:9864)
- HDFS DataNode 2 UI : [http://localhost:9865](http://localhost:9865)
- PostgreSQL : `localhost:5432` (`airflow` / `airflow`)

## 5. DAG principal

ID du DAG : `pipeline_dvf_immobilier`

### Taches executees

1. `verifier_sources`  
   Verifie la disponibilite de data.gouv.fr et WebHDFS.
2. `telecharger_dvf`  
   Recupere la ressource DVF dynamique via l'API data.gouv, telecharge l'archive zip et extrait le fichier texte.
3. `stocker_hdfs_raw`  
   Partitionne et charge dans HDFS :
   - `/data/dvf/raw/annee=YYYY/dept=75/dvf_YYYY_75.csv`
   - `/data/dvf/raw/annee=YYYY/dept=92/dvf_YYYY_92.csv`
4. `traiter_donnees`  
   Filtre les appartements parisiens, calcule le prix au m2, puis agrege par arrondissement.
5. `inserer_postgresql`  
   UPSERT dans `prix_m2_arrondissement` et `stats_marche`.
6. `generer_rapport`  
   Produit un classement des arrondissements par prix median au m2.
7. `analyser_tendances`  
   Calcule l'evolution MoM et stocke les metriques de tendance dans `stats_marche`.

## 6. Lancer le pipeline

```bash
docker exec -it dvf-airflow-scheduler airflow dags unpause pipeline_dvf_immobilier
docker exec -it dvf-airflow-scheduler airflow dags trigger pipeline_dvf_immobilier
docker exec -it dvf-airflow-scheduler airflow dags list-runs -d pipeline_dvf_immobilier
```

## 7. Verifications rapides

### 7.1 HDFS

```bash
curl "http://localhost:9870/webhdfs/v1/data/dvf/raw/annee=2026/?op=LISTSTATUS&user.name=root"
curl "http://localhost:9870/webhdfs/v1/data/dvf/raw/annee=2026/dept=75/?op=LISTSTATUS&user.name=root"
```

### 7.2 PostgreSQL

```bash
docker exec -it dvf-postgres psql -U airflow -d dvf -c "SELECT COUNT(*) FROM prix_m2_arrondissement;"
docker exec -it dvf-postgres psql -U airflow -d dvf -c "SELECT * FROM stats_marche ORDER BY date_calcul DESC LIMIT 1;"
```

### 7.3 Replication HDFS (Bonus 3)

```bash
docker exec -it dvf-namenode hdfs dfsadmin -report
docker exec -it dvf-namenode hdfs fsck /data/dvf/raw/annee=2026/dept=75/dvf_2026_75.csv -files -blocks -locations
```

Attendus :

- `Live datanodes (2)`
- `Default replication factor: 2`
- blocs avec `Live_repl=2`

## 8. Tables PostgreSQL

- `dvf_raw` : table brute (schema disponible, non alimentee par le DAG actuel)
- `prix_m2_arrondissement` : agregats mensuels par arrondissement
- `stats_marche` : stats globales + indicateurs de tendance MoM

## 9. Arret / nettoyage

```bash
docker compose -f TP3-Immobilier/docker-compose.yaml down
```

Suppression complete avec volumes :

```bash
docker compose -f TP3-Immobilier/docker-compose.yaml down -v
```
