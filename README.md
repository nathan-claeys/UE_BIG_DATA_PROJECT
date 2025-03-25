# UE_BIG_DATA_PROJECT

# Nos requêtes avec Spark et Kafka : 

## Batch :

1.  Calcul du temps minimum d'attente entre l'arrivée d'un avion et le prochain bus vers le centre-ville sur une journée.
2. Affichage de l'affluence des bus pour un arrêt donné par heure sur une journée.

## Stream : 

3. Pour une ligne données, afficher une estimation de la position des bus en direct.
4. Pour un emplacement géographique, les cinq abris à vélos les plus proches, leur addresse, ainsi que le nombre de vélos disponibles, le nombre de places disponibles et le nombre de places totales

# Démarrage

## Développement
Créer un environnement virtuel Python avec les dépendances requises pour le surlignage syntaxique :
```
python3 -m venv .venv
```
```
.venv\Scripts\activate.bat
```
```
pip install -r requirements.txt
```

## Utilisation

### Déploiement
Lancer les conteneurs nécessaires :
```
docker compose up
```

Lancer le producer (**nécessaire pour tous les consumers**) : ``` producer.ipynb```

### Requêtes
[1.](#batch) ``` plane_naolib_consumer.ipynb ```

[2.](#batch) ``` batch_bus_affluence.ipynb ```

[3.](#stream) ``` naolib_consumer.ipynb ```

[4.](#stream) ``` bike_consumer.ipynb ```



## Interfaces graphiques
kafka-ui: ``` localhost:8082```
spark-ui: ``` localhost:8080 ```
