# UE_BIG_DATA_PROJECT

# Nos requêtes : 

## Batch :

- Temps moyen d'attente entre l'arrivée d'un train et le prochain tram par heure sur une journée => Nathan
- Affluence de bus pour une ligne par heure sur une journée => Clément

## Stream : 

- Pour une ligne, les temps d'attente à chaque arrêt => Antoine
- Pour une zone géographique, disponibilité dans des stations vélo / le nombre de places totales (par ex: 3 sur 4) => JB
# Démarrage

## Développement
```
python3 -m venv .venv
```
```
.venv\Scripts\activate.bat
```
```
pip install -r requirements.txt
```


# Projet

L'objectif principal de ce projet est de permettre aux étudiants de développer des compétences pratiques dans la gestion et l'analyse de données massives. En choisissant une source de données parmi celles proposées, les étudiants - en groupe de 4 - devront mettre en oeuvre un pipeline complet, allant de la collecte à l’analyse et à la visualisation des résultats.

Choisissez parmi les sujets suivants :

## Sujets

- Mobilité : [API temps réel Naolib](https://data.nantesmetropole.fr/explore/dataset/244400404_api-temps-reel-tan/information/)
- Mobilité : [Plateform Régionale d'information pour la mobilité (Ile de France) ](https://prim.iledefrance-mobilites.fr/fr)
- Aviation : [OpenSkyNetwork API](https://openskynetwork.github.io/opensky-api/index.html)
- Transports : [SNCF API Open-Data](https://numerique.sncf.com/startup/api/)
- Transports : [Here maps](https://developer.here.com/develop/rest-apis)
- DIFFICILE : Réseaux sociaux : [Bluesky]([https://docs.bsky.app/docs/category/http-reference])

### Autres ?

Si vous souhaitez analyser une autre API, faites-moi une proposition par mail.

### Conseils

Attention aux limites d'utilisation (rate limit) : vérifiez bien celles de l'API que vous utilisez pour éviter d'être bloqués. N'hésitez pas à créer plusieurs comptes si nécessaire et évitez de consommer tous vos crédits trop vite.

## Consignes

Les groupes devront être compter 4 élèves (sauf si nombre impaire dans la promo). Pour ceux qui sont en difficulté avec les installations (problème de mémoire ou de puissance de leur machine), veillez bien à vous inclure dans un groupe n'ayant pas de soucis à ce niveau.

Vous devrez, à l'aide de Kafka (idéalement, Minio à défaut) et Apache Spark pour l'une des APIs mentionnées plus haut :
- effectuer deux requêtes en mode batch et présenter les résultats à l'aide de Pandas et Seaborn
- effectuer deux requêtes en mode streaming avec fenêtres temporelles

Pour Minio, comme pour Kafka, une première étape sera d'insérer les données. Vous pouvez prendre comme modèle les scripts des TPs respectifs.

Les requêtes développées devront avoir du sens en regard des données : gardez à l'esprit que vous devrez préciser une synthèse des résultats obtenus et expliquer leur signification. Il faut donner de la VALEUR aux données.

Optionnel au choix :
- Mixer l'utilisation de plusieurs APIs pour obtenir des résultats plus intéressants
- Relier vos requêtes de streaming à un dashboard (Le dashboard pourra s'alimenter auprès de n'importe quelle source)
- Utiliser l'une des bases NoSQL présentée en tuto ou Neo4J au lieu de Minio (attention à choir une BD adaptée aux requêtes à effectuer et à la nature des données)

## Oral

Temps total : 15 minutes

1. Démo et explications sur l'API utilisée et les données extraites, leur signification, leur intérêt
2. Montrer le code et le commenter (chacun commente une partie du code)