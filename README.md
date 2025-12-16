# Spark : Traitement en streaming des données massives en temps réel

Ce projet s’inscrit dans le cadre du module *Initiation au Big Data et au Cloud Computing*.  
Il vise à mettre en œuvre un système complet de **traitement de données en temps réel** basé sur **Apache Spark**, afin de démontrer les capacités de Spark Streaming dans l’analyse de flux continus de données.

---

## Description générale
Ce projet met en place un **pipeline de traitement de données en temps réel** basé sur **WebSocket, Apache Kafka, Apache Spark Streaming et PostgreSQL**, avec une orchestration partielle via **Docker**.

Les données de marché des cryptomonnaies sont extraites en temps réel depuis **Binance**, transmises via Kafka, traitées par Spark et stockées dans PostgreSQL pour analyse et visualisation.

---

## Prérequis

### Logiciels requis
- **Docker Desktop**
- **Java 17 (Temurin – Adoptium)**
- **Apache Spark 4.0.1**


### Dépendances Hadoop (Windows)
- `winutils.exe`
- `hadoop.dll`

---

## Installation et utilisation de Docker

Docker permet d’exécuter Kafka, Zookeeper et PostgreSQL dans des conteneurs isolés et reproductibles.

### Installation de Docker Desktop
1. Télécharger Docker Desktop depuis le site officiel
2. Installer Docker Desktop
3. Activer **WSL 2** si demandé
4. Vérifier l’installation :
```bash
docker --version
docker compose version
```
---

## Orchestration des services avec docker-compose.yml

Après installation de Docker Desktop, nous avons créé un fichier docker-compose.yml.
Ce fichier permet de :
- définir plusieurs services (Kafka, Zookeeper, PostgreSQL) que l’on veut utiliser,
- configurer les ports, réseaux et volumes,
- et surtout lancer tous les conteneurs en une seule commande.
Dans notre projet, le fichier docker-compose.yaml a servi à lancer :
- Zookeeper (coordination Kafka)
- Kafka (gestion du streaming)
- PostgreSQL (stockage des données traitées)
Le traitement Spark, lui, reste fait en local.
Une fois le docker-compose.yml prêt, on lance le cmd dans le dossier contenant le fichier .yaml et on y exécute la commande suivante :
```bash
docker compose up
```
---

## Kafka : organisation du streaming avec un topic

Un topic Kafka est un canal où Kafka stocke les messages échangés entre un producer (producteur) qui écrit les messages et un consumer (consommateur) qui lit les messages.
Dans notre projet, nous avons créé un topic :
- projet-bdcc
Ce topic sert à recevoir les messages de marché en temps réel.

---

## Extraction temps réel : WebSocket Binance vers Kafka

Un WebSocket est un protocole permettant une communication bidirectionnelle et en temps réel entre un client et un serveur via une connexion persistante.
Nous récupérons les données de Binance via un WebSocket qui est
- wss://stream.binance.com:9443/ws/!miniTicker@arr
Une fois la connexion établie, les messages reçus sont envoyés immédiatement vers Kafka via un Kafka Producer et peuvent être lus en continu via un Kafka Consumer
Les messages arrivent en JSON

---

## Traitement avec Apache Spark

Spark Streaming permet :
- le parsing JSON
- le traitement en temps réel
- le nettoyage ainsi que l'agrégation des données

Spark est exécuté **en local**, sans conteneur Docker.

---

## Stockage

Les données traitées sont stockées dans **PostgreSQL** (conteneur Docker), ce qui permet :
- l’analyse ultérieure
- l’alimentation du dashboard

---

## Dashboard

Un tableau de bord interactif a été développé avec *Streamlit* afin de visualiser, en quasi temps réel, les données de marché issues de Binance et traitées par le pipeline de streaming. 

### Architecture et flux de données

Le dashboard se connecte directement à la base *PostgreSQL* (via SQLAlchemy) contenant une table projet_bdcc. À chaque rafraîchissement, l’application exécute une requête SQL filtrant les observations selon :

  - une sélection de marché (symbol) choisie dans la barre latérale ;
  - une limite d’observations (LIMIT) afin de contrôler le volume chargé en mémoire ;
  - un tri temporel sur timestamp_ts afin de reconstruire correctement les séries temporelles.

Les données sont ensuite converties en DataFrame Pandas pour permettre la production rapide de métriques et de graphiques.

### Rafraîchissement automatique et logique de cache

Le suivi temps réel est assuré par un *auto-refresh* (streamlit_autorefresh) déclenché toutes les REFRESH_INTERVAL = 5 secondes. Ce mécanisme permet de recharger les données en continu sans intervention manuelle.

Pour éviter des requêtes inutiles et améliorer la performance, le chargement est encapsulé dans une fonction décorée par @st.cache_data avec un TTL (Time To Live). Ainsi, sur un court intervalle, Streamlit réutilise le résultat en cache plutôt que de relancer systématiquement la requête.

### Enrichissement des données : indicateurs calculés côté dashboard

Après chargement, l’application calcule plusieurs indicateurs dérivés afin d’obtenir des mesures directement interprétables :

  - *Variation relative (%)* : Cet indicateur mesure le rendement sur la période observée.

  - *Volatilité relative* : Il donne une approximation de l’amplitude des fluctuations, normalisée par le prix.

  - *Spread* : Il représente l’écart absolu entre le maximum et le minimum observés sur la période.

Ces calculs sont effectués à l’affichage afin de rendre l’interface autonome, tout en laissant la persistance se concentrer sur les variables brutes essentielles.

### Paramétrage utilisateur (Sidebar)

La barre latérale permet :

  - de sélectionner les marchés à suivre (multiselect) à partir de la liste des symboles disponibles en base ;
  - de filtrer la période d’observation (1H, 24H, 7J, 30J, Tout) via un filtre temporel appliqué sur timestamp_ts ;

Ce choix d’interface permet à l’utilisateur d'adapter dynamiquement le périmètre d’analyse sans modifier le code.

---

## Membres du groupe
- **Sarah-Laure FOGWOUNG**  
- **Aissatou Sega Diallo**  
- **Foumsou Lawa Prosper**  
- **Bassirou Compaore**  
- **Khadidiatou Diakhate**  

Étudiants en **ISE 2**, ENSAE de Dakar  
Encadrement pédagogique : **Madame Mously Diaw**

