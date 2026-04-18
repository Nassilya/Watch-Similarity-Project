# Watch Similarity Project

## Introduction
Projet de recherche d'images par le contenu (CBIR — Content-Based Image Retrieval) appliqué aux montres, réalisé dans le cadre du Master 1 Big Data & IA.
L'objectif est de trouver des montres visuellement similaires à partir d'une image donnée, en combinant deux microservices Scala/Spark, un script Python CNN, et deux interfaces utilisateur.

## Architecture du projet

```
Watches Similarity Project/
│
├── data/                             # Dataset
│   └── watches/
│       ├── images/
│       └── metadata.csv
│
├── microservice-1-scala/             # MS1 — Spark Scala
│   ├── project/
│   │   └── build.properties
│   ├── src/main/scala/
│   │   ├── Main.scala
│   │   ├── Parsing.scala
│   │   └── ImageProcessing.scala
│   └── build.sbt
│
├── microservice-2-scala/             # MS2 — Spark Scala
│   ├── project/
│   │   └── build.properties
│   ├── src/main/scala/
│   │   └── Scoring.scala
│   └── build.sbt
│
├── python/                           # CNN Feature Extraction
│   └── model.py
│
├── frontend/                         # Interface Streamlit
│   └── streamlit_app.py
│
├── frontend-web/                     # Interface Web (Flask + HTML/CSS/JS)
│   ├── app.py
│   └── index.html
│
├── preprocessed/                     # Généré — Parquet + images redimensionnées
├── embeddings/                       # Généré — vecteurs CNN (CSV)
├── output/                           # Généré — résultats finaux CSV
│
├── run.bat                           # Script de lancement global
├── .gitignore
└── README.md
```

## Technologies utilisées

| Composant | Technologie |
|---|---|
| Parsing & traitement images | Scala 2.13 + Apache Spark 3.3 (DataFrame + RDD) |
| Calcul de similarité (distribué) | Scala 2.13 + Apache Spark 3.3 (RDD + broadcast) |
| Extraction features CNN | Python + TensorFlow (MobileNetV2) |
| Interface Streamlit | Python + Streamlit |
| Interface Web | Python + Flask + HTML/CSS/JS |
| Modèle CNN | MobileNetV2 (pré-entraîné ImageNet) |
| Métrique de similarité | Similarité Cosinus |
| Format intermédiaire | Apache Parquet |

## Dataset
Télécharger le dataset depuis Kaggle :
[A Dataset of Watches](https://www.kaggle.com/datasets/mathewkouch/a-dataset-of-watches)

Placer les fichiers dans `data/watches/` :
- `images/` — dossier contenant les images JPG
- `metadata.csv` — fichier de métadonnées

## Prérequis

**Scala / sbt:**
- Java 8+
- sbt 1.x

**Python:**
```bash
pip install tensorflow pandas numpy streamlit flask pillow pyarrow
```

## Lancer le projet

### Pipeline complet (run.bat)
```bash
run.bat
```
Ce script exécute automatiquement dans l'ordre :
1. **MS1 (Spark Scala)** — Parsing CSV (DataFrame) + redimensionnement images (RDD) 
2. **Python** — Extraction features CNN (MobileNetV2) depuis le Parquet
3. **MS2 (Spark Scala)** — Calcul similarité cosinus (RDD + broadcast) 
4. **Streamlit** — Lancement de l'interface Streamlit

### Ou étape par étape

```bash
# Étape 1 - MS1 : Parsing + traitement images
cd microservice-1-scala && sbt run

# Étape 2 - Extraction features CNN
cd python && python model.py

# Étape 3 - MS2 : Scoring
cd microservice-2-scala && sbt run

# Étape 4a - Interface Streamlit
cd frontend && streamlit run streamlit_app.py

# Étape 4b - Interface Web
cd frontend-web && python app.py
```

