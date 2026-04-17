# Watch Similarity Project

## Introduction
Projet de classification et similarité de montres réalisé dans le cadre du Master 1 Big Data & IA.
L'objectif est de trouver des montres visuellement similaires à partir d'une image donnée, en combinant un microservice Scala pour le traitement des données, un microservice Python minimal pour l'extraction de features CNN, et deux interfaces utilisateur.

## Architecture du projet

```
Watches Similarity Project/
│
├── data/                             # Dataset
│   └── watches/
│       ├── images/
│       └── metadata.csv
│
├── microservice-1-scala/             # Microservice Scala
│   ├── project/
│   │   └── build.properties
│   ├── src/main/scala/
│   │   ├── Main.scala
│   │   ├── Parsing.scala
│   │   ├── ImageProcessing.scala
│   │   └── Scoring.scala
│   └── build.sbt
│
├── microservice-2-python/            # Microservice Python
│   └── model.py
│
├── frontend/                         # Interface Streamlit
│   └── streamlit_app.py
│
├── frontend-web/                     # Interface Web (Flask + HTML/CSS/JS)
│   ├── app.py
│   └── index.html
│
├── preprocessed/                     # Généré — images redimensionnées + CSV
├── embeddings/                       # Généré — vecteurs CNN
├── output/                           # Généré — résultats finaux CSV
│
├── run.bat                           # Script de lancement global
├── .gitignore
└── README.md
```

## Technologies utilisées

| Composant | Technologie |
|---|---|
| Parsing & traitement images | Scala 2.13 |
| Calcul de similarité (parallèle) | Scala 2.13 + Parallel Collections |
| Extraction features CNN | Python + TensorFlow (MobileNetV2) |
| Interface Streamlit | Python + Streamlit |
| Interface Web | Python + Flask + HTML/CSS/JS |
| Modèle CNN | MobileNetV2 (pré-entraîné ImageNet) |
| Métrique de similarité | Similarité Cosinus |

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
pip install tensorflow pandas numpy streamlit flask pillow
```

## Lancer le projet

### Pipeline complet (run.bat)
```bash
run.bat
```
Ce script exécute automatiquement dans l'ordre :
1. **Scala MS1** — Parsing du CSV + redimensionnement des images
2. **Python** — Extraction des features CNN (MobileNetV2)
3. **Scala Scoring** — Calcul de la similarité cosinus (parallèle) + génération du CSV
4. **Streamlit** — Lancement de l'interface Streamlit

### Ou étape par étape

```bash
# Étape 1 - Parsing + traitement images
cd microservice-1-scala && sbt run

# Étape 2 - Extraction features CNN
cd microservice-2-python && python model.py

# Étape 3 - Scoring
cd microservice-1-scala && sbt "runMain Scoring"

# Étape 4a - Interface Streamlit
cd frontend && streamlit run streamlit_app.py

# Étape 4b - Interface Web
cd frontend-web && python app.py
```

### Interface Web (upload libre)
Après avoir exécuté les étapes 1 à 3 :
```bash
cd frontend-web && python app.py
```
Ouvrir **http://localhost:5000** — permet d'uploader n'importe quelle photo de montre et d'obtenir les 5 montres les plus similaires du dataset.
