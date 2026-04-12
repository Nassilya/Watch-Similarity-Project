# Watch Similarity Project

## Introduction
Projet de classification et similarité de montres réalisé dans le cadre du Master 1 Big Data & IA.
L'objectif est de trouver des montres visuellement similaires à partir d'une image donnée, en combinant un microservice Scala pour le traitement des données et un microservice Python minimal pour l'extraction de features CNN.

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
├── frontend/                         # Interface utilisateur
│   └── streamlit_app.py              
│
├── preprocessed/                     # Généré par Scala — images redimensionnées + CSV 
├── embeddings/                       # Généré par Python — vecteurs CNN 
├── output/                           # Généré par Scala — résultats finaux CSV 
│
├── run.bat                            # Script de lancement global
├── .gitignore
└── README.md
```

## Technologies utilisées

| Composant | Technologie |
|---|---|
| Parsing & traitement images | Scala 2.13 |
| Calcul de similarité | Scala 2.13 |
| Extraction features CNN | Python + TensorFlow (MobileNetV2) |
| Interface utilisateur | Python + Streamlit |
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
pip install tensorflow pandas numpy streamlit
```

## Lancer le projet

### Tout lancer d'un coup
```bash
run.bat
```
Ce script exécute automatiquement dans l'ordre :
1. **Scala MS1** — Parsing du CSV + redimensionnement des images
2. **Python** — Extraction des features CNN (MobileNetV2)
3. **Scala Scoring** — Calcul de la similarité cosinus + génération du CSV de résultats
4. **Streamlit** — Lancement de l'interface utilisateur

### Ou étape par étape

```bash
# Étape 1 - Parsing + traitement images
cd microservice-1-scala && sbt run

# Étape 2 - Extraction features CNN
cd microservice-2-python && python model.py

# Étape 3 - Scoring
cd microservice-1-scala && sbt "runMain Scoring"

# Étape 4 - Interface
cd frontend && streamlit run streamlit_app.py
```
