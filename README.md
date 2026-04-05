# Watch Similarity Project

## Introduction
Projet de classification et similarité de montres réalisé dans le cadre du Master 1 Big Data & IA.
L'objectif est double :
- **Classifier** une montre à partir de son image (marque, nom, prix)
- **Trouver des montres similaires** dans le dataset à partir d'une image donnée

## Architecture du projet

```
Watch-Similarity-Project/
│
├── data/                              # Dataset 
│   └── watches/
│       ├── images/                   # Images des montres
│       └── metadata.csv              # Métadonnées 
│
├── microservice-1-scala/             # Microservice Scala
│   ├── project/
│   │   └── build.properties          
│   ├── src/main/scala/
│   │   ├── Parsing.scala             
│   │   ├── ImageProcessing.scala     
│   │   ├── Scoring.scala             
│   │   └── Main.scala                
│   └── build.sbt                     a
│
├── microservice-2-python/            # Microservice Python 
│   ├── model.py                     
│   └── train.py                     
│
├── frontend/                         # Interface utilisateur
│   └── streamlit_app.py              
│
├── .gitignore                        
└── README.md                         
```

## Dataset
Télécharger le dataset depuis Kaggle :
[A Dataset of Watches](https://www.kaggle.com/datasets/mathewkouch/a-dataset-of-watches)

