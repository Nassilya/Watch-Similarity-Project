import pandas as pd
import numpy as np
import tensorflow as tf
from tensorflow.keras.applications.mobilenet_v2 import MobileNetV2, preprocess_input
from tensorflow.keras.preprocessing import image
import os

# 1. Charger le modèle MobileNetV2 sans la partie "classification"
# On s'arrête à la couche "GlobalAveragePooling2D" pour obtenir un vecteur (1280 nombres)
base_model = MobileNetV2(weights='imagenet', include_top=False, pooling='avg', input_shape=(224, 224, 3))

def extract_cnn_features(img_path):
    try:
        # Charger l'image (déjà mise à 224x224 par ton code Scala)
        img = image.load_img(img_path, target_size=(224, 224))
        x = image.img_to_array(img)
        x = np.expand_dims(x, axis=0)
        
        # Prétraitement spécifique à MobileNet (normalisation entre -1 et 1)
        x = preprocess_input(x)
        
        # Extraire le vecteur de caractéristiques
        features = base_model.predict(x, verbose=0)
        return features.flatten()
    except Exception as e:
        print(f"Erreur sur {img_path}: {e}")
        return None

# 2. Charger ton CSV de liaison produit par Scala
csv_path = "../data/preprocessed/metadata_preprocessed.csv"
df = pd.read_csv(csv_path)

print(f"Extraction des caractéristiques CNN pour {len(df)} montres...")

embeddings = []
ids = []

for _, row in df.iterrows():
    feat = extract_cnn_features(row['processed_path'])
    if feat is not None:
        embeddings.append(feat)
        ids.append(row['id'])

# 3. Sauvegarder la matrice d'embeddings et les IDs
np.save("../data/embeddings/embeddings_cnn.npy", np.array(embeddings))
# On sauve aussi les IDs pour savoir à quoi correspondent les lignes
df_ids = pd.DataFrame({'id': ids})
df_ids.to_csv("../data/embeddings/ids_map.csv", index=False)

print("✅ Terminé ! Matrice d'embeddings (Vecteurs) créée.")