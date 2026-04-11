import pandas as pd
import numpy as np
import tensorflow as tf
from tensorflow.keras.applications.mobilenet_v2 import MobileNetV2, preprocess_input
from tensorflow.keras.preprocessing import image
from sklearn.metrics.pairwise import cosine_similarity
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

print("Terminé ! Matrice d'embeddings (Vecteurs) créée.")
print("🔍 Calcul de la matrice de similarité...")

# 1. On utilise les embeddings qu'on vient de créer
embeddings_matrix = np.array(embeddings)

# 2. Calcul de la Similarité Cosinus (donne un score entre 0 et 1)
# On compare chaque montre avec TOUTES les autres
similarity_matrix = cosine_similarity(embeddings_matrix)

# 3. Pour chaque montre, on cherche les 5 meilleures
top_n = 5
recommendations = []

for i in range(len(df)):
    # On récupère les scores de la montre i, on les trie par ordre décroissant
    # [1:] car le premier résultat est toujours la montre elle-même (100% identique)
    similar_indices = similarity_matrix[i].argsort()[-(top_n+1):-1][::-1]
    
    # On récupère les vrais IDs de ces montres
    similar_ids = [ids[idx] for idx in similar_indices]
    recommendations.append(similar_ids)

# 4. On ajoute cette colonne à notre DataFrame d'origine
df['similar_ids'] = recommendations

# 5. SAUVEGARDE FINALE (Le fameux OUTPUT du schéma)
output_dir = "../data/output"
if not os.path.exists(output_dir):
    os.makedirs(output_dir)

output_path = os.path.join(output_dir, "final_results.parquet")
df.to_parquet(output_path, index=False)

print(f"Fichier OUTPUT créé avec succès : {output_path}")