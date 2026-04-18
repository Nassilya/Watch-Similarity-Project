import pandas as pd
import numpy as np
import tensorflow as tf
from tensorflow.keras.applications.mobilenet_v2 import MobileNetV2, preprocess_input
from tensorflow.keras.preprocessing import image
import os

base_model = MobileNetV2(weights='imagenet', include_top=False, pooling='avg', input_shape=(224, 224, 3))

def extract_cnn_features(img_path):
    try:
        img = image.load_img(img_path, target_size=(224, 224))
        x = image.img_to_array(img)
        x = np.expand_dims(x, axis=0)
        x = preprocess_input(x)
        features = base_model.predict(x, verbose=0)
        return features.flatten()
    except Exception as e:
        print(f"Error on {img_path}: {e}")
        return None

df = pd.read_parquet("../preprocessed/watches.parquet")

print(f"Extracting CNN features for {len(df)} watches...")

embeddings = []
ids = []

for _, row in df.iterrows():
    feat = extract_cnn_features(row['processed_path'])
    if feat is not None:
        embeddings.append(feat)
        ids.append(row['id'])

os.makedirs("../embeddings", exist_ok=True)

embeddings_array = np.array(embeddings)
embeddings_df = pd.DataFrame(embeddings_array, columns=[f"f{i}" for i in range(embeddings_array.shape[1])])
embeddings_df.insert(0, 'id', ids)
embeddings_df.to_csv("../embeddings/embeddings.csv", index=False)

print(f"Done! Embeddings saved for {len(ids)} watches to ../embeddings/embeddings.csv")
