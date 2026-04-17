from flask import Flask, request, jsonify, send_from_directory
from PIL import Image
import numpy as np
import pandas as pd
import tensorflow as tf
from tensorflow.keras.applications.mobilenet_v2 import MobileNetV2, preprocess_input
import os

app = Flask(__name__)

print("Loading model...")
base_model = MobileNetV2(weights='imagenet', include_top=False, pooling='avg', input_shape=(224, 224, 3))

print("Loading embeddings...")
embeddings_df = pd.read_csv("../embeddings/embeddings.csv")
ids = embeddings_df['id'].astype(str).values
embeddings_matrix = embeddings_df.drop('id', axis=1).values.astype(np.float32)

norms = np.linalg.norm(embeddings_matrix, axis=1, keepdims=True)
norms[norms == 0] = 1
embeddings_norm = embeddings_matrix / norms

print("Loading metadata...")
results_df = pd.read_csv("../output/final_results.csv", dtype={'id': str})

print("Ready.")


def extract_features(img):
    img = img.convert("RGB").resize((224, 224))
    x = np.array(img, dtype=np.float32)
    x = np.expand_dims(x, axis=0)
    x = tf.keras.applications.mobilenet_v2.preprocess_input(x)
    features = base_model.predict(x, verbose=0)
    return features.flatten()


@app.route("/")
def index():
    return send_from_directory(".", "index.html")


@app.route("/watch-image/<watch_id>")
def watch_image(watch_id):
    images_dir = os.path.abspath("../data/watches/images")
    return send_from_directory(images_dir, f"{watch_id}.jpg")


@app.route("/find-similar", methods=["POST"])
def find_similar():
    if "image" not in request.files:
        return jsonify({"error": "No image provided"}), 400

    file = request.files["image"]
    try:
        img = Image.open(file.stream)
    except Exception:
        return jsonify({"error": "Invalid image file"}), 400

    features = extract_features(img)
    norm = np.linalg.norm(features)
    if norm == 0:
        return jsonify({"error": "Could not extract features"}), 400

    query_norm = features / norm
    scores = embeddings_norm.dot(query_norm)
    top5_idx = np.argsort(-scores)[:5]

    results = []
    for idx in top5_idx:
        watch_id = ids[idx]
        score = float(scores[idx])
        row = results_df[results_df["id"] == watch_id]
        if not row.empty:
            r = row.iloc[0]
            results.append({
                "id": watch_id,
                "name": r["name"],
                "brand": r["brand"],
                "price": r["price"],
                "score": round(score * 100, 1)
            })

    return jsonify({"results": results})


if __name__ == "__main__":
    app.run(debug=False, port=5000)