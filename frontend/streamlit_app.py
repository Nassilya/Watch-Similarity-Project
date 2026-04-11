import streamlit as st
import pandas as pd
import os

# Configuration de la page (onglet du navigateur)
st.set_page_config(page_title="Watch Recommender", layout="wide")

# 1. Chargement de la case "OUTPUT" (Performance : on utilise le cache)
@st.cache_data
def load_data():
    # Assure-toi que le chemin correspond à celui généré par model.py
    return pd.read_parquet("../data/output/final_results.parquet")

try:
    df = load_data()
except Exception as e:
    st.error(f"Erreur : Impossible de trouver le fichier OUTPUT \n{e}")
    st.stop()

# --- HEADER ---
st.title("⌚ Watch Similarity Finder")
st.markdown("---")

# --- BARRE LATERALE (Sélection) ---
st.sidebar.header("Paramètres")
selected_name = st.sidebar.selectbox(
    "Quelle montre vous intéresse ?",
    df['name'].unique()
)

# --- AFFICHAGE DE LA MONTRE SELECTIONNEE ---
# On récupère la ligne de la montre choisie
watch_info = df[df['name'] == selected_name].iloc[0]

col_main1, col_main2 = st.columns([1, 2])

with col_main1:
    # On vérifie si l'image existe avant de l'afficher
    if os.path.exists(watch_info['imagePath']):
        st.image(watch_info['imagePath'], use_container_width=True)
    else:
        st.warning("Image source non trouvée")

with col_main2:
    st.header(watch_info['name'])
    st.subheader(f"Marque : {watch_info['brand']}")
    st.write(f"**Prix estimé :** {watch_info['price']}")
    st.info("Le modèle MobileNetV2 a analysé les caractéristiques visuelles de cette montre pour vous proposer les meilleures alternatives")

# --- SECTION RECOMMANDATIONS ---
st.write("### 🔍 Modèles similaires (Résultats pré-calculés)")

# On récupère la liste des IDs pré-calculés dans model.py
reco_ids = watch_info['similar_ids']

# Création de 5 colonnes pour afficher le Top 5
cols = st.columns(5)

for i, sim_id in enumerate(reco_ids):
    # On cherche les infos de la montre recommandée par son ID
    item = df[df['id'] == sim_id].iloc[0]
    
    with cols[i]:
        if os.path.exists(item['imagePath']):
            st.image(item['imagePath'], use_container_width=True)
        else:
            st.grey_square() # Placeholder si image manquante
            
        st.write(f"**{item['brand']}**")
        st.caption(f"ID: {sim_id}")