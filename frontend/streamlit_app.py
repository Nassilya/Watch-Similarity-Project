import streamlit as st
import pandas as pd
import os

st.set_page_config(page_title="Watch Recommender", layout="wide")

@st.cache_data
def load_data():
    df = pd.read_csv("../output/final_results.csv", dtype={'id': str})
    df['similar_ids'] = df['similar_ids'].apply(lambda x: x.split(';'))
    df['similar_scores'] = df['similar_scores'].apply(lambda x: [float(s) for s in x.split(';')])
    return df

try:
    df = load_data()
except Exception as e:
    st.error(f"Erreur : Impossible de trouver le fichier OUTPUT \n{e}")
    st.stop()

st.title("Watch Similarity Finder")
st.markdown("---")

st.sidebar.header("Paramètres")
selected_name = st.sidebar.selectbox(
    "Quelle montre vous intéresse ?",
    df['name'].unique()
)

watch_info = df[df['name'] == selected_name].iloc[0]

col_main1, col_main2 = st.columns([1, 2])

with col_main1:
    if os.path.exists(watch_info['imagePath']):
        st.image(watch_info['imagePath'], use_container_width=True)
    else:
        st.warning("Image source non trouvée")

with col_main2:
    st.header(watch_info['name'])
    st.subheader(f"Marque : {watch_info['brand']}")
    st.write(f"**Prix estimé :** {watch_info['price']}")

st.write("### Modèles similaires")

reco_ids = watch_info['similar_ids']
reco_scores = watch_info['similar_scores']

cols = st.columns(5)

for i, (sim_id, score) in enumerate(zip(reco_ids, reco_scores)):
    matches = df[df['id'] == str(sim_id)]
    if matches.empty:
        continue
    item = matches.iloc[0]

    with cols[i]:
        if os.path.exists(item['imagePath']):
            st.image(item['imagePath'], use_container_width=True)
        else:
            st.warning("Image non trouvée")

        st.write(f"**{item['name']}**")
        st.caption(f"{item['brand']}")
        st.caption(f"{item['price']}")
        st.progress(int(score), text=f"Confiance : {score}%")
