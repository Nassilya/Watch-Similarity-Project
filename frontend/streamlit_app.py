import streamlit as st
import pandas as pd
import os

st.set_page_config(page_title="Watch Finder", layout="wide", page_icon="⌚", initial_sidebar_state="expanded")

st.markdown("""
<style>
  /* Dark background */
  .stApp { background-color: #080b12; }

  #MainMenu, footer { visibility: hidden; }

  /* Sidebar */
  [data-testid="stSidebar"] {
    background-color: #0d1020;
    border-right: 1px solid #1a1a30;
  }

  [data-testid="stSidebar"] * { color: #ccc !important; }

  /* Selectbox */
  [data-testid="stSelectbox"] > div > div {
    background-color: #111428 !important;
    border: 1px solid #2a2a3e !important;
    border-radius: 10px !important;
    color: #eee !important;
  }

  .watch-brand {
    font-size: 0.72rem;
    font-weight: 600;
    color: #6366f1;
    text-transform: uppercase;
    letter-spacing: 1px;
    margin-bottom: 4px;
  }

  .watch-name {
    font-size: 0.92rem;
    font-weight: 600;
    color: #e8e8ff;
    line-height: 1.4;
    margin-bottom: 6px;
  }

  .watch-price {
    font-size: 0.85rem;
    color: #888;
    margin-bottom: 10px;
  }

  .badge-rank {
    display: inline-block;
    background: #1e1e3a;
    color: #a78bfa;
    font-size: 0.7rem;
    font-weight: 700;
    padding: 3px 10px;
    border-radius: 20px;
    margin-bottom: 10px;
    letter-spacing: 0.5px;
  }

  .conf-row {
    display: flex;
    justify-content: space-between;
    font-size: 0.72rem;
    color: #555;
    margin-bottom: 4px;
  }

  .conf-pct { color: #a78bfa; font-weight: 700; }

  .conf-bar-bg {
    background: #1a1a30;
    border-radius: 4px;
    height: 5px;
    overflow: hidden;
  }

  .conf-bar-fill {
    height: 100%;
    background: linear-gradient(90deg, #6366f1, #a78bfa);
    border-radius: 4px;
  }

  .selected-title {
    font-size: 1.6rem;
    font-weight: 700;
    color: #e8e8ff;
    margin-bottom: 6px;
  }

  .selected-brand {
    font-size: 0.85rem;
    color: #6366f1;
    font-weight: 600;
    text-transform: uppercase;
    letter-spacing: 1px;
    margin-bottom: 12px;
  }

  .selected-price {
    font-size: 1.1rem;
    color: #aaa;
    font-weight: 500;
  }

  .section-title {
    font-size: 1.2rem;
    font-weight: 700;
    color: #e0e0ff;
    margin-bottom: 20px;
    display: flex;
    align-items: center;
    gap: 10px;
  }

  .section-badge {
    background: #1e1e3a;
    color: #a78bfa;
    font-size: 0.72rem;
    font-weight: 700;
    padding: 3px 10px;
    border-radius: 20px;
    letter-spacing: 0.5px;
  }

  .page-header {
    padding: 12px 0 32px;
    border-bottom: 1px solid #1a1a30;
    margin-bottom: 36px;
    font-size: 4rem;
  }

  .page-logo {
    font-size: 1.8rem;
    font-weight: 700;
    background: linear-gradient(135deg, #e0e0ff, #a78bfa);
    -webkit-background-clip: text;
    -webkit-text-fill-color: transparent;
    letter-spacing: -0.5px;
  }

  .page-sub { color: #555; font-size: 0.9rem; margin-top: 4px; }
</style>
""", unsafe_allow_html=True)


@st.cache_data
def load_data():
    df = pd.read_csv("../output/final_results.csv", dtype={'id': str})
    df['similar_ids'] = df['similar_ids'].apply(lambda x: x.split(';'))
    df['similar_scores'] = df['similar_scores'].apply(lambda x: [float(s) for s in x.split(';')])
    return df


try:
    df = load_data()
except Exception as e:
    st.error(f"Could not load results file: {e}")
    st.stop()


st.sidebar.title("Watch Finder")
st.sidebar.markdown("---")
selected_name = st.sidebar.selectbox("Select a watch", df['name'].unique())


st.markdown("""
<div class="page-header">
  <div class="page-logo">Watch Finder</div>
</div>
""", unsafe_allow_html=True)


watch_info = df[df['name'] == selected_name].iloc[0]

col_img, col_info = st.columns([1, 2], gap="large")

with col_img:
    if os.path.exists(watch_info['imagePath']):
        st.image(watch_info['imagePath'], use_container_width=True)

with col_info:
    st.markdown(f"""
    <div style="padding-top: 12px;">
      <div class="selected-brand">{watch_info['brand']}</div>
      <div class="selected-title">{watch_info['name']}</div>
      <div class="selected-price">{watch_info['price']}</div>
    </div>
    """, unsafe_allow_html=True)

st.markdown("<div style='margin: 32px 0 8px;'></div>", unsafe_allow_html=True)

st.markdown("""
<div class="section-title">
  Similar Watches <span class="section-badge">TOP 5</span>
</div>
""", unsafe_allow_html=True)

reco_ids = watch_info['similar_ids']
reco_scores = watch_info['similar_scores']
ranks = ['#1', '#2', '#3', '#4', '#5']


cols = st.columns(5, gap="medium")

for i, (sim_id, score) in enumerate(zip(reco_ids, reco_scores)):
    matches = df[df['id'] == str(sim_id)]
    if matches.empty:
        continue
    item = matches.iloc[0]

    with cols[i]:
        if os.path.exists(item['imagePath']):
            st.image(item['imagePath'], use_container_width=True)

        st.markdown(f"""
        <div>
          <div class="badge-rank">{ranks[i]}</div>
          <div class="watch-brand">{item['brand']}</div>
          <div class="watch-name">{item['name']}</div>
          <div class="watch-price">{item['price']}</div>
          <div class="conf-row">
            <span>Match</span>
            <span class="conf-pct">{score}%</span>
          </div>
          <div class="conf-bar-bg">
            <div class="conf-bar-fill" style="width:{score}%"></div>
          </div>
        </div>
        """, unsafe_allow_html=True)
