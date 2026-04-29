import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from wordcloud import WordCloud
import matplotlib.pyplot as plt
import matplotlib
matplotlib.use("Agg")
import httpx
import os
from datetime import datetime, timedelta
from collections import Counter
import re
import numpy as np

st.set_page_config(
    page_title="InsightFlow — YouTube Analytics",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="collapsed"
)

# ── Supabase ──────────────────────────────────────────────────
SUPABASE_URL = "https://oymwlmbqzvmpwihhhirx.supabase.co"
SUPABASE_KEY = "sb_publishable_SPQGizmtB8uBcBq6A0kfWw_HOvtQWuQ"
SUPABASE_H   = {"apikey": SUPABASE_KEY, "Authorization": f"Bearer {SUPABASE_KEY}"}

GOLD_DIR      = os.path.join("outputs", "gold")
ANALYTICS_DIR = os.path.join("outputs", "analytics")

COLORS    = ["#38bdf8","#818cf8","#34d399","#fb923c","#f472b6","#a78bfa","#fbbf24","#4ade80","#f87171","#2dd4bf"]
DAY_ORDER = ["Monday","Tuesday","Wednesday","Thursday","Friday","Saturday","Sunday"]

COUNTRY_COORDS = {
    "United States": {"lat": 37.09,  "lon": -95.71},
    "India":         {"lat": 20.59,  "lon":  78.96},
    "United Kingdom":{"lat": 55.37,  "lon":  -3.44},
    "Canada":        {"lat": 56.13,  "lon":-106.35},
    "Australia":     {"lat":-25.27,  "lon": 133.78},
    "Germany":       {"lat": 51.17,  "lon":  10.45},
    "France":        {"lat": 46.23,  "lon":   2.21},
    "Japan":         {"lat": 36.20,  "lon": 138.25},
    "South Korea":   {"lat": 35.91,  "lon": 127.77},
    "Brazil":        {"lat":-14.24,  "lon": -51.93},
}

# ── CSS — Hardcoded Dark Mode ─────────────────────────────────
st.markdown("""
<style>
  /* ── Force dark theme on all Streamlit containers ── */
  .stApp,
  .stApp > div,
  section[data-testid="stSidebar"],
  div[data-testid="stAppViewContainer"],
  div[data-testid="stHeader"],
  div[data-testid="stToolbar"],
  div[data-testid="stDecoration"],
  div.main,
  div.block-container {
    background: #0d1117 !important;
    color: #e6edf3 !important;
  }
  /* Force Streamlit's own theme system to dark */
  :root {
    --primary-color: #58a6ff !important;
    --background-color: #0d1117 !important;
    --secondary-background-color: #161b22 !important;
    --text-color: #e6edf3 !important;
  }

  #MainMenu, footer, header { visibility: hidden; }
  .block-container { padding: 0 !important; max-width: 100% !important; }
  body { font-family: 'Segoe UI', sans-serif; }

  /* ── Navbar ── */
  .navbar {
    display: flex; align-items: center; justify-content: space-between;
    background: #161b22; border-bottom: 1px solid #21262d;
    padding: 12px 24px; position: sticky; top: 0; z-index: 999;
  }
  .nav-title { font-size: 20px; font-weight: 800; color: #58a6ff; letter-spacing: -0.5px; }
  .nav-subtitle { font-size: 11px; color: #8b949e; margin-top: 2px; }
  .dark-badge {
    background: #21262d; border: 1px solid #30363d; border-radius: 8px;
    padding: 5px 12px; font-size: 11px; font-weight: 700;
    color: #58a6ff; letter-spacing: 0.5px;
  }

  /* ── Page buttons ── */
  .page-btn {
    display: inline-block; padding: 6px 20px; border-radius: 6px;
    font-size: 13px; font-weight: 600; cursor: pointer;
    border: 1px solid #30363d; margin-left: 8px;
    text-align: center; transition: all 0.2s;
  }
  .page-active   { background: #1f6feb; color: #fff; border-color: #1f6feb; }
  .page-inactive { background: transparent; color: #8b949e; }

  /* ── KPI cards ── */
  .kpi-row  { display: flex; gap: 14px; padding: 20px 24px 0 24px; flex-wrap: wrap; }
  .kpi-card {
    flex: 1; min-width: 140px; background: #161b22;
    border: 1px solid #30363d; border-radius: 12px;
    padding: 20px 24px 18px 24px; text-align: center;
  }
  .kpi-value    { font-size: 36px; font-weight: 800; color: #58a6ff; line-height: 1.1; }
  .kpi-label    { font-size: 12px; color: #adbac7; margin-top: 8px; text-transform: uppercase; letter-spacing: 0.8px; font-weight: 600; }
  .kpi-sub      { font-size: 12px; color: #8b949e; margin-top: 5px; }
  .kpi-positive { color: #3fb950; }
  .kpi-negative { color: #f85149; }
  .kpi-neutral  { color: #d29922; }

  /* ── Section title ── */
  .sec-title {
    font-size: 13px; font-weight: 800; color: #adbac7 !important;
    text-transform: uppercase; letter-spacing: 1px;
    margin: 20px 0 10px 24px;
    border-left: 3px solid #58a6ff; padding-left: 12px;
  }

  /* ── Chart containers ── */
  .chart-wrap {
    background: #161b22; border: 1px solid #30363d;
    border-radius: 12px; padding: 16px; margin: 0;
  }
  .chart-title {
    font-size: 13px; font-weight: 800; color: #e6edf3 !important;
    margin-bottom: 10px; text-transform: uppercase; letter-spacing: 0.5px;
  }
  .chart-caption { font-size: 11px; color: #adbac7 !important; margin-top: 6px; font-style: italic; }

  /* ── Insight box ── */
  .ins-box {
    background: #161b22; border: 1px solid #30363d;
    border-radius: 12px; padding: 18px 20px; min-height: 140px;
  }
  .ins-title { font-size: 12px; font-weight: 700; color: #58a6ff; text-transform: uppercase; letter-spacing: 0.5px; margin-bottom: 10px; }
  .ins-value { font-size: 30px; font-weight: 800; color: #3fb950; line-height: 1.1; }
  .ins-text  { font-size: 13px; color: #adbac7; margin-top: 8px; line-height: 1.6; }

  /* ── Content padding ── */
  .content { padding: 0 24px 24px 24px; }

  /* ── Inline chart header: title left, selectbox right in same row ── */
  .chart-header-row {
    display: flex; align-items: center; justify-content: space-between;
    margin-bottom: 4px; gap: 8px;
  }
  .chart-header-title {
    font-size: 13px; font-weight: 800; color: #e6edf3 !important;
    text-transform: uppercase; letter-spacing: 0.5px; white-space: nowrap; flex: 1;
  }
  /* ── Chart header selectbox: compact, right-aligned ── */
  /* Target selectboxes that are label_visibility=collapsed (no label rendered) */
  div[data-testid="stHorizontalBlock"] .stSelectbox {
    margin-bottom: 0 !important;
    padding-bottom: 0 !important;
  }
  div[data-testid="stHorizontalBlock"] .stSelectbox label { display: none !important; }
  div[data-testid="stHorizontalBlock"] .stSelectbox [data-baseweb="select"] > div {
    min-height: 42px !important; height: auto !important;
    padding: 8px 12px !important; font-size: 13px !important;
    background: #21262d !important; border-color: #30363d !important;
    color: #e6edf3 !important; border-radius: 6px !important;
    display: flex !important; align-items: center !important;
  }
  /* Collapse vertical gap between the header columns row and chart-wrap below */
  div[data-testid="stHorizontalBlock"] + div[data-testid="stVerticalBlock"] > div:first-child {
    margin-top: -8px !important;
  }
  /* Remove top padding from the column block itself */
  div[data-testid="column"] { padding-top: 0 !important; }

  /* ── Streamlit widget overrides ── */
  div[data-testid="metric-container"] { display: none; }

  /* Closed selectbox box */
  .stSelectbox > div > div,
  .stSelectbox [data-baseweb="select"] > div {
    background: #21262d !important;
    border-color: #30363d !important;
    color: #e6edf3 !important;
    font-size: 13px !important;
    min-height: 42px !important;
    height: auto !important;
    display: flex !important;
    align-items: center !important;
  }
  /* The selected value text */
  .stSelectbox [data-baseweb="select"] span,
  .stSelectbox [data-baseweb="select"] div,
  .stSelectbox [data-baseweb="select"] input,
  .stSelectbox [data-baseweb="select"] [data-testid="stMarkdownContainer"],
  .stSelectbox [class*="ValueContainer"] *,
  .stSelectbox [class*="singleValue"],
  .stSelectbox [class*="placeholder"] {
    color: #e6edf3 !important;
    background: transparent !important;
    font-size: 13px !important;
    line-height: 1.4 !important;
    overflow: visible !important;
    white-space: nowrap !important;
  }

  /* ── Open dropdown portal — target every possible node ── */
  [data-baseweb="popover"] *,
  [data-baseweb="menu"] *,
  ul[role="listbox"],
  ul[role="listbox"] *,
  li[role="option"],
  li[role="option"] * {
    color: #e6edf3 !important;
  }
  /* The outer popover/menu container */
  [data-baseweb="popover"],
  [data-baseweb="menu"] {
    background: #1c2128 !important;
    border: 1px solid #30363d !important;
    box-shadow: 0 8px 24px rgba(0,0,0,0.6) !important;
  }
  /* Each option row */
  [data-baseweb="option"],
  li[role="option"] {
    background: #1c2128 !important;
    color: #e6edf3 !important;
    font-size: 13px !important;
    padding: 10px 14px !important;
    cursor: pointer !important;
  }
  /* Hover + selected */
  [data-baseweb="option"]:hover,
  li[role="option"]:hover,
  [data-baseweb="option"][aria-selected="true"],
  li[role="option"][aria-selected="true"] {
    background: #21262d !important;
    color: #58a6ff !important;
  }
  /* Selected value text in closed box */
  [data-baseweb="select"] span,
  [data-baseweb="select"] div,
  [data-baseweb="select"] input {
    color: #e6edf3 !important;
    background: transparent !important;
  }
  /* Dropdown arrow */
  [data-baseweb="select"] svg { fill: #8b949e !important; }

  .stMultiSelect > div > div { background: #161b22 !important; border-color: #30363d !important; }
  .stMultiSelect span { background: #1f6feb !important; }

  /* Labels */
  label, .stSelectbox label, p { color: #8b949e !important; font-size: 11px !important; }

  /* Headings */
  h1, h2, h3, h4 { color: #e6edf3 !important; }

  /* Tabs — hide default tab bar (using buttons instead) */
  .stTabs [data-baseweb="tab-list"] { display: none; }

  /* Buttons */
  .stButton > button {
    background: #21262d !important;
    color: #e6edf3 !important;
    border: 1px solid #30363d !important;
    border-radius: 6px !important;
    font-size: 12px !important;
    font-weight: 600 !important;
  }
  .stButton > button[kind="primary"],
  .stButton > button[data-testid*="primary"] {
    background: #1f6feb !important;
    color: #fff !important;
    border-color: #1f6feb !important;
  }
  .stButton > button:hover { border-color: #58a6ff !important; color: #58a6ff !important; }

  /* Horizontal rule */
  hr { border-color: #21262d !important; }

  /* DataFrame / table */
  .stDataFrame { background: #161b22 !important; }
  [data-testid="stTable"] td, [data-testid="stTable"] th { color: #e6edf3 !important; }

  /* Info / warning boxes */
  .stAlert { background: #161b22 !important; border-color: #30363d !important; color: #e6edf3 !important; }
</style>
""", unsafe_allow_html=True)

# ── Extra global style injected into body to catch Streamlit's portal dropdowns ──
st.markdown("""
<style>
  /* Streamlit renders open dropdowns as a portal appended to <body>.
     These rules live at body level so they win over Streamlit's defaults. */
  body [data-baseweb="popover"],
  body [data-baseweb="menu"],
  body ul[role="listbox"] {
    background-color: #1c2128 !important;
    border: 1px solid #30363d !important;
    border-radius: 8px !important;
    box-shadow: 0 8px 24px rgba(0,0,0,0.7) !important;
  }
  body [data-baseweb="option"],
  body li[role="option"] {
    background-color: #1c2128 !important;
    color: #e6edf3 !important;
    font-size: 13px !important;
    padding: 10px 14px !important;
  }
  body [data-baseweb="option"]:hover,
  body li[role="option"]:hover {
    background-color: #21262d !important;
    color: #58a6ff !important;
  }
  body [data-baseweb="option"][aria-selected="true"],
  body li[role="option"][aria-selected="true"] {
    background-color: #21262d !important;
    color: #58a6ff !important;
  }
  /* All text inside the open list */
  body [data-baseweb="popover"] *,
  body [data-baseweb="menu"] *,
  body ul[role="listbox"] * {
    color: #e6edf3 !important;
  }
</style>
""", unsafe_allow_html=True)

# ── Dark plotly base ──────────────────────────────────────────
def dark(height=350, **kwargs):
    base = dict(
        plot_bgcolor  = "#161b22",
        paper_bgcolor = "#161b22",
        font          = dict(color="#c9d1d9", size=12),
        height        = height,
        margin        = dict(l=8, r=8, t=32, b=8),
        showlegend    = False,
        xaxis         = dict(
            gridcolor="#21262d", linecolor="#444d56",
            tickfont=dict(size=11, color="#adbac7"),
            title_font=dict(color="#c9d1d9"),
        ),
        yaxis         = dict(
            gridcolor="#21262d", linecolor="#444d56",
            tickfont=dict(size=11, color="#adbac7"),
            title_font=dict(color="#c9d1d9"),
        ),
    )
    base.update(kwargs)
    return base



# ── Load historical ───────────────────────────────────────────
@st.cache_data(ttl=3600)
def load_hist():
    kpis      = pd.read_csv(os.path.join(GOLD_DIR,      "kpis.csv"))
    cat_perf  = pd.read_csv(os.path.join(GOLD_DIR,      "category_performance.csv"))
    country   = pd.read_csv(os.path.join(GOLD_DIR,      "country_performance.csv"))
    channels  = pd.read_csv(os.path.join(GOLD_DIR,      "top_channels.csv"))
    day_perf  = pd.read_csv(os.path.join(GOLD_DIR,      "day_performance.csv"))
    hour_perf = pd.read_csv(os.path.join(GOLD_DIR,      "hour_performance.csv"))
    corr      = pd.read_csv(os.path.join(ANALYTICS_DIR, "correlation_matrix.csv"), index_col=0)
    eng_hmap  = pd.read_csv(os.path.join(ANALYTICS_DIR, "engagement_heatmap.csv"))
    title_eng = pd.read_csv(os.path.join(ANALYTICS_DIR, "title_length_vs_engagement.csv"))
    tag_eng   = pd.read_csv(os.path.join(ANALYTICS_DIR, "tag_count_vs_views.csv"))
    sent_dist = pd.read_csv(os.path.join(ANALYTICS_DIR, "sentiment_distribution.csv"))
    sent_eng  = pd.read_csv(os.path.join(ANALYTICS_DIR, "sentiment_vs_engagement.csv"))
    sent_cat  = pd.read_csv(os.path.join(ANALYTICS_DIR, "sentiment_by_category.csv"))
    keywords  = pd.read_csv(os.path.join(ANALYTICS_DIR, "top_keywords.csv"))
    trending  = pd.read_csv(os.path.join(ANALYTICS_DIR, "top_trending_videos.csv"))
    prescr    = pd.read_csv(os.path.join(ANALYTICS_DIR, "prescriptive.csv"))
    scatter   = pd.read_csv(os.path.join(ANALYTICS_DIR, "scatter_sample.csv"))
    liker     = pd.read_csv(os.path.join(ANALYTICS_DIR, "like_ratio_by_category.csv"))
    return (kpis, cat_perf, country, channels, day_perf, hour_perf, corr,
            eng_hmap, title_eng, tag_eng, sent_dist, sent_eng, sent_cat,
            keywords, trending, prescr, scatter, liker)

@st.cache_data(ttl=300)
def load_live(dr="all"):
    try:
        if dr == "24h":
            cut  = (datetime.utcnow()-timedelta(hours=24)).isoformat()
            filt = f"&fetch_time=gte.{cut}"
        elif dr == "7d":
            cut  = (datetime.utcnow()-timedelta(days=7)).isoformat()
            filt = f"&fetch_time=gte.{cut}"
        else:
            filt = ""

        all_data  = []
        chunk     = 500

        with httpx.Client(timeout=60) as c:
            # Get min and max id first
            r_min = c.get(
                f"{SUPABASE_URL}/rest/v1/youtube_live?select=id{filt}&order=id.asc&limit=1",
                headers=SUPABASE_H)
            r_max = c.get(
                f"{SUPABASE_URL}/rest/v1/youtube_live?select=id{filt}&order=id.desc&limit=1",
                headers=SUPABASE_H)

            if r_min.status_code != 200 or not r_min.json():
                return pd.DataFrame()

            min_id = r_min.json()[0]["id"]
            max_id = r_max.json()[0]["id"]

            # Fetch in id-range chunks
            current_id = min_id
            while current_id <= max_id:
                end_id = current_id + chunk - 1
                url    = (f"{SUPABASE_URL}/rest/v1/youtube_live"
                         f"?select=*{filt}"
                         f"&id=gte.{current_id}"
                         f"&id=lte.{end_id}"
                         f"&order=id.asc")
                r = c.get(url, headers=SUPABASE_H)
                if r.status_code == 200:
                    batch = r.json()
                    if batch:
                        all_data.extend(batch)
                current_id = end_id + 1

        if all_data:
            df = pd.DataFrame(all_data)
            df = df.drop_duplicates(subset=["id"])
            df["fetch_time"] = pd.to_datetime(df["fetch_time"])
            for col in ["views","likes","comments"]:
                df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0).astype(int)
            df["engagement"] = (df["likes"]+df["comments"])/(df["views"]+1)
            df["like_ratio"] = df["likes"]/(df["likes"]+1)
            if "country" not in df.columns:
                df["country"] = "United States"
            return df
    except Exception as e:
        st.error(f"Supabase error: {e}")
    return pd.DataFrame()

def get_keywords(titles, n=30):
    sw = {"the","a","an","in","on","of","to","and","is","for","with","at","by",
          "from","this","that","are","was","it","be","or","but","not","have",
          "has","will","can","i","you","we","my","its","new","how","what","why"}
    words = []
    for t in titles.dropna():
        words += [w.lower() for w in re.findall(r'\b[a-zA-Z]{3,}\b', str(t))
                  if w.lower() not in sw]
    return dict(Counter(words).most_common(n))

def compute_forecast(df_live, horizon_h=24):
    """Linear regression forecast per category using numpy polyfit."""
    results = []
    runs = sorted(df_live["fetch_time"].unique())
    if len(runs) < 2:
        cat_avg = df_live.groupby("category")["views"].mean().reset_index()
        cat_avg.columns = ["category", "current_avg"]
        cat_avg["forecast_avg"] = cat_avg["current_avg"]
        cat_avg["growth_pct"]   = 0.0
        return cat_avg
    run_index = {r: i for i, r in enumerate(runs)}
    df_live   = df_live.copy()
    df_live["run_idx"] = df_live["fetch_time"].map(run_index)
    for cat, grp in df_live.groupby("category"):
        ts = grp.groupby("run_idx")["views"].mean().reset_index().sort_values("run_idx")
        if len(ts) < 2:
            current = ts["views"].iloc[-1]
            results.append({"category": cat, "current_avg": round(current),
                            "forecast_avg": round(current), "growth_pct": 0.0})
            continue
        x = ts["run_idx"].values.astype(float)
        y = ts["views"].values.astype(float)
        m, b     = np.polyfit(x, y, 1)
        next_run = x[-1] + (horizon_h / 3.0)
        forecast = max(m * next_run + b, 0)
        current  = float(y[-1])
        growth   = ((forecast - current) / (current + 1)) * 100
        results.append({"category": cat, "current_avg": round(current),
                        "forecast_avg": round(forecast), "growth_pct": round(growth, 1)})
    return pd.DataFrame(results).sort_values("growth_pct", ascending=False)

# ── Session state for page ────────────────────────────────────
if "page" not in st.session_state:
    st.session_state.page = "live"

# ── NAVBAR ────────────────────────────────────────────────────
st.markdown("""
<div class="navbar">
  <div>
    <div class="nav-title">&#128202; InsightFlow</div>
    <div class="nav-subtitle">YouTube Trending Analytics</div>
  </div>
  <div class="dark-badge">&#9790; Dark Mode</div>
</div>
""", unsafe_allow_html=True)

# Page switcher buttons
col_nav1, col_nav2, col_nav3 = st.columns([6, 1, 1])
with col_nav2:
    if st.button("🔴 Live Data",
                 type="primary" if st.session_state.page=="live" else "secondary",
                 use_container_width=True):
        st.session_state.page = "live"
        st.rerun()
with col_nav3:
    if st.button("📂 Historical",
                 type="primary" if st.session_state.page=="hist" else "secondary",
                 use_container_width=True):
        st.session_state.page = "hist"
        st.rerun()

st.markdown("---")

# ══════════════════════════════════════════════════════════════
# LIVE PAGE
# ══════════════════════════════════════════════════════════════
if st.session_state.page == "live":

    # ── Top filter row ────────────────────────────────────────
    f1,f2,f3,f4 = st.columns([1,1,1,1])
    with f1:
        date_range = st.selectbox("📅 Date Range",
            ["Last 24 Hours","Last 7 Days","All Time"], key="live_dr")
        dr_key = {"Last 24 Hours":"24h","Last 7 Days":"7d","All Time":"all"}[date_range]
    
    raw_live = load_live(dr_key)

    if len(raw_live) == 0:
        st.warning("No live data yet. GitHub Actions collects every 3 hours.")
        st.stop()

    all_cats      = sorted(raw_live["category"].dropna().unique())
    all_countries = sorted(raw_live["country"].dropna().unique())
    all_sents     = ["positive","neutral","negative"]

    
    with f2:
        sel_cats = st.selectbox("🎬 Category", ["All"] + list(all_cats), key="lc")
    with f3:
        sel_countries = st.selectbox("🌍 Country", ["All"] + list(all_countries), key="lco")
    with f4:
        sel_sent = st.selectbox("😊 Sentiment", ["All","positive","neutral","negative"], key="ls")

# Apply filters
    df = raw_live.copy()
    if sel_cats != "All":
        df = df[df["category"] == sel_cats]
    if sel_countries != "All":
        df = df[df["country"] == sel_countries]
    if sel_sent != "All":
        df = df[df["sentiment"] == sel_sent]

    if len(df) == 0:
        st.warning("No data matches filters.")
        st.stop()

    total_runs   = raw_live["fetch_time"].nunique()
    latest_fetch = raw_live["fetch_time"].max()
    pos_pct      = round(len(df[df["sentiment"]=="positive"])/len(df)*100,1)

    # ── KPI Row ───────────────────────────────────────────────
    st.markdown(f"""
    <div class="kpi-row">
      <div class="kpi-card">
        <div class="kpi-value">{len(df):,}</div>
        <div class="kpi-label">Total Videos</div>
        <div style="font-size:12px;color:#adbac7;margin-top:6px"></div>
      </div>
      <div class="kpi-card">
        <div class="kpi-value">{int(df['views'].sum()/1e9 if df['views'].sum()>1e9 else df['views'].sum()/1e6):.1f}{"B" if df['views'].sum()>1e9 else "M"}</div>
        <div class="kpi-label">Total Views</div>
        <div style="font-size:12px;color:#3fb950;margin-top:6px">↑ Lifetime aggregate</div>
      </div>
      <div class="kpi-card">
        <div class="kpi-value">{int(df['likes'].sum()/1e6):.1f}M</div>
        <div class="kpi-label">Total Likes</div>
        <div style="font-size:12px;color:#adbac7;margin-top:6px">Across all categories</div>
      </div>
      <div class="kpi-card">
        <div class="kpi-value">{int(df['comments'].sum()/1e6):.1f}M</div>
        <div class="kpi-label">Total Comments</div>
        <div style="font-size:12px;color:#adbac7;margin-top:6px">Community engagement</div>
      </div>
      <div class="kpi-card">
        <div class="kpi-value kpi-positive">{round(df['likes'].sum()/max(df['views'].sum(),1)*100,2)}%</div>
        <div class="kpi-label">Avg Like Rate</div>
        <div style="font-size:12px;color:#3fb950;margin-top:6px">↑ Likes per 100 views</div>
      </div>
      <div class="kpi-card">
        <div class="kpi-value">{round(df['comments'].sum()/max(df['views'].sum(),1)*100,3)}%</div>
        <div class="kpi-label">Avg Comment Rate</div>
        <div style="font-size:12px;color:#adbac7;margin-top:6px">↑ Comments per 100 views</div>
      </div>
      <div class="kpi-card">
        <div class="kpi-value kpi-positive">{pos_pct}%</div>
        <div class="kpi-label">Positive Sentiment</div>
      </div>
      <div class="kpi-card">
        <div class="kpi-value">{total_runs}</div>
        <div class="kpi-label">Collection Runs</div>
      </div>
    </div>
    """, unsafe_allow_html=True)

    st.markdown('<div class="sec-title">Descriptive Analytics — What is happening?</div>', unsafe_allow_html=True)

    with st.container():
        st.markdown('<div class="content">', unsafe_allow_html=True)

        # Row 1: World map + Top video per country
        r1c1, r1c2 = st.columns([3, 2])

        with r1c1:
            st.markdown('<div class="chart-wrap"><div class="chart-title">🌍 Trending Videos World Map</div>', unsafe_allow_html=True)
            cmap = df.groupby("country").agg(
                total=("video_id","count"), avg_views=("views","mean")).reset_index()
            cmap["lat"] = cmap["country"].map(lambda x: COUNTRY_COORDS.get(x,{}).get("lat",0))
            cmap["lon"] = cmap["country"].map(lambda x: COUNTRY_COORDS.get(x,{}).get("lon",0))
            cmap = cmap[cmap["lat"]!=0]
            fig_map = px.scatter_geo(cmap, lat="lat", lon="lon",
                size="total", color="avg_views",
                hover_name="country",
                hover_data={"total":True,"avg_views":":.0f","lat":False,"lon":False},
                color_continuous_scale="Blues", size_max=40,
                projection="natural earth")
            fig_map.update_layout(
                paper_bgcolor="#161b22", geo=dict(
                    bgcolor="#161b22", landcolor="#21262d",
                    oceancolor="#0d1117", showocean=True,
                    coastlinecolor="#444d56", showframe=False,
                    showcoastlines=True),
                height=320, margin=dict(l=0,r=0,t=0,b=0),
                coloraxis_colorbar=dict(tickfont=dict(color="#adbac7", size=10)))
            st.plotly_chart(fig_map, use_container_width=True)
            st.markdown('<div class="chart-caption">Which countries have the most trending YouTube content right now?</div></div>', unsafe_allow_html=True)

        with r1c2:
            st.markdown('<div class="chart-wrap"><div class="chart-title">🏆 #1 Trending Video per Country</div>', unsafe_allow_html=True)
            latest_time = df["fetch_time"].max()
            latest_df   = df[df["fetch_time"]==latest_time]
            top_country = latest_df.sort_values("views",ascending=False).groupby("country").first().reset_index()
            top_country["short"] = top_country["title"].str[:30]+"..."
            top_country = top_country.sort_values("views", ascending=True)
            fig_tc = px.bar(top_country,
                x="views", y="country", orientation="h",
                color="category", text="short",
                color_discrete_sequence=COLORS)
            fig_tc.update_traces(textposition="inside", textfont_size=11)
            fig_tc.update_layout(
                plot_bgcolor="#161b22", paper_bgcolor="#161b22",
                font=dict(color="#c9d1d9", size=12), height=320,
                margin=dict(l=8,r=8,t=24,b=8), showlegend=True,
                xaxis=dict(gridcolor="#21262d", linecolor="#444d56", tickfont=dict(size=11, color="#adbac7")),
                yaxis=dict(gridcolor="#21262d", linecolor="#444d56", tickfont=dict(size=11, color="#adbac7")),
                legend=dict(bgcolor="#161b22", font=dict(color="#c9d1d9", size=10),
                        orientation="h", yanchor="bottom", y=1.02))
            st.plotly_chart(fig_tc, use_container_width=True)
            st.markdown('<div class="chart-caption">Top trending video in each country from latest collection run</div></div>', unsafe_allow_html=True)

        # Row 2: Category Performance + View Growth (2 equal columns)
        r2c1, r2c2 = st.columns(2)

        with r2c1:
            _hL, _hR = st.columns([3, 1])
            with _hR:
                cat_metric = st.selectbox("cat_m", ["Views", "Likes", "Comments", "Videos"], key="cat_metric_live", label_visibility="collapsed")
            st.markdown(f'<div class="chart-wrap"><div class="chart-header-row"><div class="chart-header-title">📊 Category Performance by {cat_metric}</div></div>', unsafe_allow_html=True)
            cat_agg = df.groupby("category").agg(
                avg_views    = ("views",    "mean"),
                avg_likes    = ("likes",    "mean"),
                avg_comments = ("comments", "mean"),
                total_videos = ("video_id", "count"),
                avg_eng      = ("engagement","mean")
            ).reset_index()
            metric_map = {
                "Views":    ("avg_views",    "Avg Views"),
                "Likes":    ("avg_likes",    "Avg Likes"),
                "Comments": ("avg_comments", "Avg Comments"),
                "Videos":   ("total_videos", "Total Videos"),
            }
            col_key, col_label = metric_map[cat_metric]
            cat_agg = cat_agg.sort_values(col_key, ascending=True)
            fig_cat = px.bar(cat_agg, x=col_key, y="category", orientation="h",
                color="avg_eng", color_continuous_scale="Blues",
                labels={col_key: col_label, "category": "", "avg_eng": "Engagement"})
            fig_cat.update_layout(**dark(320))
            st.plotly_chart(fig_cat, use_container_width=True)
            st.markdown(f'<div class="chart-caption">Which category dominates in {cat_metric.lower()} right now?</div></div>', unsafe_allow_html=True)

        with r2c2:
            st.markdown('<div class="chart-wrap"><div class="chart-title">📈 View Growth — Top 5 Videos</div>', unsafe_allow_html=True)
            if total_runs > 1:
                top5  = df.groupby("title")["views"].max().nlargest(5).index.tolist()
                gdf   = df[df["title"].isin(top5)].copy()
                gdf["short"] = gdf["title"].str[:20]+"..."
                gdf = gdf.sort_values("fetch_time")
                fig_g = px.line(gdf, x="fetch_time", y="views", color="short",
                    markers=True, color_discrete_sequence=COLORS,
                    labels={"fetch_time":"","views":"Views","short":""})
                fig_g.update_layout(**dark(320, showlegend=True,
                    legend=dict(bgcolor="#161b22", font=dict(color="#c9d1d9", size=11),
                               orientation="v", x=1.01)))
                st.plotly_chart(fig_g, use_container_width=True)
                st.markdown('<div class="chart-caption">How view counts grow across collection runs</div></div>', unsafe_allow_html=True)
            else:
                st.info("Need 2+ runs for growth chart")
                st.markdown('</div>', unsafe_allow_html=True)

        # ── Top 10 Videos + Top 10 Channels with toggles ─────
        st.markdown("---")
        r_top_c1, r_top_c2 = st.columns(2)

        with r_top_c1:
            _vL, _vR = st.columns([3, 1])
            with _vR:
                vid_metric = st.selectbox("vid_m", ["Views", "Likes", "Comments", "Videos"], key="vid_metric_live", label_visibility="collapsed")
            st.markdown(f'<div class="chart-wrap"><div class="chart-header-row"><div class="chart-header-title">🏆 Top 10 Videos by {vid_metric}</div></div>', unsafe_allow_html=True)
            vid_col_map = {
                "Views":    ("views",    "sum", "Total Views"),
                "Likes":    ("likes",    "sum", "Total Likes"),
                "Comments": ("comments", "sum", "Total Comments"),
                "Videos":   ("video_id", "count", "Video Count"),
            }
            vid_col, vid_agg_fn, vid_label = vid_col_map[vid_metric]
            # Group across ALL runs so we always get 10 results
            if vid_metric == "Videos":
                top10_v = df.groupby("title").agg(
                    value    = ("video_id", "count"),
                    category = ("category", "first"),
                ).reset_index()
            else:
                top10_v = df.groupby("title").agg(
                    value    = (vid_col, "sum"),
                    category = ("category", "first"),
                ).reset_index()
            top10_v = top10_v.nlargest(10, "value").sort_values("value", ascending=True)
            top10_v["short"] = top10_v["title"].str[:35] + "..."
            fig_top10_v = px.bar(
                top10_v, x="value", y="short", orientation="h",
                color="category", color_discrete_sequence=COLORS,
                labels={"value": vid_label, "short": "", "category": ""}
            )
            fig_top10_v.update_layout(**dark(340, showlegend=True,
                legend=dict(bgcolor="#161b22", font=dict(color="#c9d1d9", size=11),
                           orientation="h", yanchor="bottom", y=1.02)))
            st.plotly_chart(fig_top10_v, use_container_width=True)
            st.markdown(f'<div class="chart-caption">Top 10 videos by {vid_metric.lower()} — aggregated across all collection runs</div></div>', unsafe_allow_html=True)

        with r_top_c2:
            _cL, _cR = st.columns([3, 1])
            with _cR:
                ch_metric = st.selectbox("ch_m", ["Views", "Likes", "Comments", "Videos"], key="ch_metric_live", label_visibility="collapsed")
            st.markdown(f'<div class="chart-wrap"><div class="chart-header-row"><div class="chart-header-title">📡 Top 10 Channels by {ch_metric}</div></div>', unsafe_allow_html=True)
            ch_col_map = {
                "Views":    ("views",    "sum", "Total Views"),
                "Likes":    ("likes",    "sum", "Total Likes"),
                "Comments": ("comments", "sum", "Total Comments"),
                "Videos":   ("video_id", "count", "Video Count"),
            }
            ch_col, ch_agg_fn, ch_label = ch_col_map[ch_metric]
            if ch_metric == "Videos":
                ch_agg = df.groupby("channel").agg(value=("video_id","count")).reset_index()
            else:
                ch_agg = df.groupby("channel").agg(value=(ch_col,"sum")).reset_index()
            top10_ch = ch_agg.nlargest(10, "value").sort_values("value", ascending=True)
            fig_top10_ch = px.bar(
                top10_ch, x="value", y="channel", orientation="h",
                color="value", color_continuous_scale="Blues",
                labels={"value": ch_label, "channel": ""}
            )
            fig_top10_ch.update_layout(**dark(340))
            st.plotly_chart(fig_top10_ch, use_container_width=True)
            st.markdown(f'<div class="chart-caption">Top 10 channels by {ch_metric.lower()} — aggregated across all runs</div></div>', unsafe_allow_html=True)

        st.markdown("---")
        st.markdown('<div class="sec-title">Diagnostic Analytics — Why is it happening?</div>', unsafe_allow_html=True)

        # Row 3: Correlation + Heatmap
        r3c1, r3c2 = st.columns(2)

        with r3c1:
            st.markdown('<div class="chart-wrap"><div class="chart-title">🔥 Engagement Heatmap — Category × Country</div>', unsafe_allow_html=True)
            if "country" in df.columns and df["country"].nunique() > 1:
                hmap = df.groupby(["category","country"])["engagement"].mean().reset_index()
                hpiv = hmap.pivot(index="category", columns="country", values="engagement").fillna(0)
                fig_hm = px.imshow(hpiv, color_continuous_scale="Blues",
                    text_auto=".3f", aspect="auto")
                fig_hm.update_layout(**dark(300))
                fig_hm.update_traces(textfont_size=11)
                st.plotly_chart(fig_hm, use_container_width=True)
            else:
                st.info("Need multiple countries for heatmap")
            st.markdown('<div class="chart-caption">Which Category + Country combination drives most engagement?</div></div>', unsafe_allow_html=True)

        with r3c2:
            st.markdown('<div class="chart-wrap"><div class="chart-title">🔵 Views vs Likes — Live Scatter</div>', unsafe_allow_html=True)
            samp = df.sample(min(800,len(df)), random_state=42)
            fig_sc = px.scatter(samp, x="views", y="likes",
                color="category", size="engagement",
                hover_data=["title","country"],
                color_discrete_sequence=COLORS, opacity=0.7,
                labels={"views":"Views","likes":"Likes","category":""})
            fig_sc.update_layout(**dark(300, showlegend=True,
                legend=dict(bgcolor="#161b22", font=dict(color="#c9d1d9", size=10),
                           orientation="h",yanchor="bottom",y=1.02,x=0)))
            st.plotly_chart(fig_sc, use_container_width=True)
            st.markdown('<div class="chart-caption">Does higher viewership always mean more likes?</div></div>', unsafe_allow_html=True)

        # Row 4: Category trend + Sentiment by category
        r4c1, r4c2 = st.columns(2)

        with r4c1:
            st.markdown('<div class="chart-wrap"><div class="chart-title">📉 Category Trend Over Time</div>', unsafe_allow_html=True)
            if total_runs > 1:
                ctrend = df.groupby(["fetch_time","category"])["views"].mean().reset_index()
                ctrend["fetch_time"] = ctrend["fetch_time"].dt.strftime("%m-%d %H:%M")
                fig_ct = px.line(ctrend, x="fetch_time", y="views", color="category",
                    markers=True, color_discrete_sequence=COLORS,
                    labels={"fetch_time":"","views":"Avg Views","category":""})
                fig_ct.update_layout(**dark(280, showlegend=True,
                    legend=dict(bgcolor="#161b22", font=dict(color="#c9d1d9", size=10),
                               orientation="h",yanchor="bottom",y=1.02)))
                st.plotly_chart(fig_ct, use_container_width=True)
                st.markdown('<div class="chart-caption">Which categories are gaining or losing popularity?</div></div>', unsafe_allow_html=True)
            else:
                st.info("Need 2+ runs for trend chart")
                st.markdown('</div>', unsafe_allow_html=True)

        with r4c2:
            st.markdown('<div class="chart-wrap"><div class="chart-title">🎭 Sentiment by Category</div>', unsafe_allow_html=True)
            scat = df.groupby(["category","sentiment"]).size().reset_index(name="count")
            fig_scat = px.bar(scat, x="category", y="count", color="sentiment",
                barmode="stack",
                color_discrete_map={"positive":"#3fb950","neutral":"#8b949e","negative":"#f85149"},
                labels={"count":"Videos","category":"","sentiment":""})
            fig_scat.update_layout(**dark(280, showlegend=True,
                legend=dict(bgcolor="#161b22", font=dict(color="#c9d1d9", size=10),
                           orientation="h",yanchor="bottom",y=1.02)))
            st.plotly_chart(fig_scat, use_container_width=True)
            st.markdown('<div class="chart-caption">Which categories use more emotional titles?</div></div>', unsafe_allow_html=True)

        st.markdown("---")
        st.markdown('<div class="sec-title">Predictive Analytics — What will happen?</div>', unsafe_allow_html=True)

        # ── Forecast using linear regression on run history ───
        fcast_df = compute_forecast(raw_live, horizon_h=24)

        if len(fcast_df) > 0:
            best_growth  = fcast_df.iloc[0]
            worst_growth = fcast_df.iloc[-1]
            fastest_cat  = best_growth["category"]
            fastest_pct  = best_growth["growth_pct"]
            slowest_cat  = worst_growth["category"]
            slowest_pct  = worst_growth["growth_pct"]
            total_fcast  = int(fcast_df["forecast_avg"].sum())
            n_growing    = len(fcast_df[fcast_df["growth_pct"] > 0])

            color_fast = "#3fb950" if fastest_pct >= 0 else "#f85149"
            color_slow = "#f85149" if slowest_pct < 0 else "#d29922"
            arrow_fast = "↑" if fastest_pct >= 0 else "↓"
            arrow_slow = "↓" if slowest_pct < 0 else "→"

            st.markdown(f"""
            <div class="kpi-row" style="padding-top:8px; padding-bottom:12px;">
              <div class="kpi-card">
                <div class="kpi-value" style="color:{color_fast}; font-size:26px;">{arrow_fast} {abs(fastest_pct):.1f}%</div>
                <div class="kpi-label">Fastest Growing</div>
                <div style="font-size:12px;color:#adbac7;margin-top:6px">{fastest_cat} · next 24h</div>
              </div>
              <div class="kpi-card">
                <div class="kpi-value" style="color:{color_slow}; font-size:26px;">{arrow_slow} {abs(slowest_pct):.1f}%</div>
                <div class="kpi-label">Slowest / Declining</div>
                <div style="font-size:12px;color:#adbac7;margin-top:6px">{slowest_cat} · next 24h</div>
              </div>
              <div class="kpi-card">
                <div class="kpi-value" style="font-size:26px;">{total_fcast:,}</div>
                <div class="kpi-label">Forecast Total Views</div>
                <div style="font-size:12px;color:#adbac7;margin-top:6px">Sum across categories · 24h</div>
              </div>
              <div class="kpi-card">
                <div class="kpi-value" style="color:#3fb950; font-size:26px;">{n_growing}/{len(fcast_df)}</div>
                <div class="kpi-label">Categories Growing</div>
                <div style="font-size:12px;color:#3fb950;margin-top:6px">Linear regression model</div>
              </div>
            </div>
            """, unsafe_allow_html=True)

        r5c1, r5c2 = st.columns(2)

        with r5c1:
            # Chart 1: 24h Forecast overlay bar
            st.markdown('<div class="chart-wrap"><div class="chart-title">📈 24h View Forecast — Current vs Predicted</div>', unsafe_allow_html=True)
            if len(fcast_df) > 0:
                fig_fc = go.Figure()
                fig_fc.add_trace(go.Bar(
                    x=fcast_df["current_avg"], y=fcast_df["category"],
                    orientation="h", name="Current Avg Views",
                    marker_color="#38bdf8", opacity=0.55,
                ))
                fig_fc.add_trace(go.Bar(
                    x=fcast_df["forecast_avg"], y=fcast_df["category"],
                    orientation="h", name="Forecast (24h)",
                    marker_color="#818cf8", opacity=0.9,
                ))
                fig_fc.update_layout(
                    plot_bgcolor="#161b22", paper_bgcolor="#161b22",
                    font=dict(color="#c9d1d9", size=11), height=320,
                    margin=dict(l=8, r=8, t=24, b=8), barmode="overlay",
                    xaxis=dict(title="Avg Views", gridcolor="#21262d", linecolor="#444d56", tickfont=dict(size=11, color="#adbac7"), title_font=dict(color="#c9d1d9")),
                    yaxis=dict(gridcolor="#21262d", linecolor="#444d56"),
                    legend=dict(bgcolor="#161b22", font=dict(color="#c9d1d9", size=10),
                                orientation="h", yanchor="bottom", y=1.02),
                    showlegend=True,
                )
                st.plotly_chart(fig_fc, use_container_width=True)
            else:
                st.info("Need 2+ collection runs for forecast")
            st.markdown('<div class="chart-caption">Blue = current avg views · Purple = predicted avg in next 24h via linear regression on run history</div></div>', unsafe_allow_html=True)

        with r5c2:
            # Chart 2: Forecast Growth % bar (green/red)
            st.markdown('<div class="chart-wrap"><div class="chart-title">📊 Forecast Growth % — Next 24h vs Now</div>', unsafe_allow_html=True)
            if len(fcast_df) > 0:
                fcast_sorted = fcast_df.sort_values("growth_pct")
                bar_colors   = ["#3fb950" if g >= 0 else "#f85149"
                                for g in fcast_sorted["growth_pct"]]
                fig_gr = go.Figure(go.Bar(
                    x=fcast_sorted["growth_pct"], y=fcast_sorted["category"],
                    orientation="h", marker_color=bar_colors,
                    text=[f"{g:+.1f}%" for g in fcast_sorted["growth_pct"]],
                    textposition="outside", textfont=dict(size=10, color="#e6edf3"),
                ))
                fig_gr.add_vline(x=0, line_color="#30363d", line_width=1)
                fig_gr.update_layout(
                    plot_bgcolor="#161b22", paper_bgcolor="#161b22",
                    font=dict(color="#c9d1d9", size=11), height=320,
                    margin=dict(l=8, r=70, t=24, b=8), showlegend=False,
                    xaxis=dict(title="Growth %", gridcolor="#21262d", title_font=dict(color="#c9d1d9"),
                               linecolor="#444d56", zeroline=False),
                    yaxis=dict(gridcolor="#21262d", linecolor="#444d56"),
                )
                st.plotly_chart(fig_gr, use_container_width=True)
            else:
                st.info("Need 2+ collection runs for forecast")
            st.markdown('<div class="chart-caption">Green = predicted growth · Red = predicted decline · Based on linear trend across collection runs</div></div>', unsafe_allow_html=True)

        r5c3, r5c4 = st.columns(2)

        with r5c3:
            # Chart 3: Sentiment vs Engagement (kept, moved here)
            st.markdown('<div class="chart-wrap"><div class="chart-title">📊 Sentiment vs Engagement</div>', unsafe_allow_html=True)
            se = df.groupby("sentiment").agg(
                avg_views=("views","mean"), avg_eng=("engagement","mean")).reset_index()
            fig_se = go.Figure()
            fig_se.add_trace(go.Bar(x=se["sentiment"], y=se["avg_views"],
                name="Avg Views", marker_color="#38bdf8", yaxis="y"))
            fig_se.add_trace(go.Scatter(x=se["sentiment"], y=se["avg_eng"],
                name="Engagement", yaxis="y2",
                line=dict(color="#f472b6", width=3), mode="lines+markers"))
            fig_se.update_layout(
                plot_bgcolor="#161b22", paper_bgcolor="#161b22",
                font=dict(color="#c9d1d9", size=11), height=280,
                margin=dict(l=8, r=8, t=24, b=8),
                xaxis=dict(gridcolor="#21262d"),
                yaxis=dict(title="Avg Views", gridcolor="#21262d"),
                yaxis2=dict(title="Engagement", overlaying="y", side="right"),
                legend=dict(bgcolor="#161b22", font=dict(color="#c9d1d9", size=10)))
            st.plotly_chart(fig_se, use_container_width=True)
            st.markdown('<div class="chart-caption">Do positive titles drive more engagement? Dual-axis view.</div></div>', unsafe_allow_html=True)

        with r5c4:
            # Chart 4: Keyword cloud
            st.markdown('<div class="chart-wrap"><div class="chart-title">☁️ Live Trending Keywords</div>', unsafe_allow_html=True)
            kw = get_keywords(df["title"])
            if kw:
                wc = WordCloud(width=600, height=260, background_color="#161b22",
                               colormap="Blues", max_words=40).generate_from_frequencies(kw)
                fig_wc, ax = plt.subplots(figsize=(6, 2.6))
                fig_wc.patch.set_facecolor("#161b22")
                ax.imshow(wc, interpolation="bilinear")
                ax.axis("off")
                st.pyplot(fig_wc)
            st.markdown('<div class="chart-caption">Most frequent words in live trending titles — signals emerging topics</div></div>', unsafe_allow_html=True)

        st.markdown("---")
        st.markdown('<div class="sec-title">Prescriptive Analytics — What should be done?</div>',
                    unsafe_allow_html=True)
        st.markdown('<p style="font-size:13px;color:#adbac7;margin:0 0 12px 24px;">⚠️ Global recommendations based on all data — not affected by filters above. These are creator action items.</p>', unsafe_allow_html=True)

        # ── Load Gold layer tables from Supabase ─────────────
        try:
            gold_results = {}
            with httpx.Client(timeout=30) as _c:
                for _t in ["gold_category","gold_country","gold_sentiment","gold_trending"]:
                    _r = _c.get(
                        f"{SUPABASE_URL}/rest/v1/{_t}?select=*&order=fetch_time.desc&limit=500",
                        headers=SUPABASE_H)
                    gold_results[_t] = pd.DataFrame(_r.json()) if _r.status_code==200 and _r.json() else pd.DataFrame()
        except Exception:
            gold_results = {}

        gold_cat     = gold_results.get("gold_category",  pd.DataFrame())
        gold_co      = gold_results.get("gold_country",   pd.DataFrame())
        gold_sent_df = gold_results.get("gold_sentiment", pd.DataFrame())
        gold_tr      = gold_results.get("gold_trending",  pd.DataFrame())

        # ── Derive all recommendation values ─────────────────
        # Best & worst category by engagement
        if len(gold_cat) > 0:
            lt  = gold_cat["fetch_time"].max()
            gc  = gold_cat[gold_cat["fetch_time"] == lt].sort_values("avg_engagement_rate", ascending=False)
            best_cat        = gc.iloc[0]["category"]
            best_cat_eng    = round(gc.iloc[0]["avg_engagement_rate"], 4)
            worst_cat       = gc.iloc[-1]["category"]
            worst_cat_eng   = round(gc.iloc[-1]["avg_engagement_rate"], 4)
            best_views_cat  = gc.sort_values("avg_views", ascending=False).iloc[0]["category"]
            best_views_val  = int(gc["avg_views"].max())
            # Fastest growing from forecast
            fc = compute_forecast(raw_live, horizon_h=24)
            fastest_cat     = fc.iloc[0]["category"] if len(fc) > 0 else best_cat
            fastest_pct     = fc.iloc[0]["growth_pct"] if len(fc) > 0 else 0.0
        else:
            cat_eng         = df.groupby("category")["engagement"].mean()
            best_cat        = cat_eng.idxmax()
            best_cat_eng    = round(cat_eng.max(), 4)
            worst_cat       = cat_eng.idxmin()
            worst_cat_eng   = round(cat_eng.min(), 4)
            best_views_cat  = df.groupby("category")["views"].mean().idxmax()
            best_views_val  = int(df.groupby("category")["views"].mean().max())
            fc              = compute_forecast(raw_live, horizon_h=24)
            fastest_cat     = fc.iloc[0]["category"] if len(fc) > 0 else best_cat
            fastest_pct     = fc.iloc[0]["growth_pct"] if len(fc) > 0 else 0.0

        # Best country
        if len(gold_co) > 0:
            lt           = gold_co["fetch_time"].max()
            gc2          = gold_co[gold_co["fetch_time"] == lt]
            best_country = gc2.sort_values("avg_engagement_rate", ascending=False).iloc[0]["country"]
            best_co_eng  = round(gc2["avg_engagement_rate"].max(), 4)
        else:
            best_country = df.groupby("country")["engagement"].mean().idxmax()
            best_co_eng  = round(df.groupby("country")["engagement"].mean().max(), 4)

        # Best sentiment
        if len(gold_sent_df) > 0:
            lt            = gold_sent_df["fetch_time"].max()
            gs            = gold_sent_df[gold_sent_df["fetch_time"] == lt]
            best_sent     = gs.sort_values("avg_engagement_rate", ascending=False).iloc[0]["sentiment"]
            best_sent_eng = round(gs["avg_engagement_rate"].max(), 4)
        else:
            best_sent     = df.groupby("sentiment")["engagement"].mean().idxmax()
            best_sent_eng = round(df.groupby("sentiment")["engagement"].mean().max(), 4)

        # Top trending video
        if len(gold_tr) > 0:
            lt        = gold_tr["fetch_time"].max()
            gt        = gold_tr[gold_tr["fetch_time"] == lt].sort_values("trending_score", ascending=False).iloc[0]
            top_title = gt["title"][:32] + "..."
            top_score = round(float(gt["trending_score"]), 3)
            top_cat   = gt["category"]
        else:
            top_v     = df.nlargest(1, "views").iloc[0]
            top_title = top_v["title"][:32] + "..."
            top_score = 0.0
            top_cat   = top_v["category"]

        corr_val     = round(df[["views","likes"]].corr().iloc[0,1], 3)
        fastest_arrow = "↑" if fastest_pct >= 0 else "↓"
        fastest_color = "#3fb950" if fastest_pct >= 0 else "#f85149"

        # ── Row 1: Action cards (like friend's "Publish NOW" style) ──
        st.markdown("#### 🎯 Model-Driven Action Cards")
        ac1, ac2, ac3 = st.columns(3)

        ac1.markdown(f"""
        <div class="ins-box" style="border-left:4px solid #3fb950; margin-bottom:12px;">
            <div class="ins-title" style="color:#3fb950;">🚀 Publish NOW in {best_cat}</div>
            <div style="font-size:13px;color:#e6edf3;margin:8px 0;font-weight:600;">Best engagement category right now</div>
            <div class="ins-text">Avg engagement rate: <b>{best_cat_eng}</b><br>
            Avg views: <b>{best_views_val:,}</b><br>
            Source: Gold Layer · Live aggregation</div>
        </div>""", unsafe_allow_html=True)

        ac2.markdown(f"""
        <div class="ins-box" style="border-left:4px solid #38bdf8; margin-bottom:12px;">
            <div class="ins-title" style="color:#38bdf8;">📈 Fastest Growing: {fastest_cat}</div>
            <div style="font-size:13px;color:#e6edf3;margin:8px 0;font-weight:600;">{fastest_arrow} {abs(fastest_pct):.1f}% predicted growth</div>
            <div class="ins-text">Linear regression on run history.<br>
            Upload now to ride the momentum wave.<br>
            Source: Predictive · 24h forecast</div>
        </div>""", unsafe_allow_html=True)

        ac3.markdown(f"""
        <div class="ins-box" style="border-left:4px solid #f85149; margin-bottom:12px;">
            <div class="ins-title" style="color:#f85149;">⚠️ Cool Down: {worst_cat}</div>
            <div style="font-size:13px;color:#e6edf3;margin:8px 0;font-weight:600;">Lowest engagement category</div>
            <div class="ins-text">Avg engagement rate: <b>{worst_cat_eng}</b><br>
            Pause paid promotions here.<br>
            Redirect budget to <b>{best_cat}</b>.</div>
        </div>""", unsafe_allow_html=True)

        # ── Row 2: Strategy cards ─────────────────────────────
        ac4, ac5, ac6 = st.columns(3)

        ac4.markdown(f"""
        <div class="ins-box" style="border-left:4px solid #818cf8; margin-bottom:12px;">
            <div class="ins-title" style="color:#818cf8;">🌍 Best Target Market</div>
            <div style="font-size:13px;color:#e6edf3;margin:8px 0;font-weight:600;">{best_country}</div>
            <div class="ins-text">Highest engagement rate: <b>{best_co_eng}</b><br>
            Target this audience for sponsorships.<br>
            Source: Gold Layer · Country aggregation</div>
        </div>""", unsafe_allow_html=True)

        ac5.markdown(f"""
        <div class="ins-box" style="border-left:4px solid #f472b6; margin-bottom:12px;">
            <div class="ins-title" style="color:#f472b6;">😊 Use {best_sent.title()} Titles</div>
            <div style="font-size:13px;color:#e6edf3;margin:8px 0;font-weight:600;">Engagement rate: {best_sent_eng}</div>
            <div class="ins-text">{best_sent.title()} sentiment titles drive highest engagement.<br>
            Views↔Likes correlation: <b>{corr_val}</b> (strong).<br>
            Source: Gold Layer · Sentiment aggregation</div>
        </div>""", unsafe_allow_html=True)

        ac6.markdown(f"""
        <div class="ins-box" style="border-left:4px solid #fbbf24; margin-bottom:12px;">
            <div class="ins-title" style="color:#fbbf24;">🏆 Stable Bet: {top_cat}</div>
            <div style="font-size:13px;color:#e6edf3;margin:8px 0;font-weight:600;">Top trending · Score: {top_score}</div>
            <div class="ins-text">"{top_title}"<br>
            Reliable baseline audience — good for long-term commitment deals.<br>
            Source: Gold Layer · Trending score</div>
        </div>""", unsafe_allow_html=True)

        # ── Summary insight bar ───────────────────────────────
        st.markdown(f"""
        <div style="background:#161b22;border:1px solid #30363d;border-radius:10px;
                    padding:14px 20px;margin-top:8px;display:flex;gap:32px;flex-wrap:wrap;">
            <div><span style="color:#adbac7;font-size:12px;text-transform:uppercase;letter-spacing:0.8px;">Best Category</span>
                 <div style="color:#3fb950;font-weight:700;font-size:15px;">{best_cat}</div></div>
            <div><span style="color:#adbac7;font-size:12px;text-transform:uppercase;letter-spacing:0.8px;">Best Country</span>
                 <div style="color:#38bdf8;font-weight:700;font-size:15px;">{best_country}</div></div>
            <div><span style="color:#adbac7;font-size:12px;text-transform:uppercase;letter-spacing:0.8px;">Best Tone</span>
                 <div style="color:#f472b6;font-weight:700;font-size:15px;">{best_sent.title()}</div></div>
            <div><span style="color:#adbac7;font-size:12px;text-transform:uppercase;letter-spacing:0.8px;">Fastest Growing</span>
                 <div style="color:{fastest_color};font-weight:700;font-size:15px;">{fastest_cat} {fastest_arrow}{abs(fastest_pct):.1f}%</div></div>
            <div><span style="color:#adbac7;font-size:12px;text-transform:uppercase;letter-spacing:0.8px;">Views↔Likes Corr</span>
                 <div style="color:#fbbf24;font-weight:700;font-size:15px;">{corr_val}</div></div>
            <div><span style="color:#adbac7;font-size:12px;text-transform:uppercase;letter-spacing:0.8px;">Data Source</span>
                 <div style="color:#8b949e;font-weight:700;font-size:15px;">Gold Layer · Live</div></div>
        </div>
        """, unsafe_allow_html=True)

            # Gold category chart
        # if len(gold_cat) > 0:
        #         st.markdown("#### 📊 Gold Layer — Category Aggregation")
        #         lt   = gold_cat["fetch_time"].max()
        #         gcat = gold_cat[gold_cat["fetch_time"]==lt].sort_values("avg_trending_score",ascending=False)
        #         fig_gold = go.Figure()
        #         fig_gold.add_trace(go.Bar(
        #             x=gcat["category"],y=gcat["avg_views"],
        #             name="Avg Views",marker_color="#38bdf8",yaxis="y"))
        #         fig_gold.add_trace(go.Scatter(
        #             x=gcat["category"],y=gcat["avg_engagement_rate"],
        #             name="Avg Engagement",yaxis="y2",
        #             line=dict(color="#f472b6",width=3),mode="lines+markers"))
        #         fig_gold.update_layout(
        #             plot_bgcolor="#161b22",paper_bgcolor="#161b22",
        #             font=dict(color="#c9d1d9", size=11),height=320,
        #             margin=dict(l=8,r=8,t=24,b=8),
        #             xaxis=dict(gridcolor="#21262d",tickangle=-20),
        #             yaxis=dict(title="Avg Views", gridcolor="#21262d", linecolor="#444d56", tickfont=dict(size=11, color="#adbac7"), title_font=dict(color="#c9d1d9")),
        #             yaxis2=dict(title="Avg Engagement Rate", overlaying="y", side="right", tickfont=dict(size=11, color="#adbac7"), title_font=dict(color="#c9d1d9")),
        #             showlegend=True,
        #             legend=dict(bgcolor="#161b22", font=dict(color="#c9d1d9", size=10),
        #                     orientation="h",yanchor="bottom",y=1.02))
        #         st.plotly_chart(fig_gold, use_container_width=True)
                # st.caption("Gold layer: groupBy(category).agg(avg_views, avg_engagement_rate, count)")

            # Gold trending table
        # if len(gold_tr) > 0:
        #         st.markdown("#### 🏆 Gold Layer — Top 10 Trending Videos")
        #         lt = gold_tr["fetch_time"].max()
        #         t10 = gold_tr[gold_tr["fetch_time"]==lt].nlargest(10,"trending_score")[
        #             ["title","category","country","views","engagement_rate","trending_score"]].copy()
        #         t10["title"]          = t10["title"].str[:35]+"..."
        #         t10["views"]          = t10["views"].apply(lambda x: f"{int(x):,}")
        #         t10["trending_score"] = t10["trending_score"].round(3)
        #         t10.columns = ["Title","Category","Country","Views","Engagement","Score"]
        #         st.dataframe(t10, use_container_width=True, hide_index=True)
        #         st.caption("Gold layer: orderBy(trending_score desc).limit(10)")

        # st.markdown('</div>', unsafe_allow_html=True)

# ══════════════════════════════════════════════════════════════
# HISTORICAL PAGE
# ══════════════════════════════════════════════════════════════
else:
    (kpis, cat_perf, country_h, channels, day_perf, hour_perf, corr,
     eng_hmap, title_eng, tag_eng, sent_dist, sent_eng, sent_cat,
     keywords, trending, prescr, scatter, liker) = load_hist()
    kpi = kpis.iloc[0]

    # ── Top filter row ────────────────────────────────────────
    f1,f2,f3 = st.columns(3)
    all_cats_h     = sorted(cat_perf["category"].unique())
    all_countries_h= sorted(country_h["publish_country"].unique())

    with f1:
        sel_cats_h = st.selectbox("🎬 Category", ["All"] + list(all_cats_h), key="hc")
    with f2:
        sel_countries_h = st.selectbox("🌍 Country", ["All"] + list(all_countries_h), key="hco")
    with f3:
        sel_sent_h = st.selectbox("😊 Sentiment", ["All","positive","neutral","negative"], key="hs")

    cat_f     = cat_perf if sel_cats_h=="All" else cat_perf[cat_perf["category"]==sel_cats_h]
    country_f = country_h if sel_countries_h=="All" else country_h[country_h["publish_country"]==sel_countries_h]
    scatter_f = scatter.copy()
    if sel_cats_h != "All": scatter_f = scatter_f[scatter_f["category"]==sel_cats_h]
    if sel_sent_h != "All": scatter_f = scatter_f[scatter_f["sentiment"]==sel_sent_h]
    eng_f     = eng_hmap if sel_cats_h=="All" else eng_hmap[eng_hmap["category"]==sel_cats_h]
    sent_cat_f= sent_cat if sel_cats_h=="All" else sent_cat[sent_cat["category"]==sel_cats_h]
    day_f     = day_perf
    liker_f   = liker if sel_cats_h=="All" else liker[liker["category"]==sel_cats_h]

    pos_pct_h = round(sent_dist[sent_dist["sentiment"]=="positive"]["count"].sum() /
                      sent_dist["count"].sum() * 100, 1)

    # ── KPI Row ───────────────────────────────────────────────
    st.markdown(f"""
    <div class="kpi-row">
      <div class="kpi-card">
        <div class="kpi-value">{int(kpi['total_videos']):,}</div>
        <div class="kpi-label">Total Videos</div>
      </div>
      <div class="kpi-card">
        <div class="kpi-value">{int(kpi['avg_views']):,}</div>
        <div class="kpi-label">Avg Views</div>
      </div>
      <div class="kpi-card">
        <div class="kpi-value">{int(kpi['avg_likes']):,}</div>
        <div class="kpi-label">Avg Likes</div>
      </div>
      <div class="kpi-card">
        <div class="kpi-value kpi-positive">{pos_pct_h}%</div>
        <div class="kpi-label">Positive Sentiment</div>
      </div>
      <div class="kpi-card">
        <div class="kpi-value">{int(kpi['total_channels']):,}</div>
        <div class="kpi-label">Total Channels</div>
      </div>
      <div class="kpi-card">
        <div class="kpi-value">{int(kpi['total_countries']):,}</div>
        <div class="kpi-label">Countries</div>
      </div>
    </div>
    """, unsafe_allow_html=True)

    st.markdown('<div class="sec-title">Descriptive Analytics — What is happening?</div>', unsafe_allow_html=True)

    with st.container():
        st.markdown('<div class="content">', unsafe_allow_html=True)

        r1c1, r1c2 = st.columns([3,2])

        with r1c1:
            st.markdown('<div class="chart-wrap"><div class="chart-title">🌍 Top Countries by Video Count</div>', unsafe_allow_html=True)
            fig_c = px.bar(country_f.head(15).sort_values("total_videos"),
                x="total_videos", y="publish_country", orientation="h",
                color="avg_engagement", color_continuous_scale="Blues",
                labels={"total_videos":"Total Videos","publish_country":"","avg_engagement":"Engagement"})
            fig_c.update_layout(**dark(320))
            st.plotly_chart(fig_c, use_container_width=True)
            st.markdown('<div class="chart-caption">Which countries produce the most trending YouTube content?</div></div>', unsafe_allow_html=True)

        with r1c2:
            st.markdown('<div class="chart-wrap"><div class="chart-title">📊 Category Views vs Engagement</div>', unsafe_allow_html=True)
            fig_ce = px.scatter(cat_f,
                x="avg_views", y="avg_engagement", size="total_videos",
                color="category", color_discrete_sequence=COLORS,
                hover_data=["avg_likes","total_videos"],
                labels={"avg_views":"Avg Views","avg_engagement":"Engagement","category":""})
            fig_ce.update_layout(**dark(320, showlegend=True,
                legend=dict(bgcolor="#161b22", font=dict(color="#c9d1d9", size=11),
                           orientation="h",yanchor="bottom",y=1.02)))
            st.plotly_chart(fig_ce, use_container_width=True)
            st.markdown('<div class="chart-caption">Which categories have both high views AND high engagement?</div></div>', unsafe_allow_html=True)

        r2c1, r2c2, r2c3 = st.columns(3)

        with r2c1:
            st.markdown('<div class="chart-wrap"><div class="chart-title">📅 Engagement by Day of Week</div>', unsafe_allow_html=True)
            day_f2 = day_f.copy()
            day_f2["published_day_of_week"] = pd.Categorical(
                day_f2["published_day_of_week"], categories=DAY_ORDER, ordered=True)
            day_f2 = day_f2.sort_values("published_day_of_week")
            fig_day = go.Figure()
            fig_day.add_trace(go.Bar(x=day_f2["published_day_of_week"],y=day_f2["avg_views"],
                name="Avg Views",marker_color="#38bdf8",yaxis="y"))
            fig_day.add_trace(go.Scatter(x=day_f2["published_day_of_week"],y=day_f2["avg_engagement"],
                name="Engagement",line=dict(color="#f472b6",width=2),
                yaxis="y2",mode="lines+markers"))
            fig_day.update_layout(
                plot_bgcolor="#161b22",paper_bgcolor="#161b22",
                font=dict(color="#c9d1d9", size=11),height=260,
                margin=dict(l=8,r=8,t=24,b=8),
                xaxis=dict(gridcolor="#21262d",tickfont=dict(size=10, color="#adbac7")),
                yaxis=dict(title="Avg Views", gridcolor="#21262d", linecolor="#444d56", tickfont=dict(size=11, color="#adbac7"), title_font=dict(color="#c9d1d9")),
                yaxis2=dict(title="Engagement", overlaying="y", side="right", tickfont=dict(size=11, color="#adbac7"), title_font=dict(color="#c9d1d9")),
                legend=dict(bgcolor="#161b22", font=dict(color="#c9d1d9", size=10),
                           orientation="h",yanchor="bottom",y=1.02))
            st.plotly_chart(fig_day, use_container_width=True)
            st.markdown('<div class="chart-caption">Which day drives the most engagement?</div></div>', unsafe_allow_html=True)

        with r2c2:
            st.markdown('<div class="chart-wrap"><div class="chart-title">🕐 Views by Publish Hour</div>', unsafe_allow_html=True)
            fig_hr = px.bar(hour_perf.sort_values("publish_hour"),
                x="publish_hour", y="avg_views",
                color="avg_views", color_continuous_scale="Blues",
                labels={"publish_hour":"Hour (24h)","avg_views":"Avg Views"})
            fig_hr.update_layout(**dark(260))
            st.plotly_chart(fig_hr, use_container_width=True)
            st.markdown('<div class="chart-caption">What upload time gets the most views?</div></div>', unsafe_allow_html=True)

        with r2c3:
            st.markdown('<div class="chart-wrap"><div class="chart-title">😊 Sentiment Distribution</div>', unsafe_allow_html=True)
            fig_sd = px.pie(sent_dist, values="count", names="sentiment", hole=0.55,
                color="sentiment",
                color_discrete_map={"positive":"#3fb950","neutral":"#8b949e","negative":"#f85149"})
            fig_sd.update_traces(textposition="inside", textinfo="percent+label", textfont_size=11)
            fig_sd.update_layout(**dark(260))
            st.plotly_chart(fig_sd, use_container_width=True)
            st.markdown('<div class="chart-caption">Overall tone of trending YouTube titles</div></div>', unsafe_allow_html=True)

        st.markdown("---")
        st.markdown('<div class="sec-title">Diagnostic Analytics — Why is it happening?</div>', unsafe_allow_html=True)

        r3c1, r3c2 = st.columns(2)

        with r3c1:
            st.markdown('<div class="chart-wrap"><div class="chart-title">🔥 Full Correlation Matrix</div>', unsafe_allow_html=True)
            fig_corr = px.imshow(corr, color_continuous_scale="RdBu_r",
                zmin=-1, zmax=1, text_auto=True, aspect="auto")
            fig_corr.update_layout(**dark(320))
            fig_corr.update_traces(textfont_size=11)
            st.plotly_chart(fig_corr, use_container_width=True)
            st.markdown('<div class="chart-caption">How do views, likes, comments, title length, tags, and sentiment relate?</div></div>', unsafe_allow_html=True)

        with r3c2:
            st.markdown('<div class="chart-wrap"><div class="chart-title">🔥 Engagement Heatmap — Category × Day</div>', unsafe_allow_html=True)
            if len(eng_f) > 0:
                ep = eng_f.pivot(index="category",columns="day",values="engagement_rate").fillna(0)
                dc = [d for d in DAY_ORDER if d in ep.columns]
                ep = ep[dc]
                fig_eh = px.imshow(ep, color_continuous_scale="Blues",
                    text_auto=".3f", aspect="auto")
                fig_eh.update_layout(**dark(320))
                fig_eh.update_traces(textfont_size=11)
                st.plotly_chart(fig_eh, use_container_width=True)
            st.markdown('<div class="chart-caption">Which Category + Day maximises engagement?</div></div>', unsafe_allow_html=True)

        r4c1, r4c2, r4c3 = st.columns(3)

        with r4c1:
            st.markdown('<div class="chart-wrap"><div class="chart-title">🔵 Views vs Likes Scatter</div>', unsafe_allow_html=True)
            fig_sc2 = px.scatter(scatter_f.sample(min(500,len(scatter_f)),random_state=42),
                x="views", y="likes", color="category",
                color_discrete_sequence=COLORS, opacity=0.6,
                hover_data=["title"],
                labels={"views":"Views","likes":"Likes","category":""})
            fig_sc2.update_layout(**dark(260))
            st.plotly_chart(fig_sc2, use_container_width=True)
            st.markdown('<div class="chart-caption">Views-likes relationship across categories</div></div>', unsafe_allow_html=True)

        with r4c2:
            st.markdown('<div class="chart-wrap"><div class="chart-title">📝 Title Length vs Engagement</div>', unsafe_allow_html=True)
            fig_tl = px.bar(title_eng,
                x="title_length_bucket", y="avg_engagement",
                color="avg_engagement", color_continuous_scale="Blues",
                labels={"title_length_bucket":"Title Length","avg_engagement":"Avg Engagement"})
            fig_tl.update_layout(**dark(260))
            st.plotly_chart(fig_tl, use_container_width=True)
            st.markdown('<div class="chart-caption">Does title length affect engagement?</div></div>', unsafe_allow_html=True)

        with r4c3:
            st.markdown('<div class="chart-wrap"><div class="chart-title">❤️ Like Ratio by Category</div>', unsafe_allow_html=True)
            fig_lr = px.bar(liker_f.sort_values("avg_like_ratio",ascending=False),
                x="category", y="avg_like_ratio",
                color="avg_like_ratio", color_continuous_scale="Greens",
                labels={"category":"","avg_like_ratio":"Like Ratio"})
            fig_lr.update_layout(
                plot_bgcolor="#161b22", paper_bgcolor="#161b22",
                font=dict(color="#c9d1d9", size=12), height=260,
                margin=dict(l=8,r=8,t=24,b=8), showlegend=False,
                xaxis=dict(tickangle=-30, gridcolor="#21262d", linecolor="#444d56", tickfont=dict(size=11, color="#adbac7")),
                yaxis=dict(gridcolor="#21262d", linecolor="#444d56", tickfont=dict(size=11, color="#adbac7")))
            st.plotly_chart(fig_lr, use_container_width=True)
            st.markdown('<div class="chart-caption">Audience approval score by category</div></div>', unsafe_allow_html=True)

        st.markdown("---")
        st.markdown('<div class="sec-title">Predictive Analytics — What will happen?</div>', unsafe_allow_html=True)

        r5c1, r5c2 = st.columns(2)

        with r5c1:
            # Simulated growth forecast from engagement normalisation
            st.markdown('<div class="chart-wrap"><div class="chart-title">📈 24h Simulated Forecast — Category Growth %</div>', unsafe_allow_html=True)
            cat_f2 = cat_perf.copy()
            eng_mean = cat_f2["avg_engagement"].mean()
            eng_std  = cat_f2["avg_engagement"].std() + 1e-6
            cat_f2["simulated_growth_pct"] = (
                (cat_f2["avg_engagement"] - eng_mean) / eng_std * 10
            ).round(1)
            cat_f2 = cat_f2.sort_values("simulated_growth_pct")
            bar_colors_h = ["#3fb950" if g >= 0 else "#f85149"
                            for g in cat_f2["simulated_growth_pct"]]
            fig_hfc = go.Figure(go.Bar(
                x=cat_f2["simulated_growth_pct"], y=cat_f2["category"],
                orientation="h", marker_color=bar_colors_h,
                text=[f"{g:+.1f}%" for g in cat_f2["simulated_growth_pct"]],
                textposition="outside", textfont=dict(size=11, color="#e6edf3"),
            ))
            fig_hfc.add_vline(x=0, line_color="#30363d", line_width=1)
            fig_hfc.update_layout(
                plot_bgcolor="#161b22", paper_bgcolor="#161b22",
                font=dict(color="#c9d1d9", size=11), height=340,
                margin=dict(l=8, r=70, t=24, b=8), showlegend=False,
                xaxis=dict(title="Simulated Growth %", gridcolor="#21262d", title_font=dict(color="#c9d1d9"),
                           linecolor="#444d56", zeroline=False),
                yaxis=dict(gridcolor="#21262d", linecolor="#444d56"),
            )
            st.plotly_chart(fig_hfc, use_container_width=True)
            st.markdown('<div class="chart-caption">Engagement-normalised forecast — categories above mean engagement score trend positive</div></div>', unsafe_allow_html=True)

        with r5c2:
            # Peak hour bar with best hour annotation
            st.markdown('<div class="chart-wrap"><div class="chart-title">⏰ Best Publish Hour — Peak Window Forecast</div>', unsafe_allow_html=True)
            fig_hr2 = go.Figure()
            fig_hr2.add_trace(go.Bar(
                x=hour_perf["publish_hour"], y=hour_perf["avg_views"],
                marker=dict(color=hour_perf["avg_views"],
                            colorscale="Blues", showscale=False),
            ))
            best_h = int(hour_perf.loc[hour_perf["avg_views"].idxmax(), "publish_hour"])
            fig_hr2.add_vline(
                x=best_h, line_color="#3fb950", line_width=2,
                annotation_text=f"Peak: {best_h}:00",
                annotation_font_color="#3fb950",
                annotation_position="top right",
            )
            fig_hr2.update_layout(
                plot_bgcolor="#161b22", paper_bgcolor="#161b22",
                font=dict(color="#c9d1d9", size=11), height=340,
                margin=dict(l=8, r=8, t=24, b=8), showlegend=False,
                xaxis=dict(title="Hour (24h)", gridcolor="#21262d", title_font=dict(color="#c9d1d9"),
                           linecolor="#444d56", dtick=2),
                yaxis=dict(title="Avg Views", gridcolor="#21262d", linecolor="#444d56"),
            )
            st.plotly_chart(fig_hr2, use_container_width=True)
            st.markdown(f'<div class="chart-caption">Best publish hour: <b>{best_h}:00–{best_h+1}:00</b> UTC — historically highest avg views · Green = peak window</div></div>', unsafe_allow_html=True)

        r5c3, r5c4 = st.columns(2)

        with r5c3:
            st.markdown('<div class="chart-wrap"><div class="chart-title">🎭 Sentiment by Category</div>', unsafe_allow_html=True)
            fig_sc3 = px.bar(sent_cat_f, x="category", y="count", color="sentiment",
                barmode="stack",
                color_discrete_map={"positive":"#3fb950","neutral":"#8b949e","negative":"#f85149"},
                labels={"count":"Videos","category":"","sentiment":""})
            fig_sc3.update_layout(**dark(280, showlegend=True,
                legend=dict(bgcolor="#161b22", font=dict(color="#c9d1d9", size=10),
                           orientation="h",yanchor="bottom",y=1.02)))
            st.plotly_chart(fig_sc3, use_container_width=True)
            st.markdown('<div class="chart-caption">Which categories use most emotional titles?</div></div>', unsafe_allow_html=True)

        with r5c4:
            st.markdown('<div class="chart-wrap"><div class="chart-title">☁️ Trending Keywords</div>', unsafe_allow_html=True)
            kw_dict = dict(zip(keywords["keyword"], keywords["count"]))
            if kw_dict:
                wc2 = WordCloud(width=600, height=260, background_color="#161b22",
                                colormap="Blues", max_words=40).generate_from_frequencies(kw_dict)
                fig_wc2, ax2 = plt.subplots(figsize=(6, 2.6))
                fig_wc2.patch.set_facecolor("#161b22")
                ax2.imshow(wc2, interpolation="bilinear")
                ax2.axis("off")
                st.pyplot(fig_wc2)
            st.markdown('<div class="chart-caption">Most frequent words in trending titles — dominant content themes</div></div>', unsafe_allow_html=True)

        st.markdown("---")
        st.markdown('<div class="sec-title">Prescriptive Analytics — What should be done?</div>', unsafe_allow_html=True)

        row = prescr.iloc[0]
        p1,p2,p3,p4,p5 = st.columns(5)
        items = [
            (p1, "📁 Best Category",    row["rec_category"].split("'")[1] if "'" in row["rec_category"] else row["rec_category"][:20],    row["rec_category"]),
            (p2, "📅 Best Day",         row["rec_day"].split()[2],         row["rec_day"]),
            (p3, "⏰ Best Hour",        row["rec_hour"].split()[2],         row["rec_hour"]),
            (p4, "📝 Title Length",     row["rec_title_length"].split()[1], row["rec_title_length"]),
            (p5, "🏷️ Tags",            row["rec_tags"].split()[1],         row["rec_tags"]),
        ]
        for col, title, val, desc in items:
            col.markdown(f"""
            <div class="ins-box">
                <div class="ins-title">{title}</div>
                <div class="ins-value">{val}</div>
                <div class="ins-text">{desc}</div>
            </div>""", unsafe_allow_html=True)

        st.markdown("---")
        st.markdown('<div class="sec-title">Top Trending Channels</div>', unsafe_allow_html=True)
        fig_ch = px.bar(channels.head(15).sort_values("total_views"),
            x="total_views", y="channel_title", orientation="h",
            color="avg_engagement", color_continuous_scale="Teal",
            labels={"total_views":"Total Views","channel_title":"","avg_engagement":"Avg Engagement"})
        fig_ch.update_layout(**dark(400))
        st.plotly_chart(fig_ch, use_container_width=True)

# # ── Footer ────────────────────────────────────────────────────
# st.markdown("""
# <div style='text-align:center;color:#21262d;font-size:10px;padding:16px;'>
# InsightFlow · AI528 Big Data Analytics · IIT Ropar · Kafka → PySpark → Delta Lake → Streamlit Cloud
# </div>""", unsafe_allow_html=True)