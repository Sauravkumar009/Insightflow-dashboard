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

# ── CSS ───────────────────────────────────────────────────────
st.markdown("""
<style>
  /* Global */
  .stApp { background:#0d1117; color:#e6edf3; font-family:'Segoe UI',sans-serif; }
  #MainMenu,footer,header { visibility:hidden; }
  .block-container { padding:0 !important; max-width:100% !important; }

  /* Top navbar */
  .navbar {
    display:flex; align-items:center; justify-content:space-between;
    background:#161b22; border-bottom:1px solid #21262d;
    padding:12px 24px; position:sticky; top:0; z-index:999;
  }
  .nav-title { font-size:20px; font-weight:800; color:#58a6ff; letter-spacing:-0.5px; }
  .nav-subtitle { font-size:11px; color:#8b949e; margin-top:2px; }

  /* Page buttons */
  .page-btn {
    display:inline-block; padding:6px 20px; border-radius:6px;
    font-size:13px; font-weight:600; cursor:pointer;
    border:1px solid #30363d; margin-left:8px;
    text-align:center; transition:all 0.2s;
  }
  .page-active { background:#1f6feb; color:#fff; border-color:#1f6feb; }
  .page-inactive { background:transparent; color:#8b949e; }

  /* KPI cards */
  .kpi-row { display:flex; gap:12px; padding:16px 24px 0 24px; }
  .kpi-card {
    flex:1; background:#161b22; border:1px solid #21262d;
    border-radius:10px; padding:16px 20px; text-align:center;
  }
  .kpi-value { font-size:32px; font-weight:800; color:#58a6ff; line-height:1; }
  .kpi-label {
    font-size:11px; color:#8b949e; margin-top:6px;
    text-transform:uppercase; letter-spacing:0.8px;
  }
  .kpi-positive { color:#3fb950; }
  .kpi-negative { color:#f85149; }
  .kpi-neutral  { color:#d29922; }

  /* Section title */
  .sec-title {
    font-size:13px; font-weight:700; color:#8b949e;
    text-transform:uppercase; letter-spacing:1px;
    padding:0 24px; margin:16px 0 8px 0;
    border-left:3px solid #58a6ff; padding-left:12px;
    margin-left:24px;
  }

  /* Chart containers */
  .chart-wrap {
    background:#161b22; border:1px solid #21262d;
    border-radius:10px; padding:12px; margin:0;
  }
  .chart-title {
    font-size:12px; font-weight:700; color:#c9d1d9;
    margin-bottom:8px; text-transform:uppercase; letter-spacing:0.5px;
  }
  .chart-caption { font-size:10px; color:#8b949e; margin-top:4px; font-style:italic; }

  /* Insight box */
  .ins-box {
    background:#161b22; border:1px solid #21262d;
    border-radius:10px; padding:14px 16px; height:100%;
  }
  .ins-title { font-size:11px; font-weight:700; color:#58a6ff; text-transform:uppercase; letter-spacing:0.5px; margin-bottom:8px; }
  .ins-value { font-size:28px; font-weight:800; color:#3fb950; }
  .ins-text  { font-size:12px; color:#8b949e; margin-top:4px; line-height:1.5; }

  /* Filter row */
  .filter-row { display:flex; align-items:center; gap:8px; }

  /* Streamlit overrides */
  div[data-testid="metric-container"] { display:none; }
  .stSelectbox > div > div { background:#161b22 !important; border-color:#30363d !important; color:#e6edf3 !important; font-size:12px !important; }
  .stMultiSelect > div > div { background:#161b22 !important; border-color:#30363d !important; }
  .stMultiSelect span { background:#1f6feb !important; }
  label { color:#8b949e !important; font-size:11px !important; }
  .stTabs [data-baseweb="tab-list"] { display:none; }
  h1,h2,h3,h4 { color:#e6edf3 !important; }

  /* Padding for content area */
  .content { padding:0 24px 24px 24px; }
</style>
""", unsafe_allow_html=True)

# ── Dark plotly base ──────────────────────────────────────────
def dark(height=350, **kwargs):
    base = dict(
        plot_bgcolor  = "#161b22",
        paper_bgcolor = "#161b22",
        font          = dict(color="#8b949e", size=11),
        height        = height,
        margin        = dict(l=8, r=8, t=24, b=8),
        showlegend    = False,
        xaxis         = dict(gridcolor="#21262d", linecolor="#30363d", tickfont=dict(size=10)),
        yaxis         = dict(gridcolor="#21262d", linecolor="#30363d", tickfont=dict(size=10)),
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

# ── Session state for page ────────────────────────────────────
if "page" not in st.session_state:
    st.session_state.page = "live"

# ── NAVBAR ────────────────────────────────────────────────────
st.markdown(f"""
<div class="navbar">
  <div>
    <div class="nav-title">📊 InsightFlow</div>
    <div class="nav-subtitle">YouTube Trending Analytics · PySpark · Delta Lake · AI528</div>
  </div>
  <div style="display:flex; align-items:center; gap:4px;">
    <span style="font-size:11px; color:#8b949e; margin-right:8px;">VIEW:</span>
  </div>
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
      </div>
      <div class="kpi-card">
        <div class="kpi-value">{int(df['views'].mean()):,}</div>
        <div class="kpi-label">Avg Views</div>
      </div>
      <div class="kpi-card">
        <div class="kpi-value">{int(df['likes'].mean()):,}</div>
        <div class="kpi-label">Avg Likes</div>
      </div>
      <div class="kpi-card">
        <div class="kpi-value kpi-positive">{pos_pct}%</div>
        <div class="kpi-label">Positive Sentiment</div>
      </div>
      <div class="kpi-card">
        <div class="kpi-value">{df['country'].nunique()}</div>
        <div class="kpi-label">Countries</div>
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
                    coastlinecolor="#30363d", showframe=False,
                    showcoastlines=True),
                height=320, margin=dict(l=0,r=0,t=0,b=0),
                coloraxis_colorbar=dict(tickfont=dict(color="#8b949e",size=9)))
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
            fig_tc.update_traces(textposition="inside", textfont_size=9)
            fig_tc.update_layout(
                plot_bgcolor="#161b22", paper_bgcolor="#161b22",
                font=dict(color="#8b949e", size=11), height=320,
                margin=dict(l=8,r=8,t=24,b=8), showlegend=True,
                xaxis=dict(gridcolor="#21262d", linecolor="#30363d", tickfont=dict(size=10)),
                yaxis=dict(gridcolor="#21262d", linecolor="#30363d", tickfont=dict(size=10)),
                legend=dict(bgcolor="#161b22", font=dict(size=9),
                        orientation="h", yanchor="bottom", y=1.02))
            st.plotly_chart(fig_tc, use_container_width=True)
            st.markdown('<div class="chart-caption">Top trending video in each country from latest collection run</div></div>', unsafe_allow_html=True)

        # Row 2: Category views + Day performance
        r2c1, r2c2, r2c3 = st.columns(3)

        with r2c1:
            st.markdown('<div class="chart-wrap"><div class="chart-title">📊 Avg Views by Category</div>', unsafe_allow_html=True)
            cat_agg = df.groupby("category").agg(
                avg_views=("views","mean"), avg_eng=("engagement","mean")).reset_index()
            cat_agg = cat_agg.sort_values("avg_views", ascending=True)
            fig_cat = px.bar(cat_agg, x="avg_views", y="category", orientation="h",
                color="avg_eng", color_continuous_scale="Blues",
                labels={"avg_views":"Avg Views","category":"","avg_eng":"Engagement"})
            fig_cat.update_layout(**dark(280))
            st.plotly_chart(fig_cat, use_container_width=True)
            st.markdown('<div class="chart-caption">Which category dominates in views right now?</div></div>', unsafe_allow_html=True)

        with r2c2:
            st.markdown('<div class="chart-wrap"><div class="chart-title">😊 Sentiment Distribution</div>', unsafe_allow_html=True)
            sent_agg = df["sentiment"].value_counts().reset_index()
            sent_agg.columns = ["sentiment","count"]
            fig_sent = px.pie(sent_agg, values="count", names="sentiment", hole=0.55,
                color="sentiment",
                color_discrete_map={"positive":"#3fb950","neutral":"#8b949e","negative":"#f85149"})
            fig_sent.update_traces(textposition="inside", textinfo="percent+label",
                textfont_size=11, hovertemplate="%{label}: %{value:,}<extra></extra>")
            fig_sent.update_layout(**dark(280))
            st.plotly_chart(fig_sent, use_container_width=True)
            st.markdown('<div class="chart-caption">Emotional tone of today\'s trending titles</div></div>', unsafe_allow_html=True)

        with r2c3:
            st.markdown('<div class="chart-wrap"><div class="chart-title">📈 View Growth — Top 5 Videos</div>', unsafe_allow_html=True)
            if total_runs > 1:
                top5  = df.groupby("title")["views"].max().nlargest(5).index.tolist()
                gdf   = df[df["title"].isin(top5)].copy()
                gdf["short"] = gdf["title"].str[:20]+"..."
                gdf = gdf.sort_values("fetch_time")
                fig_g = px.line(gdf, x="fetch_time", y="views", color="short",
                    markers=True, color_discrete_sequence=COLORS,
                    labels={"fetch_time":"","views":"Views","short":""})
                fig_g.update_layout(**dark(280, showlegend=True,
                    legend=dict(bgcolor="#161b22",font=dict(size=8),
                               orientation="v", x=1.01)))
                st.plotly_chart(fig_g, use_container_width=True)
                st.markdown('<div class="chart-caption">How view counts grow across collection runs</div></div>', unsafe_allow_html=True)
            else:
                st.info("Need 2+ runs for growth chart")
                st.markdown('</div>', unsafe_allow_html=True)

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
                fig_hm.update_traces(textfont_size=9)
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
                legend=dict(bgcolor="#161b22",font=dict(size=9),
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
                    legend=dict(bgcolor="#161b22",font=dict(size=9),
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
                legend=dict(bgcolor="#161b22",font=dict(size=9),
                           orientation="h",yanchor="bottom",y=1.02)))
            st.plotly_chart(fig_scat, use_container_width=True)
            st.markdown('<div class="chart-caption">Which categories use more emotional titles?</div></div>', unsafe_allow_html=True)

        st.markdown("---")
        st.markdown('<div class="sec-title">Predictive Analytics — What will happen?</div>', unsafe_allow_html=True)

        r5c1, r5c2 = st.columns(2)

        with r5c1:
            st.markdown('<div class="chart-wrap"><div class="chart-title">☁️ Live Trending Keywords</div>', unsafe_allow_html=True)
            kw = get_keywords(df["title"])
            if kw:
                wc = WordCloud(width=600,height=280,background_color="#161b22",
                               colormap="Blues",max_words=40).generate_from_frequencies(kw)
                fig_wc, ax = plt.subplots(figsize=(6,2.8))
                fig_wc.patch.set_facecolor("#161b22")
                ax.imshow(wc, interpolation="bilinear")
                ax.axis("off")
                st.pyplot(fig_wc)
            st.markdown('<div class="chart-caption">Most frequent words in live trending titles</div></div>', unsafe_allow_html=True)

        with r5c2:
            st.markdown('<div class="chart-wrap"><div class="chart-title">📊 Sentiment vs Engagement</div>', unsafe_allow_html=True)
            se = df.groupby("sentiment").agg(
                avg_views=("views","mean"), avg_eng=("engagement","mean")).reset_index()
            fig_se = go.Figure()
            fig_se.add_trace(go.Bar(x=se["sentiment"],y=se["avg_views"],
                name="Avg Views",marker_color="#38bdf8",yaxis="y"))
            fig_se.add_trace(go.Scatter(x=se["sentiment"],y=se["avg_eng"],
                name="Engagement",yaxis="y2",
                line=dict(color="#f472b6",width=3),mode="lines+markers"))
            fig_se.update_layout(
                plot_bgcolor="#161b22",paper_bgcolor="#161b22",
                font=dict(color="#8b949e",size=10),height=280,
                margin=dict(l=8,r=8,t=24,b=8),
                xaxis=dict(gridcolor="#21262d"),
                yaxis=dict(title="Avg Views",gridcolor="#21262d"),
                yaxis2=dict(title="Engagement",overlaying="y",side="right"),
                legend=dict(bgcolor="#161b22",font=dict(size=9)))
            st.plotly_chart(fig_se, use_container_width=True)
            st.markdown('<div class="chart-caption">Do positive titles drive more engagement?</div></div>', unsafe_allow_html=True)

            st.markdown("---")
            st.markdown('<div class="sec-title">Prescriptive Analytics — What should be done?</div>',
                        unsafe_allow_html=True)

            @st.cache_data(ttl=300)
            def load_gold():
                try:
                    results = {}
                    with httpx.Client(timeout=30) as c:
                        for table in ["gold_category","gold_country","gold_sentiment","gold_trending"]:
                            r = c.get(
                                f"{SUPABASE_URL}/rest/v1/{table}?select=*&order=fetch_time.desc&limit=500",
                                headers=SUPABASE_H)
                            results[table] = pd.DataFrame(r.json()) if r.status_code==200 and r.json() else pd.DataFrame()
                    return results
                except:
                    return {}

            gold     = load_gold()
            gold_cat = gold.get("gold_category",  pd.DataFrame())
            gold_co  = gold.get("gold_country",   pd.DataFrame())
            gold_sent_df = gold.get("gold_sentiment", pd.DataFrame())
            gold_tr  = gold.get("gold_trending",  pd.DataFrame())

            # Best values from Gold layer
            if len(gold_cat) > 0:
                lt = gold_cat["fetch_time"].max()
                gc = gold_cat[gold_cat["fetch_time"]==lt]
                best_cat     = gc.sort_values("avg_engagement_rate",ascending=False).iloc[0]["category"]
                best_cat_eng = round(gc["avg_engagement_rate"].max(), 4)
                best_views_cat = gc.sort_values("avg_views",ascending=False).iloc[0]["category"]
                best_views_val = int(gc["avg_views"].max())
            else:
                best_cat     = df.groupby("category")["engagement"].mean().idxmax()
                best_cat_eng = round(df.groupby("category")["engagement"].mean().max(), 4)
                best_views_cat = df.groupby("category")["views"].mean().idxmax()
                best_views_val = int(df.groupby("category")["views"].mean().max())

            if len(gold_co) > 0:
                lt = gold_co["fetch_time"].max()
                gc2 = gold_co[gold_co["fetch_time"]==lt]
                best_country = gc2.sort_values("avg_engagement_rate",ascending=False).iloc[0]["country"]
                best_co_eng  = round(gc2["avg_engagement_rate"].max(), 4)
            else:
                best_country = df.groupby("country")["engagement"].mean().idxmax()
                best_co_eng  = round(df.groupby("country")["engagement"].mean().max(), 4)

            if len(gold_sent_df) > 0:
                lt = gold_sent_df["fetch_time"].max()
                gs = gold_sent_df[gold_sent_df["fetch_time"]==lt]
                best_sent     = gs.sort_values("avg_engagement_rate",ascending=False).iloc[0]["sentiment"]
                best_sent_eng = round(gs["avg_engagement_rate"].max(), 4)
            else:
                best_sent     = df.groupby("sentiment")["engagement"].mean().idxmax()
                best_sent_eng = round(df.groupby("sentiment")["engagement"].mean().max(), 4)

            if len(gold_tr) > 0:
                lt = gold_tr["fetch_time"].max()
                gt = gold_tr[gold_tr["fetch_time"]==lt].sort_values("trending_score",ascending=False).iloc[0]
                top_title = gt["title"][:28]+"..."
                top_score = round(gt["trending_score"], 3)
                top_cat   = gt["category"]
            else:
                top_v     = df.nlargest(1,"trending_score" if "trending_score" in df.columns else "views").iloc[0]
                top_title = top_v["title"][:28]+"..."
                top_score = round(top_v.get("trending_score", 0), 3)
                top_cat   = top_v["category"]

            corr_val = round(df[["views","likes"]].corr().iloc[0,1], 3)

            p1,p2,p3,p4,p5,p6 = st.columns(6)
            for col, title, val, desc in [
                (p1,"📁 Best Category",   best_cat,      f"Engagement: {best_cat_eng} (Gold layer)"),
                (p2,"🌍 Best Country",    best_country,  f"Engagement: {best_co_eng} (Gold layer)"),
                (p3,"😊 Best Tone",       best_sent,     f"Engagement: {best_sent_eng} (Gold layer)"),
                (p4,"👁 Peak Views",      best_views_cat,f"Avg: {best_views_val:,} views (Gold layer)"),
                (p5,"📊 Views↔Likes",    str(corr_val), "Correlation strength"),
                (p6,"🏆 Top Trending",   top_title,     f"Score: {top_score} | {top_cat}"),
            ]:
                col.markdown(f"""
                <div class="ins-box">
                    <div class="ins-title">{title}</div>
                    <div class="ins-value" style="font-size:16px;">{val}</div>
                    <div class="ins-text">{desc}</div>
                </div>""", unsafe_allow_html=True)

            # Gold category chart
            if len(gold_cat) > 0:
                st.markdown("#### 📊 Gold Layer — Category Aggregation")
                lt   = gold_cat["fetch_time"].max()
                gcat = gold_cat[gold_cat["fetch_time"]==lt].sort_values("avg_trending_score",ascending=False)
                fig_gold = go.Figure()
                fig_gold.add_trace(go.Bar(
                    x=gcat["category"],y=gcat["avg_views"],
                    name="Avg Views",marker_color="#38bdf8",yaxis="y"))
                fig_gold.add_trace(go.Scatter(
                    x=gcat["category"],y=gcat["avg_engagement_rate"],
                    name="Avg Engagement",yaxis="y2",
                    line=dict(color="#f472b6",width=3),mode="lines+markers"))
                fig_gold.update_layout(
                    plot_bgcolor="#161b22",paper_bgcolor="#161b22",
                    font=dict(color="#8b949e",size=10),height=320,
                    margin=dict(l=8,r=8,t=24,b=8),
                    xaxis=dict(gridcolor="#21262d",tickangle=-20),
                    yaxis=dict(title="Avg Views",gridcolor="#21262d"),
                    yaxis2=dict(title="Avg Engagement Rate",overlaying="y",side="right"),
                    showlegend=True,
                    legend=dict(bgcolor="#161b22",font=dict(size=9),
                            orientation="h",yanchor="bottom",y=1.02))
                st.plotly_chart(fig_gold, use_container_width=True)
                st.caption("Gold layer: groupBy(category).agg(avg_views, avg_engagement_rate, count)")

            # Gold trending table
            if len(gold_tr) > 0:
                st.markdown("#### 🏆 Gold Layer — Top 10 Trending Videos")
                lt = gold_tr["fetch_time"].max()
                t10 = gold_tr[gold_tr["fetch_time"]==lt].nlargest(10,"trending_score")[
                    ["title","category","country","views","engagement_rate","trending_score"]].copy()
                t10["title"]          = t10["title"].str[:35]+"..."
                t10["views"]          = t10["views"].apply(lambda x: f"{int(x):,}")
                t10["trending_score"] = t10["trending_score"].round(3)
                t10.columns = ["Title","Category","Country","Views","Engagement","Score"]
                st.dataframe(t10, use_container_width=True, hide_index=True)
                st.caption("Gold layer: orderBy(trending_score desc).limit(10)")

            st.markdown('</div>', unsafe_allow_html=True)

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
                legend=dict(bgcolor="#161b22",font=dict(size=8),
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
                font=dict(color="#8b949e",size=10),height=260,
                margin=dict(l=8,r=8,t=24,b=8),
                xaxis=dict(gridcolor="#21262d",tickfont=dict(size=9)),
                yaxis=dict(title="Avg Views",gridcolor="#21262d"),
                yaxis2=dict(title="Engagement",overlaying="y",side="right"),
                legend=dict(bgcolor="#161b22",font=dict(size=9),
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
            fig_corr.update_traces(textfont_size=9)
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
                fig_eh.update_traces(textfont_size=9)
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
                font=dict(color="#8b949e", size=11), height=260,
                margin=dict(l=8,r=8,t=24,b=8), showlegend=False,
                xaxis=dict(tickangle=-30, gridcolor="#21262d", linecolor="#30363d", tickfont=dict(size=10)),
                yaxis=dict(gridcolor="#21262d", linecolor="#30363d", tickfont=dict(size=10)))
            st.plotly_chart(fig_lr, use_container_width=True)
            st.markdown('<div class="chart-caption">Audience approval score by category</div></div>', unsafe_allow_html=True)

        st.markdown("---")
        st.markdown('<div class="sec-title">Predictive Analytics — What will happen?</div>', unsafe_allow_html=True)

        r5c1, r5c2 = st.columns(2)

        with r5c1:
            st.markdown('<div class="chart-wrap"><div class="chart-title">🎭 Sentiment by Category</div>', unsafe_allow_html=True)
            fig_sc3 = px.bar(sent_cat_f, x="category", y="count", color="sentiment",
                barmode="stack",
                color_discrete_map={"positive":"#3fb950","neutral":"#8b949e","negative":"#f85149"},
                labels={"count":"Videos","category":"","sentiment":""})
            fig_sc3.update_layout(**dark(300, showlegend=True,
                legend=dict(bgcolor="#161b22",font=dict(size=9),
                           orientation="h",yanchor="bottom",y=1.02)))
            st.plotly_chart(fig_sc3, use_container_width=True)
            st.markdown('<div class="chart-caption">Which categories use most emotional titles?</div></div>', unsafe_allow_html=True)

        with r5c2:
            st.markdown('<div class="chart-wrap"><div class="chart-title">☁️ Trending Keywords</div>', unsafe_allow_html=True)
            kw_dict = dict(zip(keywords["keyword"], keywords["count"]))
            if kw_dict:
                wc2 = WordCloud(width=600,height=280,background_color="#161b22",
                                colormap="Blues",max_words=40).generate_from_frequencies(kw_dict)
                fig_wc2, ax2 = plt.subplots(figsize=(6,2.8))
                fig_wc2.patch.set_facecolor("#161b22")
                ax2.imshow(wc2, interpolation="bilinear")
                ax2.axis("off")
                st.pyplot(fig_wc2)
            st.markdown('<div class="chart-caption">Most frequent words in trending titles</div></div>', unsafe_allow_html=True)

            # Sentiment by category
            st.markdown('<div class="chart-wrap"><div class="chart-title">🎭 Sentiment by Category (Live)</div>', unsafe_allow_html=True)
            scat2 = df.groupby(["category","sentiment"]).size().reset_index(name="count")
            fig_scat2 = px.bar(scat2, x="category", y="count", color="sentiment",
                barmode="stack",
                color_discrete_map={"positive":"#3fb950","neutral":"#8b949e","negative":"#f85149"},
                labels={"count":"Videos","category":"","sentiment":""})
            fig_scat2.update_layout(**dark(280, showlegend=True,
                legend=dict(bgcolor="#161b22",font=dict(size=9),
                        orientation="h",yanchor="bottom",y=1.02)))
            st.plotly_chart(fig_scat2, use_container_width=True)
            st.markdown('<div class="chart-caption">Which categories use more emotional titles in live data?</div></div>', unsafe_allow_html=True)

            # Top trending table
            st.markdown('<div class="chart-wrap"><div class="chart-title">🏆 Top 10 Trending Videos Right Now</div>', unsafe_allow_html=True)
            latest_time2 = df["fetch_time"].max()
            latest_df2   = df[df["fetch_time"]==latest_time2]
            if "trending_score" in latest_df2.columns and latest_df2["trending_score"].sum() > 0:
                score_col = "trending_score"
            else:
                latest_df2 = latest_df2.copy()
                latest_df2["trending_score"] = (
                    latest_df2["views"]/(latest_df2["views"].max()+1)*0.5 +
                    latest_df2["engagement"]/(latest_df2["engagement"].max()+1)*0.3 +
                    latest_df2["like_ratio"]/(latest_df2["like_ratio"].max()+1)*0.2
                )
                score_col = "trending_score"
            top10_live = latest_df2.nlargest(10, score_col)[
                ["title","category","country","views","engagement_rate" if "engagement_rate" in latest_df2.columns else "engagement", score_col]].copy()
            top10_live["title"]         = top10_live["title"].str[:40]+"..."
            top10_live["views"]         = top10_live["views"].apply(lambda x: f"{int(x):,}")
            top10_live[score_col]       = top10_live[score_col].round(3)
            top10_live.columns          = ["Title","Category","Country","Views","Engagement","Score"]
            st.dataframe(top10_live, use_container_width=True, hide_index=True)
            st.markdown('<div class="chart-caption">Composite score: 40% views + 30% engagement + 20% like ratio + 10% comment rate</div></div>', unsafe_allow_html=True)

            st.markdown("---")

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

        st.markdown('</div>', unsafe_allow_html=True)

# # ── Footer ────────────────────────────────────────────────────
# st.markdown("""
# <div style='text-align:center;color:#21262d;font-size:10px;padding:16px;'>
# InsightFlow · AI528 Big Data Analytics · IIT Ropar · Kafka → PySpark → Delta Lake → Streamlit Cloud
# </div>""", unsafe_allow_html=True)