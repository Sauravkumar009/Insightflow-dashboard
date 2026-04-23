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

st.set_page_config(
    page_title="InsightFlow — YouTube Analytics",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)

# ── Supabase config ───────────────────────────────────────────
SUPABASE_URL = "https://oymwlmbqzvmpwihhhirx.supabase.co"
SUPABASE_KEY = "sb_publishable_SPQGizmtB8uBcBq6A0kfWw_HOvtQWuQ"
SUPABASE_HEADERS = {
    "apikey"       : SUPABASE_KEY,
    "Authorization": f"Bearer {SUPABASE_KEY}",
}

GOLD_DIR      = os.path.join("outputs", "gold")
ANALYTICS_DIR = os.path.join("outputs", "analytics")

# ── Dark theme CSS ────────────────────────────────────────────
st.markdown("""
<style>
    .stApp { background-color: #0a0a0f; color: #e2e8f0; }
    section[data-testid="stSidebar"] {
        background-color: #0d0d1a;
        border-right: 1px solid #1e293b;
    }
    /* Toggle pill */
    .toggle-container {
        display: flex; justify-content: center;
        background: #0f172a; border-radius: 50px;
        padding: 4px; border: 1px solid #1e293b;
        width: fit-content; margin: 0 auto 20px auto;
    }
    .toggle-btn {
        padding: 10px 32px; border-radius: 50px;
        font-size: 14px; font-weight: 600;
        cursor: pointer; border: none; transition: all 0.3s;
    }
    .toggle-active {
        background: linear-gradient(135deg, #0ea5e9, #6366f1);
        color: white; box-shadow: 0 4px 15px rgba(14,165,233,0.3);
    }
    .toggle-inactive { background: transparent; color: #64748b; }
    /* Metric cards */
    div[data-testid="metric-container"] {
        background: linear-gradient(135deg, #0f172a, #1e293b);
        border: 1px solid #334155; border-radius: 12px; padding: 16px;
    }
    div[data-testid="stMetricValue"] { color: #38bdf8 !important; font-size: 28px !important; }
    div[data-testid="stMetricLabel"] { color: #94a3b8 !important; }
    div[data-testid="stMetricDelta"] { font-size: 12px !important; }
    /* Section headers */
    .section-header {
        background: linear-gradient(90deg, #0f172a, transparent);
        border-left: 4px solid #38bdf8;
        padding: 10px 18px; margin: 24px 0 14px 0;
        border-radius: 0 8px 8px 0;
        font-size: 16px; font-weight: 700; color: #e2e8f0;
    }
    /* Cards */
    .insight-box {
        background: #0f172a; border: 1px solid #1e293b;
        border-radius: 12px; padding: 16px 20px; margin: 6px 0;
    }
    .rec-box {
        background: linear-gradient(135deg, #0d1f37, #0f172a);
        border: 1px solid #0ea5e9; border-radius: 12px;
        padding: 16px 20px; margin: 6px 0;
    }
    .insight-title { font-size: 12px; font-weight: 700; color: #38bdf8; margin-bottom: 6px; text-transform: uppercase; letter-spacing: 0.5px; }
    .insight-text  { font-size: 13px; color: #cbd5e1; line-height: 1.6; }
    /* Sidebar filters */
    .sidebar-section {
        background: #0f172a; border-radius: 10px;
        padding: 12px; margin: 8px 0; border: 1px solid #1e293b;
    }
    .sidebar-title { font-size: 11px; color: #64748b; text-transform: uppercase; letter-spacing: 1px; margin-bottom: 8px; font-weight: 700; }
    /* Live indicator */
    .live-dot {
        display: inline-block; width: 8px; height: 8px;
        background: #22c55e; border-radius: 50%;
        animation: pulse 2s infinite; margin-right: 6px;
    }
    @keyframes pulse {
        0%,100% { opacity:1; } 50% { opacity:0.3; }
    }
    h1,h2,h3 { color: #e2e8f0 !important; }
    .stDataFrame { background: #0f172a !important; }
    /* Tab styling */
    .stTabs [data-baseweb="tab-list"] { background: #0f172a; border-radius: 10px; padding: 4px; }
    .stTabs [data-baseweb="tab"] { color: #64748b; border-radius: 8px; }
    .stTabs [aria-selected="true"] { background: #1e293b !important; color: #38bdf8 !important; }
</style>
""", unsafe_allow_html=True)

COLORS    = ["#38bdf8","#818cf8","#34d399","#fb923c","#f472b6","#a78bfa","#fbbf24","#4ade80"]
DARK_BASE = dict(plot_bgcolor="#0f172a", paper_bgcolor="#0a0a0f", font_color="#94a3b8")
GRID      = dict(gridcolor="#1e293b", linecolor="#334155")
DAY_ORDER = ["Monday","Tuesday","Wednesday","Thursday","Friday","Saturday","Sunday"]

def dark_layout(height=400, **kwargs):
    return dict(**DARK_BASE, height=height, xaxis=GRID, yaxis=GRID, **kwargs)

# ── Load historical data ──────────────────────────────────────
@st.cache_data(ttl=3600)
def load_historical():
    kpis       = pd.read_csv(os.path.join(GOLD_DIR,      "kpis.csv"))
    cat_perf   = pd.read_csv(os.path.join(GOLD_DIR,      "category_performance.csv"))
    country    = pd.read_csv(os.path.join(GOLD_DIR,      "country_performance.csv"))
    channels   = pd.read_csv(os.path.join(GOLD_DIR,      "top_channels.csv"))
    day_perf   = pd.read_csv(os.path.join(GOLD_DIR,      "day_performance.csv"))
    hour_perf  = pd.read_csv(os.path.join(GOLD_DIR,      "hour_performance.csv"))
    corr       = pd.read_csv(os.path.join(ANALYTICS_DIR, "correlation_matrix.csv"), index_col=0)
    eng_hmap   = pd.read_csv(os.path.join(ANALYTICS_DIR, "engagement_heatmap.csv"))
    title_eng  = pd.read_csv(os.path.join(ANALYTICS_DIR, "title_length_vs_engagement.csv"))
    tag_eng    = pd.read_csv(os.path.join(ANALYTICS_DIR, "tag_count_vs_views.csv"))
    sent_dist  = pd.read_csv(os.path.join(ANALYTICS_DIR, "sentiment_distribution.csv"))
    sent_eng   = pd.read_csv(os.path.join(ANALYTICS_DIR, "sentiment_vs_engagement.csv"))
    sent_cat   = pd.read_csv(os.path.join(ANALYTICS_DIR, "sentiment_by_category.csv"))
    keywords   = pd.read_csv(os.path.join(ANALYTICS_DIR, "top_keywords.csv"))
    trending   = pd.read_csv(os.path.join(ANALYTICS_DIR, "top_trending_videos.csv"))
    prescr     = pd.read_csv(os.path.join(ANALYTICS_DIR, "prescriptive.csv"))
    scatter    = pd.read_csv(os.path.join(ANALYTICS_DIR, "scatter_sample.csv"))
    like_ratio = pd.read_csv(os.path.join(ANALYTICS_DIR, "like_ratio_by_category.csv"))
    return (kpis, cat_perf, country, channels, day_perf, hour_perf, corr,
            eng_hmap, title_eng, tag_eng, sent_dist, sent_eng,
            sent_cat, keywords, trending, prescr, scatter, like_ratio)

# ── Load live data from Supabase ──────────────────────────────
@st.cache_data(ttl=300)
def load_live_data(date_range="all"):
    try:
        if date_range == "24h":
            cutoff = (datetime.utcnow() - timedelta(hours=24)).isoformat()
            url    = f"{SUPABASE_URL}/rest/v1/youtube_live?select=*&fetch_time=gte.{cutoff}&order=fetch_time.desc&limit=10000"
        elif date_range == "7d":
            cutoff = (datetime.utcnow() - timedelta(days=7)).isoformat()
            url    = f"{SUPABASE_URL}/rest/v1/youtube_live?select=*&fetch_time=gte.{cutoff}&order=fetch_time.desc&limit=10000"
        else:
            url = f"{SUPABASE_URL}/rest/v1/youtube_live?select=*&order=fetch_time.desc&limit=10000"

        with httpx.Client(timeout=30) as client:
            response = client.get(url, headers=SUPABASE_HEADERS)
            if response.status_code == 200:
                data = response.json()
                if data:
                    df = pd.DataFrame(data)
                    df["fetch_time"] = pd.to_datetime(df["fetch_time"])
                    df["views"]      = pd.to_numeric(df["views"],    errors="coerce").fillna(0).astype(int)
                    df["likes"]      = pd.to_numeric(df["likes"],    errors="coerce").fillna(0).astype(int)
                    df["comments"]   = pd.to_numeric(df["comments"], errors="coerce").fillna(0).astype(int)
                    df["engagement"] = (df["likes"] + df["comments"]) / (df["views"] + 1)
                    df["like_ratio"] = df["likes"] / (df["likes"] + 1)
                    if "country" not in df.columns:
                        df["country"] = "United States"
                    return df
        return pd.DataFrame()
    except Exception as e:
        st.error(f"Supabase error: {e}")
        return pd.DataFrame()

def get_live_keywords(titles, n=30):
    import re
    from collections import Counter
    stopwords = {"the","a","an","in","on","of","to","and","is","for","with",
                 "at","by","from","this","that","are","was","it","be","or",
                 "but","not","have","has","will","can","i","you","we","my"}
    words = []
    for t in titles.dropna():
        words += [w.lower() for w in re.findall(r'\b[a-zA-Z]{3,}\b', str(t))
                  if w.lower() not in stopwords]
    return Counter(words).most_common(n)

(kpis_h, cat_perf_h, country_h, channels_h, day_perf_h, hour_perf_h, corr_h,
 eng_hmap_h, title_eng_h, tag_eng_h, sent_dist_h, sent_eng_h,
 sent_cat_h, keywords_h, trending_h, prescr_h, scatter_h, like_ratio_h) = load_historical()

kpi_h = kpis_h.iloc[0]

# ── Sidebar ───────────────────────────────────────────────────
with st.sidebar:
    st.markdown("""
    <div style='text-align:center; padding:16px 0;'>
        <div style='font-size:24px; font-weight:900; color:#38bdf8;'>📊 InsightFlow</div>
        <div style='font-size:11px; color:#475569; margin-top:4px;'>YouTube Analytics Platform</div>
    </div>
    """, unsafe_allow_html=True)

    # Mode toggle in sidebar
    st.markdown('<div class="sidebar-title">📡 Data Mode</div>', unsafe_allow_html=True)
    mode = st.radio("", ["🔴  Live Data", "📂  Historical"],
                    label_visibility="collapsed")
    is_live = mode == "🔴  Live Data"

    st.markdown("---")

    if is_live:
        st.markdown('<div class="sidebar-title">⏱ Date Range</div>', unsafe_allow_html=True)
        date_range = st.selectbox("", ["Last 24 Hours", "Last 7 Days", "All Time"],
                                  label_visibility="collapsed")
        date_range_key = {"Last 24 Hours":"24h","Last 7 Days":"7d","All Time":"all"}[date_range]

        live_df = load_live_data(date_range_key)

        st.markdown("---")
        st.markdown('<div class="sidebar-title">🗂 Filters</div>', unsafe_allow_html=True)

        if len(live_df) > 0:
            all_cats     = sorted(live_df["category"].dropna().unique().tolist())
            all_countries= sorted(live_df["country"].dropna().unique().tolist())
        else:
            all_cats      = []
            all_countries = []

        sel_cats      = st.multiselect("Category",  all_cats,      default=all_cats)
        sel_countries = st.multiselect("Country",   all_countries, default=all_countries)
        sel_sentiment = st.multiselect("Sentiment", ["positive","neutral","negative"],
                                       default=["positive","neutral","negative"])

        # Apply filters
        if len(live_df) > 0:
            live_f = live_df[
                live_df["category"].isin(sel_cats) &
                live_df["country"].isin(sel_countries) &
                live_df["sentiment"].isin(sel_sentiment)
            ].copy()
        else:
            live_f = pd.DataFrame()

        st.markdown("---")
        if len(live_df) > 0:
            total_runs = live_df["fetch_time"].nunique()
            st.markdown(f"""
            <div class="insight-box">
                <div class="insight-title">Live Status</div>
                <div class="insight-text">
                <span class="live-dot"></span> Active<br>
                🗄 {len(live_df):,} total records<br>
                🔄 {total_runs} collection runs<br>
                🕐 Every 3 hours via GitHub Actions
                </div>
            </div>""", unsafe_allow_html=True)

    else:
        live_df = pd.DataFrame()
        live_f  = pd.DataFrame()
        date_range_key = "all"

        st.markdown('<div class="sidebar-title">🗂 Filters</div>', unsafe_allow_html=True)
        all_cats_h    = sorted(cat_perf_h["category"].unique().tolist())
        all_countries_h = sorted(country_h["publish_country"].unique().tolist())
        sel_cats      = st.multiselect("Category",  all_cats_h,      default=all_cats_h)
        sel_countries = st.multiselect("Country",   all_countries_h, default=all_countries_h)
        sel_sentiment = st.multiselect("Sentiment", ["positive","neutral","negative"],
                                       default=["positive","neutral","negative"])

        st.markdown("---")
        st.markdown(f"""
        <div class="insight-box">
            <div class="insight-title">Dataset Info</div>
            <div class="insight-text">
            📊 {int(kpi_h['total_videos']):,} videos<br>
            🌍 {int(kpi_h['total_countries']):,} countries<br>
            📺 {int(kpi_h['total_channels']):,} channels<br>
            📅 Historical trending data
            </div>
        </div>""", unsafe_allow_html=True)

    st.markdown("---")
    st.markdown("""
    <div style='font-size:10px; color:#334155; text-align:center;'>
    Pipeline: Kafka → PySpark → Delta Lake<br>
    AI528 Big Data Analytics · IIT Ropar
    </div>""", unsafe_allow_html=True)

# ── Header ────────────────────────────────────────────────────
st.markdown(f"""
<div style='text-align:center; padding:10px 0 4px 0;'>
    <h1 style='font-size:32px; font-weight:900; color:#38bdf8; margin:0;'>📊 InsightFlow</h1>
    <p style='color:#475569; font-size:13px; margin:4px 0;'>
        {"<span class='live-dot'></span> Live YouTube Analytics — Auto-refreshing every 5 minutes" if is_live else "📂 Historical YouTube Analytics — 161,470 trending videos"}
    </p>
</div>
""", unsafe_allow_html=True)

st.markdown("---")

# ══════════════════════════════════════════
# LIVE MODE
# ══════════════════════════════════════════
if is_live:
    if len(live_f) == 0:
        st.warning("No live data found for the selected filters. GitHub Actions collects data every 3 hours.")
        st.stop()

    total_runs   = live_df["fetch_time"].nunique()
    latest_fetch = live_df["fetch_time"].max()
    latest_str   = pd.Timestamp(latest_fetch).strftime("%Y-%m-%d %H:%M UTC")

    # ── KPI Row ───────────────────────────────────────────────
    k1,k2,k3,k4,k5,k6 = st.columns(6)
    k1.metric("Total Records",    f"{len(live_f):,}")
    k2.metric("Collection Runs",  f"{total_runs}")
    k3.metric("Avg Views",        f"{int(live_f['views'].mean()):,}")
    k4.metric("Avg Likes",        f"{int(live_f['likes'].mean()):,}")
    k5.metric("Countries",        f"{live_f['country'].nunique()}")
    k6.metric("Last Fetch",       latest_str)

    st.markdown("---")

    # ── TABS FOR LIVE ─────────────────────────────────────────
    lt1, lt2, lt3, lt4 = st.tabs([
        "📈 Descriptive", "🔍 Diagnostic",
        "🔮 Predictive",  "💡 Prescriptive"
    ])

    # ── LIVE DESCRIPTIVE ──────────────────────────────────────
    with lt1:
        st.markdown('<div class="section-header">What is happening right now?</div>',
                    unsafe_allow_html=True)

        # World map
        st.markdown("#### 🌍 Trending Videos by Country")
        country_map = live_f.groupby("country").agg(
            total_videos = ("video_id","count"),
            avg_views    = ("views","mean"),
            avg_likes    = ("likes","mean")
        ).reset_index().round(0)

        COUNTRY_COORDS = {
            "United States":{"lat":37.09,"lon":-95.71},
            "India":         {"lat":20.59,"lon":78.96},
            "United Kingdom":{"lat":55.37,"lon":-3.44},
            "Canada":        {"lat":56.13,"lon":-106.35},
            "Australia":     {"lat":-25.27,"lon":133.78},
            "Germany":       {"lat":51.17,"lon":10.45},
            "France":        {"lat":46.23,"lon":2.21},
            "Japan":         {"lat":36.20,"lon":138.25},
            "South Korea":   {"lat":35.91,"lon":127.77},
            "Brazil":        {"lat":-14.24,"lon":-51.93},
        }
        country_map["lat"] = country_map["country"].map(lambda x: COUNTRY_COORDS.get(x,{}).get("lat",0))
        country_map["lon"] = country_map["country"].map(lambda x: COUNTRY_COORDS.get(x,{}).get("lon",0))
        country_map = country_map[country_map["lat"] != 0]

        fig_map = px.scatter_geo(country_map,
            lat="lat", lon="lon",
            size="total_videos", color="avg_views",
            hover_name="country",
            hover_data={"total_videos":True,"avg_views":True,"lat":False,"lon":False},
            color_continuous_scale="Blues",
            size_max=50,
            projection="natural earth",
            labels={"avg_views":"Avg Views","total_videos":"Videos"}
        )
        fig_map.update_layout(
            paper_bgcolor="#0a0a0f", geo=dict(
                bgcolor="#0f172a", landcolor="#1e293b",
                oceancolor="#0a0a0f", showocean=True,
                coastlinecolor="#334155", showframe=False
            ), height=420, margin=dict(l=0,r=0,t=0,b=0)
        )
        st.plotly_chart(fig_map, use_container_width=True)
        st.caption("Which countries have the most trending YouTube videos right now?")

        st.markdown("---")

        # Country wise trending videos chart
        st.markdown("#### 🗺️ Top Trending Videos by Country")
        top_per_country = live_f.sort_values("views", ascending=False).groupby("country").head(1)
        top_per_country = top_per_country.sort_values("views", ascending=False)
        top_per_country["short_title"] = top_per_country["title"].str[:40] + "..."

        fig_ctv = px.bar(top_per_country,
            x="views", y="country", orientation="h",
            color="category", text="short_title",
            color_discrete_sequence=COLORS,
            labels={"views":"Views","country":"Country","category":"Category"}
        )
        fig_ctv.update_traces(textposition="inside", textfont_size=10)
        fig_ctv.update_layout(**dark_layout(400), showlegend=True,
            legend=dict(bgcolor="#0f172a", bordercolor="#334155"))
        st.plotly_chart(fig_ctv, use_container_width=True)
        st.caption("What is the #1 trending video in each country right now?")

        col1, col2 = st.columns(2)
        with col1:
            st.markdown("#### 📊 Category Performance — Avg Views")
            cat_live = live_f.groupby("category").agg(
                avg_views  = ("views","mean"),
                avg_likes  = ("likes","mean"),
                total      = ("video_id","count"),
                avg_eng    = ("engagement","mean")
            ).reset_index().sort_values("avg_views", ascending=True)

            fig_cat = px.bar(cat_live,
                x="avg_views", y="category", orientation="h",
                color="avg_eng", color_continuous_scale="Blues",
                hover_data=["avg_likes","total"],
                labels={"avg_views":"Avg Views","category":"Category","avg_eng":"Engagement"})
            fig_cat.update_layout(**dark_layout(380), showlegend=False)
            st.plotly_chart(fig_cat, use_container_width=True)
            st.caption("Which category has the highest average views right now?")

        with col2:
            st.markdown("#### 🏆 Top 10 Trending Videos Right Now")
            # Show latest fetch only for top trending
            latest_fetch_time = live_f["fetch_time"].max()
            latest_videos = live_f[live_f["fetch_time"] == latest_fetch_time]
            top10 = latest_videos.nlargest(10,"views")[
                ["title","category","country","views","likes","sentiment"]].copy()
            top10["title"] = top10["title"].str[:35] + "..."
            top10["views"] = top10["views"].apply(lambda x: f"{int(x):,}")
            top10["likes"] = top10["likes"].apply(lambda x: f"{int(x):,}")
            st.dataframe(top10, use_container_width=True, hide_index=True)
            st.caption(f"From latest fetch: {pd.Timestamp(latest_fetch_time).strftime('%Y-%m-%d %H:%M UTC')}")

        col3, col4 = st.columns(2)
        with col3:
            st.markdown("#### 📅 Avg Views by Day of Week (Live)")
            if "published" in live_f.columns:
                live_f["pub_day"] = pd.to_datetime(live_f["published"], errors="coerce").dt.day_name()
                day_live = live_f.groupby("pub_day")["views"].mean().reset_index()
                day_live.columns = ["Day","Avg Views"]
                day_live["order"] = day_live["Day"].apply(
                    lambda x: DAY_ORDER.index(x) if x in DAY_ORDER else 7)
                day_live = day_live.sort_values("order")
                fig_day = px.bar(day_live, x="Day", y="Avg Views",
                    color="Avg Views", color_continuous_scale="Blues")
                fig_day.update_layout(**dark_layout(320), showlegend=False)
                st.plotly_chart(fig_day, use_container_width=True)
                st.caption("Which day of the week gets the most views in live trending?")

        with col4:
            st.markdown("#### 📈 View Growth — Top 5 Videos Over Time")
            if total_runs > 1:
                top5_ids  = live_f.groupby("title")["views"].max().nlargest(5).index.tolist()
                growth_df = live_f[live_f["title"].isin(top5_ids)].copy()
                growth_df["short"] = growth_df["title"].str[:25] + "..."
                growth_df = growth_df.sort_values("fetch_time")
                fig_g = px.line(growth_df,
                    x="fetch_time", y="views", color="short",
                    markers=True, color_discrete_sequence=COLORS,
                    labels={"fetch_time":"Time","views":"Views","short":"Video"})
                fig_g.update_layout(**dark_layout(320),
                    legend=dict(bgcolor="#0f172a", bordercolor="#334155"))
                st.plotly_chart(fig_g, use_container_width=True)
                st.caption("How are view counts growing for top videos across collection runs?")
            else:
                st.info("Need 2+ collection runs to show growth chart.")

    # ── LIVE DIAGNOSTIC ───────────────────────────────────────
    with lt2:
        st.markdown('<div class="section-header">Why is it happening? — Live Correlation Analysis</div>',
                    unsafe_allow_html=True)

        # Correlation matrix from live data
        st.markdown("#### 🔥 Live Data Correlation Matrix")
        live_corr_cols = ["views","likes","comments","engagement","like_ratio"]
        live_corr = live_f[live_corr_cols].corr().round(3)
        fig_corr = px.imshow(live_corr,
            color_continuous_scale="RdBu_r", zmin=-1, zmax=1,
            text_auto=True, aspect="auto")
        fig_corr.update_layout(
            plot_bgcolor="#0f172a", paper_bgcolor="#0a0a0f",
            font_color="#94a3b8", height=400)
        st.plotly_chart(fig_corr, use_container_width=True)
        st.caption("How do views, likes, and comments relate to each other in live data?")

        col1, col2 = st.columns(2)
        with col1:
            st.markdown("#### 🔵 Views vs Likes — Live Scatter")
            fig_sc = px.scatter(live_f.sample(min(1000, len(live_f)), random_state=42),
                x="views", y="likes", color="category",
                size="engagement", hover_data=["title","country"],
                color_discrete_sequence=COLORS, opacity=0.7,
                labels={"views":"Views","likes":"Likes"})
            fig_sc.update_layout(**dark_layout(380))
            st.plotly_chart(fig_sc, use_container_width=True)
            st.caption("Does higher viewership always lead to more likes in live data?")

        with col2:
            st.markdown("#### ❤️ Like Ratio by Category (Live)")
            like_cat = live_f.groupby("category")["like_ratio"].mean().reset_index()
            like_cat.columns = ["Category","Like Ratio"]
            like_cat = like_cat.sort_values("Like Ratio", ascending=False)
            fig_lr = px.bar(like_cat,
                x="Category", y="Like Ratio",
                color="Like Ratio", color_continuous_scale="Greens")
            fig_lr.update_layout(**dark_layout(380), showlegend=False)
            st.plotly_chart(fig_lr, use_container_width=True)
            st.caption("Which categories have the most audience approval?")

        # Engagement heatmap category x country
        st.markdown("#### 🔥 Engagement Heatmap — Category × Country")
        if "country" in live_f.columns:
            eng_pivot = live_f.groupby(["category","country"])["engagement"].mean().reset_index()
            eng_pivot = eng_pivot.pivot(index="category", columns="country", values="engagement").fillna(0)
            fig_heat = px.imshow(eng_pivot,
                color_continuous_scale="Blues", text_auto=".3f", aspect="auto")
            fig_heat.update_layout(
                plot_bgcolor="#0f172a", paper_bgcolor="#0a0a0f",
                font_color="#94a3b8", height=420)
            st.plotly_chart(fig_heat, use_container_width=True)
            st.caption("Which Category + Country combination drives the most engagement?")

        # Category trend over time
        st.markdown("#### 📉 Category Trend Over Time (Avg Views per Run)")
        if total_runs > 1:
            cat_trend = live_f.groupby(["fetch_time","category"])["views"].mean().reset_index()
            cat_trend["fetch_time"] = cat_trend["fetch_time"].dt.strftime("%m-%d %H:%M")
            fig_trend = px.line(cat_trend,
                x="fetch_time", y="views", color="category",
                markers=True, color_discrete_sequence=COLORS,
                labels={"fetch_time":"Collection Time","views":"Avg Views","category":"Category"})
            fig_trend.update_layout(**dark_layout(400),
                legend=dict(bgcolor="#0f172a", bordercolor="#334155"))
            st.plotly_chart(fig_trend, use_container_width=True)
            st.caption("Which categories are gaining or losing popularity over time?")

    # ── LIVE PREDICTIVE ───────────────────────────────────────
    with lt3:
        st.markdown('<div class="section-header">What will happen? — Live Sentiment & Trends</div>',
                    unsafe_allow_html=True)

        col1, col2 = st.columns(2)
        with col1:
            st.markdown("#### 😊 Live Sentiment Distribution")
            sent_live = live_f["sentiment"].value_counts().reset_index()
            sent_live.columns = ["sentiment","count"]
            fig_sent = px.pie(sent_live, values="count", names="sentiment", hole=0.5,
                color="sentiment",
                color_discrete_map={"positive":"#34d399","neutral":"#94a3b8","negative":"#f87171"})
            fig_sent.update_layout(
                plot_bgcolor="#0f172a", paper_bgcolor="#0a0a0f",
                font_color="#94a3b8", height=350)
            st.plotly_chart(fig_sent, use_container_width=True)
            st.caption("What is the emotional tone of today's trending titles?")

        with col2:
            st.markdown("#### 📊 Sentiment vs Engagement (Live)")
            sent_eng_live = live_f.groupby("sentiment").agg(
                avg_views = ("views","mean"),
                avg_eng   = ("engagement","mean"),
                count     = ("video_id","count")
            ).reset_index()
            fig_se = go.Figure()
            fig_se.add_trace(go.Bar(
                x=sent_eng_live["sentiment"], y=sent_eng_live["avg_views"],
                name="Avg Views", marker_color="#38bdf8", yaxis="y"))
            fig_se.add_trace(go.Scatter(
                x=sent_eng_live["sentiment"], y=sent_eng_live["avg_eng"],
                name="Engagement", yaxis="y2",
                line=dict(color="#f472b6", width=3), mode="lines+markers"))
            fig_se.update_layout(
                plot_bgcolor="#0f172a", paper_bgcolor="#0a0a0f", font_color="#94a3b8",
                height=350, xaxis=GRID,
                yaxis=dict(title="Avg Views", **GRID),
                yaxis2=dict(title="Avg Engagement", overlaying="y", side="right"),
                legend=dict(bgcolor="#0f172a"))
            st.plotly_chart(fig_se, use_container_width=True)
            st.caption("Do positive titles perform better than negative ones in live data?")

        # Sentiment by category stacked
        st.markdown("#### 🎭 Sentiment by Category (Live)")
        sent_cat_live = live_f.groupby(["category","sentiment"]).size().reset_index(name="count")
        fig_scat = px.bar(sent_cat_live, x="category", y="count", color="sentiment",
            barmode="stack",
            color_discrete_map={"positive":"#34d399","neutral":"#94a3b8","negative":"#f87171"})
        fig_scat.update_layout(**dark_layout(350),
            legend=dict(bgcolor="#0f172a", bordercolor="#334155"))
        st.plotly_chart(fig_scat, use_container_width=True)
        st.caption("Which categories use more emotional titles?")

        col3, col4 = st.columns(2)
        with col3:
            st.markdown("#### ☁️ Live Trending Keywords")
            kw_live = get_live_keywords(live_f["title"])
            if kw_live:
                kw_dict = dict(kw_live)
                wc = WordCloud(width=600, height=300, background_color="#0a0a0f",
                               colormap="cool", max_words=40).generate_from_frequencies(kw_dict)
                fig_wc, ax = plt.subplots(figsize=(6,3))
                fig_wc.patch.set_facecolor("#0a0a0f")
                ax.imshow(wc, interpolation="bilinear")
                ax.axis("off")
                st.pyplot(fig_wc)
            st.caption("What topics dominate live trending content?")

        with col4:
            st.markdown("#### 🏆 Top Trending Videos (Composite Score)")
            latest_only = live_f[live_f["fetch_time"] == live_f["fetch_time"].max()].copy()
            latest_only["score"] = (
                latest_only["views"] / (latest_only["views"].max() + 1) * 0.5 +
                latest_only["engagement"] / (latest_only["engagement"].max() + 1) * 0.3 +
                latest_only["like_ratio"] / (latest_only["like_ratio"].max() + 1) * 0.2
            )
            top_score = latest_only.nlargest(10,"score")[
                ["title","category","country","views","score"]].copy()
            top_score["title"] = top_score["title"].str[:30] + "..."
            top_score["views"] = top_score["views"].apply(lambda x: f"{int(x):,}")
            top_score["score"] = top_score["score"].round(3)
            st.dataframe(top_score, use_container_width=True, hide_index=True)
            st.caption("Composite: 50% views + 30% engagement + 20% like ratio")

    # ── LIVE PRESCRIPTIVE ─────────────────────────────────────
    with lt4:
        st.markdown('<div class="section-header">What should be done? — Live Data Recommendations</div>',
                    unsafe_allow_html=True)

        # Compute live recommendations
        best_cat  = live_f.groupby("category")["engagement"].mean().idxmax()
        best_cat_views = int(live_f.groupby("category")["views"].mean().max())
        best_country  = live_f.groupby("country")["engagement"].mean().idxmax()
        best_sent = live_f.groupby("sentiment")["engagement"].mean().idxmax()
        views_likes_corr = live_f[["views","likes"]].corr().iloc[0,1].round(3)
        positive_pct = round(len(live_f[live_f["sentiment"]=="positive"]) / len(live_f) * 100, 1)

        c1, c2, c3 = st.columns(3)
        with c1:
            st.markdown(f'<div class="rec-box"><div class="insight-title">📁 Best Category Right Now</div><div class="insight-text">Upload in <b>{best_cat}</b> — highest live engagement rate</div></div>', unsafe_allow_html=True)
            st.markdown(f'<div class="rec-box"><div class="insight-title">🌍 Best Target Country</div><div class="insight-text">Target <b>{best_country}</b> audience — highest engagement in live data</div></div>', unsafe_allow_html=True)
        with c2:
            st.markdown(f'<div class="rec-box"><div class="insight-title">😊 Best Title Sentiment</div><div class="insight-text">Use <b>{best_sent}</b> titles — drives most engagement in current trends</div></div>', unsafe_allow_html=True)
            st.markdown(f'<div class="rec-box"><div class="insight-title">📊 Views-Likes Correlation</div><div class="insight-text">Correlation: <b>{views_likes_corr}</b> — {"very strong" if views_likes_corr > 0.8 else "strong"} positive relationship in live data</div></div>', unsafe_allow_html=True)
        with c3:
            st.markdown(f'<div class="rec-box"><div class="insight-title">🎯 Content Tone Today</div><div class="insight-text"><b>{positive_pct}%</b> of trending titles are positive — audience prefers uplifting content today</div></div>', unsafe_allow_html=True)
            st.markdown(f'<div class="rec-box"><div class="insight-title">👁 Peak Views Category</div><div class="insight-text">Highest avg views: <b>{live_f.groupby("category")["views"].mean().idxmax()}</b> with {best_cat_views:,} avg views</div></div>', unsafe_allow_html=True)

        st.markdown("---")
        st.markdown("### 📊 Live Data Insights Summary")

        # Category comparison chart
        cat_summary = live_f.groupby("category").agg(
            avg_views  = ("views","mean"),
            avg_eng    = ("engagement","mean"),
            avg_likes  = ("likes","mean"),
            total      = ("video_id","count")
        ).reset_index().sort_values("avg_views", ascending=False)

        fig_sum = go.Figure()
        fig_sum.add_trace(go.Bar(
            x=cat_summary["category"], y=cat_summary["avg_views"],
            name="Avg Views", marker_color="#38bdf8", yaxis="y"))
        fig_sum.add_trace(go.Scatter(
            x=cat_summary["category"], y=cat_summary["avg_eng"],
            name="Avg Engagement", yaxis="y2",
            line=dict(color="#f472b6", width=3), mode="lines+markers"))
        fig_sum.update_layout(
            plot_bgcolor="#0f172a", paper_bgcolor="#0a0a0f", font_color="#94a3b8",
            height=400, xaxis=dict(title="Category", tickangle=-30, **GRID),
            yaxis=dict(title="Avg Views", **GRID),
            yaxis2=dict(title="Avg Engagement", overlaying="y", side="right"),
            legend=dict(bgcolor="#0f172a", bordercolor="#334155"))
        st.plotly_chart(fig_sum, use_container_width=True)
        st.caption("Complete category performance — views and engagement together from live data")

# ══════════════════════════════════════════
# HISTORICAL MODE
# ══════════════════════════════════════════
else:
    cat_f = cat_perf_h[cat_perf_h["category"].isin(sel_cats)]
    scatter_f = scatter_h[
        scatter_h["category"].isin(sel_cats) &
        scatter_h["sentiment"].isin(sel_sentiment)
    ]

    k1,k2,k3,k4,k5,k6 = st.columns(6)
    k1.metric("Total Videos",   f"{int(kpi_h['total_videos']):,}")
    k2.metric("Total Channels", f"{int(kpi_h['total_channels']):,}")
    k3.metric("Countries",      f"{int(kpi_h['total_countries']):,}")
    k4.metric("Avg Views",      f"{int(kpi_h['avg_views']):,}")
    k5.metric("Avg Likes",      f"{int(kpi_h['avg_likes']):,}")
    k6.metric("Avg Engagement", f"{kpi_h['avg_engagement_rate']:.3f}")

    st.markdown("---")

    tab1, tab2, tab3, tab4 = st.tabs([
        "📈 Descriptive", "🔍 Diagnostic",
        "🔮 Predictive",  "💡 Prescriptive"
    ])

    with tab1:
        st.markdown('<div class="section-header">What is happening? — Historical Overview</div>',
                    unsafe_allow_html=True)

        col1, col2 = st.columns(2)
        with col1:
            st.markdown("#### 📊 Category — Views vs Engagement")
            fig = px.scatter(cat_f,
                x="avg_views", y="avg_engagement",
                size="total_videos", color="category",
                hover_data=["avg_likes","avg_comments","total_videos"],
                color_discrete_sequence=COLORS)
            fig.update_layout(**dark_layout(380), showlegend=True)
            st.plotly_chart(fig, use_container_width=True)
            st.caption("Which categories have high views AND high engagement simultaneously?")

        with col2:
            st.markdown("#### 🌍 Top Countries by Video Count")
            country_f = country_h[country_h["publish_country"].isin(sel_countries)]
            fig2 = px.bar(country_f.head(10).sort_values("total_videos"),
                x="total_videos", y="publish_country", orientation="h",
                color="avg_engagement", color_continuous_scale="Blues")
            fig2.update_layout(**dark_layout(380), showlegend=False)
            st.plotly_chart(fig2, use_container_width=True)
            st.caption("Which countries produce the most trending content?")

        col3, col4 = st.columns(2)
        with col3:
            st.markdown("#### 📅 Engagement & Views by Day")
            day_f = day_perf_h[day_perf_h["published_day_of_week"].isin(sel_days if 'sel_days' in dir() else DAY_ORDER)]
            fig3 = go.Figure()
            fig3.add_trace(go.Bar(x=day_f["published_day_of_week"], y=day_f["avg_views"],
                name="Avg Views", marker_color="#38bdf8", yaxis="y"))
            fig3.add_trace(go.Scatter(x=day_f["published_day_of_week"], y=day_f["avg_engagement"],
                name="Engagement", line=dict(color="#f472b6", width=3),
                yaxis="y2", mode="lines+markers"))
            fig3.update_layout(
                plot_bgcolor="#0f172a", paper_bgcolor="#0a0a0f", font_color="#94a3b8",
                height=380, xaxis=GRID,
                yaxis=dict(title="Avg Views", **GRID),
                yaxis2=dict(title="Engagement Rate", overlaying="y", side="right"),
                legend=dict(bgcolor="#0f172a"))
            st.plotly_chart(fig3, use_container_width=True)
            st.caption("Which day drives most engagement?")

        with col4:
            st.markdown("#### 🕐 Views by Publish Hour")
            fig4 = px.bar(hour_perf_h.sort_values("publish_hour"),
                x="publish_hour", y="avg_views",
                color="avg_views", color_continuous_scale="Blues")
            fig4.update_layout(**dark_layout(380), showlegend=False)
            st.plotly_chart(fig4, use_container_width=True)
            st.caption("What time of day produces most viewed videos?")

        st.markdown("#### 🏆 Top 15 Channels by Total Views")
        fig5 = px.bar(channels_h.sort_values("total_views"),
            x="total_views", y="channel_title", orientation="h",
            color="avg_engagement", color_continuous_scale="Teal")
        fig5.update_layout(**dark_layout(500), showlegend=False)
        st.plotly_chart(fig5, use_container_width=True)
        st.caption("Do top-viewed channels also have high engagement rates?")

    with tab2:
        st.markdown('<div class="section-header">Why is it happening? — Correlation Analysis</div>',
                    unsafe_allow_html=True)

        st.markdown("#### 🔥 Full Correlation Matrix")
        fig_corr = px.imshow(corr_h,
            color_continuous_scale="RdBu_r", zmin=-1, zmax=1,
            text_auto=True, aspect="auto")
        fig_corr.update_layout(
            plot_bgcolor="#0f172a", paper_bgcolor="#0a0a0f",
            font_color="#94a3b8", height=500)
        st.plotly_chart(fig_corr, use_container_width=True)
        st.caption("How do views, likes, comments, title length, tags, and sentiment relate?")

        col1, col2 = st.columns(2)
        with col1:
            st.markdown("#### 🔥 Engagement Heatmap — Category × Day")
            eng_f = eng_hmap_h[eng_hmap_h["category"].isin(sel_cats)]
            if len(eng_f) > 0:
                eng_pivot = eng_f.pivot(index="category", columns="day",
                                        values="engagement_rate").fillna(0)
                day_cols  = [d for d in DAY_ORDER if d in eng_pivot.columns]
                eng_pivot = eng_pivot[day_cols]
                fig_heat  = px.imshow(eng_pivot, color_continuous_scale="Blues",
                                      text_auto=".3f", aspect="auto")
                fig_heat.update_layout(
                    plot_bgcolor="#0f172a", paper_bgcolor="#0a0a0f",
                    font_color="#94a3b8", height=450)
                st.plotly_chart(fig_heat, use_container_width=True)
            st.caption("Which Category + Day combination maximises engagement?")

        with col2:
            st.markdown("#### 📝 Title Length vs Engagement")
            fig_title = go.Figure()
            fig_title.add_trace(go.Bar(
                x=title_eng_h["title_length_bucket"].astype(str),
                y=title_eng_h["avg_engagement"],
                name="Engagement", marker_color="#38bdf8", yaxis="y"))
            fig_title.add_trace(go.Scatter(
                x=title_eng_h["title_length_bucket"].astype(str),
                y=title_eng_h["avg_views"],
                name="Avg Views", yaxis="y2",
                line=dict(color="#f472b6", width=2), mode="lines+markers"))
            fig_title.update_layout(
                plot_bgcolor="#0f172a", paper_bgcolor="#0a0a0f", font_color="#94a3b8",
                height=380, xaxis=dict(title="Title Length", **GRID),
                yaxis=dict(title="Avg Engagement", **GRID),
                yaxis2=dict(title="Avg Views", overlaying="y", side="right"),
                legend=dict(bgcolor="#0f172a"))
            st.plotly_chart(fig_title, use_container_width=True)
            st.caption("Does title length affect video performance?")

        st.markdown("#### 🔵 Views vs Likes vs Engagement — 3-Way")
        fig_sc2 = px.scatter(scatter_f,
            x="views", y="likes", color="category", size="engagement_rate",
            hover_data=["title","channel_title"], color_discrete_sequence=COLORS, opacity=0.7)
        fig_sc2.update_layout(**dark_layout(450))
        st.plotly_chart(fig_sc2, use_container_width=True)
        st.caption("Do high-view videos always have proportionally high likes?")

    with tab3:
        st.markdown('<div class="section-header">What will happen? — Sentiment & Trend Prediction</div>',
                    unsafe_allow_html=True)

        col1, col2 = st.columns(2)
        with col1:
            st.markdown("#### 😊 Sentiment Distribution")
            fig_sent = px.pie(sent_dist_h, values="count", names="sentiment", hole=0.5,
                color="sentiment",
                color_discrete_map={"positive":"#34d399","neutral":"#94a3b8","negative":"#f87171"})
            fig_sent.update_layout(
                plot_bgcolor="#0f172a", paper_bgcolor="#0a0a0f",
                font_color="#94a3b8", height=350)
            st.plotly_chart(fig_sent, use_container_width=True)
            st.caption("Overall emotional tone of trending YouTube titles?")

        with col2:
            st.markdown("#### 📊 Sentiment vs Engagement")
            fig_se2 = go.Figure()
            fig_se2.add_trace(go.Bar(
                x=sent_eng_h["sentiment"], y=sent_eng_h["avg_views"],
                name="Avg Views", marker_color="#38bdf8", yaxis="y"))
            fig_se2.add_trace(go.Scatter(
                x=sent_eng_h["sentiment"], y=sent_eng_h["avg_engagement"],
                name="Engagement", yaxis="y2",
                line=dict(color="#f472b6", width=3), mode="lines+markers"))
            fig_se2.update_layout(
                plot_bgcolor="#0f172a", paper_bgcolor="#0a0a0f", font_color="#94a3b8",
                height=350, xaxis=GRID,
                yaxis=dict(title="Avg Views", **GRID),
                yaxis2=dict(title="Avg Engagement", overlaying="y", side="right"),
                legend=dict(bgcolor="#0f172a"))
            st.plotly_chart(fig_se2, use_container_width=True)
            st.caption("Do positive titles drive more engagement?")

        st.markdown("#### 🎭 Sentiment by Category")
        sent_cat_f = sent_cat_h[sent_cat_h["category"].isin(sel_cats)]
        fig_scat2  = px.bar(sent_cat_f, x="category", y="count", color="sentiment",
            barmode="stack",
            color_discrete_map={"positive":"#34d399","neutral":"#94a3b8","negative":"#f87171"})
        fig_scat2.update_layout(**dark_layout(350),
            legend=dict(bgcolor="#0f172a", bordercolor="#334155"))
        st.plotly_chart(fig_scat2, use_container_width=True)

        col3, col4 = st.columns(2)
        with col3:
            st.markdown("#### ☁️ Trending Keywords")
            kw_dict_h = dict(zip(keywords_h["keyword"], keywords_h["count"]))
            if kw_dict_h:
                wc2 = WordCloud(width=600, height=300, background_color="#0a0a0f",
                                colormap="cool", max_words=40).generate_from_frequencies(kw_dict_h)
                fig_wc2, ax2 = plt.subplots(figsize=(6,3))
                fig_wc2.patch.set_facecolor("#0a0a0f")
                ax2.imshow(wc2, interpolation="bilinear")
                ax2.axis("off")
                st.pyplot(fig_wc2)
            st.caption("What topics dominate historical trending content?")

        with col4:
            st.markdown("#### 🏆 Top Trending Videos")
            top10_h = trending_h.head(10)[["title","category","views","engagement_rate","trending_score"]].copy()
            top10_h["title"]          = top10_h["title"].str[:35] + "..."
            top10_h["views"]          = top10_h["views"].apply(lambda x: f"{int(x):,}")
            top10_h["trending_score"] = top10_h["trending_score"].round(3)
            top10_h.columns = ["Title","Category","Views","Engagement","Score"]
            st.dataframe(top10_h, use_container_width=True, hide_index=True)

    with tab4:
        st.markdown('<div class="section-header">What should be done? — Data-Backed Recommendations</div>',
                    unsafe_allow_html=True)

        row = prescr_h.iloc[0]
        c1, c2, c3 = st.columns(3)
        recs = [
            ("📁 Best Category",    row["rec_category"]),
            ("📅 Best Day",         row["rec_day"]),
            ("⏰ Best Upload Time", row["rec_hour"]),
            ("📝 Title Length",     row["rec_title_length"]),
            ("🏷️ Tag Strategy",    row["rec_tags"]),
            ("😊 Title Sentiment",  row["rec_sentiment"]),
        ]
        for i, (title, val) in enumerate(recs):
            col = [c1,c2,c3][i%3]
            col.markdown(f'<div class="rec-box"><div class="insight-title">{title}</div><div class="insight-text">{val}</div></div>',
                         unsafe_allow_html=True)

        st.markdown("---")
        i1,i2,i3,i4 = st.columns(4)
        metrics = [
            ("Views ↔ Likes",             row["insight_1"].split(": ")[1], "#38bdf8"),
            ("Title Length ↔ Engagement", row["insight_2"].split(": ")[1], "#34d399"),
            ("Tags ↔ Views",              row["insight_3"].split(": ")[1], "#fb923c"),
            ("Sentiment ↔ Engagement",    row["insight_4"].split(": ")[1], "#f472b6"),
        ]
        for col, (title, val, color) in zip([i1,i2,i3,i4], metrics):
            col.markdown(f"""
            <div class="insight-box">
                <div class="insight-title">{title}</div>
                <div style="font-size:26px; color:{color}; font-weight:800;">{val}</div>
            </div>""", unsafe_allow_html=True)

# ── Auto refresh every 5 mins for live mode ───────────────────
if is_live:
    import time
    st.markdown("---")
    placeholder = st.empty()
    for i in range(300, 0, -1):
        placeholder.markdown(
            f'<div style="text-align:center; color:#334155; font-size:11px;">🔄 Auto-refreshing in {i}s</div>',
            unsafe_allow_html=True)
        time.sleep(1)
    st.rerun()

# # ── Footer ────────────────────────────────────────────────────
# st.markdown("""
# <div style='text-align:center; color:#1e293b; font-size:11px; padding:10px;'>
# InsightFlow · AI528 Big Data Analytics · IIT Ropar · Kafka → PySpark → Delta Lake → Streamlit
# </div>""", unsafe_allow_html=True)