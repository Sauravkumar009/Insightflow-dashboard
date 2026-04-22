import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from wordcloud import WordCloud
import matplotlib.pyplot as plt
import matplotlib
matplotlib.use("Agg")
import os

st.set_page_config(
    page_title="InsightFlow — YouTube Analytics",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)

st.markdown("""
<style>
    .stApp { background-color: #0a0a0f; color: #e2e8f0; }
    section[data-testid="stSidebar"] { background-color: #0f0f1a; border-right: 1px solid #1e293b; }
    .section-header {
        background: linear-gradient(90deg, #0f172a, transparent);
        border-left: 4px solid #38bdf8;
        padding: 8px 16px; margin: 20px 0 12px 0;
        border-radius: 0 8px 8px 0;
    }
    .insight-box {
        background: #0f172a; border: 1px solid #1e3a5f;
        border-radius: 10px; padding: 14px 18px; margin: 6px 0;
    }
    .insight-title { font-size: 13px; font-weight: 700; color: #38bdf8; margin-bottom: 4px; }
    .insight-text { font-size: 12px; color: #cbd5e1; line-height: 1.5; }
    .rec-box {
        background: linear-gradient(135deg, #0d2137, #0f172a);
        border: 1px solid #0ea5e9; border-radius: 10px;
        padding: 14px 18px; margin: 6px 0;
    }
    div[data-testid="metric-container"] {
        background: #0f172a; border: 1px solid #1e293b;
        border-radius: 10px; padding: 12px;
    }
    div[data-testid="stMetricValue"] { color: #38bdf8 !important; }
    h1, h2, h3 { color: #e2e8f0 !important; }
</style>
""", unsafe_allow_html=True)

GOLD_DIR      = os.path.join("outputs", "gold")
ANALYTICS_DIR = os.path.join("outputs", "analytics")
LIVE_FILE     = os.path.join("outputs", "youtube_api_live.csv")

# Dark layout base (no yaxis keys)
DARK_BASE = dict(
    plot_bgcolor  = "#0f172a",
    paper_bgcolor = "#0a0a0f",
    font_color    = "#94a3b8",
)
GRID = dict(gridcolor="#1e293b", linecolor="#334155")
COLORS = ["#38bdf8","#818cf8","#34d399","#fb923c","#f472b6","#a78bfa","#fbbf24","#4ade80"]

def dark_layout(height=400, **kwargs):
    return dict(**DARK_BASE, height=height,
                xaxis=GRID, yaxis=GRID, **kwargs)

@st.cache_data(ttl=300)
def load_data():
    kpis       = pd.read_csv(os.path.join(GOLD_DIR,      "kpis.csv"))
    cat_perf   = pd.read_csv(os.path.join(GOLD_DIR,      "category_performance.csv"))
    country    = pd.read_csv(os.path.join(GOLD_DIR,      "country_performance.csv"))
    channels   = pd.read_csv(os.path.join(GOLD_DIR,      "top_channels.csv"))
    day_perf   = pd.read_csv(os.path.join(GOLD_DIR,      "day_performance.csv"))
    hour_perf  = pd.read_csv(os.path.join(GOLD_DIR,      "hour_performance.csv"))
    corr       = pd.read_csv(os.path.join(ANALYTICS_DIR, "correlation_matrix.csv"), index_col=0)
    eng_hmap   = pd.read_csv(os.path.join(ANALYTICS_DIR, "engagement_heatmap.csv"))
    views_hmap = pd.read_csv(os.path.join(ANALYTICS_DIR, "views_heatmap.csv"))
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
            eng_hmap, views_hmap, title_eng, tag_eng, sent_dist, sent_eng,
            sent_cat, keywords, trending, prescr, scatter, like_ratio)

@st.cache_data(ttl=60)
def load_live():
    if os.path.exists(LIVE_FILE):
        return pd.read_csv(LIVE_FILE)
    return pd.DataFrame()

(kpis, cat_perf, country, channels, day_perf, hour_perf, corr,
 eng_hmap, views_hmap, title_eng, tag_eng, sent_dist, sent_eng,
 sent_cat, keywords, trending, prescr, scatter, like_ratio) = load_data()

live_df = load_live()
kpi = kpis.iloc[0]
day_order = ["Monday","Tuesday","Wednesday","Thursday","Friday","Saturday","Sunday"]

# ── Sidebar ───────────────────────────────────────────────────
with st.sidebar:
    st.markdown("## 📊 InsightFlow")
    st.markdown("*YouTube Analytics Dashboard*")
    st.markdown("---")
    all_cats     = sorted(cat_perf["category"].unique().tolist())
    sel_cats     = st.multiselect("Category",    all_cats,    default=all_cats)
    all_countries= sorted(country["publish_country"].unique().tolist())
    sel_countries= st.multiselect("Country",     all_countries, default=all_countries)
    sel_days     = st.multiselect("Day of Week", day_order,   default=day_order)
    sel_sentiment= st.multiselect("Sentiment",   ["positive","neutral","negative"],
                                  default=["positive","neutral","negative"])
    st.markdown("---")
    st.markdown("**Pipeline**")
    st.markdown("🔴 Kafka Simulated")
    st.markdown("⚡ PySpark 4.1")
    st.markdown("🏔️ Delta Lake")
    if len(live_df) > 0:
        st.markdown("---")
        st.markdown(f"**Live:** {len(live_df):,} videos · {live_df['fetch_time'].nunique()} runs")

# ── Header ────────────────────────────────────────────────────
st.markdown("""
<div style='text-align:center; padding:10px 0 20px 0;'>
<h1 style='font-size:36px; font-weight:900; color:#38bdf8; margin:0;'>📊 InsightFlow</h1>
<p style='color:#64748b; font-size:14px; margin:4px 0 0 0;'>
YouTube Trending Analytics · PySpark · Delta Lake · AI528 Big Data</p>
</div>
""", unsafe_allow_html=True)

tab1, tab2, tab3, tab4, tab5 = st.tabs([
    "📈 Descriptive", "🔍 Diagnostic", "🔮 Predictive",
    "💡 Prescriptive", "🔴 Live Data"
])

# ══════════════════════════════════════════
# TAB 1 — DESCRIPTIVE
# ══════════════════════════════════════════
with tab1:
    st.markdown('<div class="section-header"><b>What is happening? — Overview of YouTube Trending Data</b></div>',
                unsafe_allow_html=True)

    c1,c2,c3,c4,c5,c6 = st.columns(6)
    c1.metric("Total Videos",   f"{int(kpi['total_videos']):,}")
    c2.metric("Total Channels", f"{int(kpi['total_channels']):,}")
    c3.metric("Countries",      f"{int(kpi['total_countries']):,}")
    c4.metric("Avg Views",      f"{int(kpi['avg_views']):,}")
    c5.metric("Avg Likes",      f"{int(kpi['avg_likes']):,}")
    c6.metric("Avg Engagement", f"{kpi['avg_engagement_rate']:.3f}")

    st.markdown("---")
    cat_f = cat_perf[cat_perf["category"].isin(sel_cats)]

    col1, col2 = st.columns(2)
    with col1:
        st.markdown("#### 📊 Category — Views vs Engagement")
        fig = px.scatter(cat_f,
            x="avg_views", y="avg_engagement",
            size="total_videos", color="category",
            hover_data=["avg_likes","avg_comments","total_videos"],
            color_discrete_sequence=COLORS,
            labels={"avg_views":"Avg Views","avg_engagement":"Avg Engagement Rate"})
        fig.update_layout(**dark_layout(380), showlegend=True)
        st.plotly_chart(fig, use_container_width=True)
        st.caption("Which categories have high views AND high engagement simultaneously?")

    with col2:
        st.markdown("#### 🌍 Top Countries by Video Count")
        country_f = country[country["publish_country"].isin(sel_countries)]
        fig2 = px.bar(country_f.head(10).sort_values("total_videos"),
            x="total_videos", y="publish_country", orientation="h",
            color="avg_engagement", color_continuous_scale="Blues",
            labels={"total_videos":"Total Videos","publish_country":"Country"})
        fig2.update_layout(**dark_layout(380), showlegend=False)
        st.plotly_chart(fig2, use_container_width=True)
        st.caption("Which countries produce the most trending content?")

    col3, col4 = st.columns(2)
    with col3:
        st.markdown("#### 📅 Engagement & Views by Day of Week")
        day_f = day_perf[day_perf["published_day_of_week"].isin(sel_days)]
        fig3 = go.Figure()
        fig3.add_trace(go.Bar(
            x=day_f["published_day_of_week"], y=day_f["avg_views"],
            name="Avg Views", marker_color="#38bdf8", yaxis="y",
            hovertemplate="Day: %{x}<br>Avg Views: %{y:,.0f}<extra></extra>"))
        fig3.add_trace(go.Scatter(
            x=day_f["published_day_of_week"], y=day_f["avg_engagement"],
            name="Avg Engagement", line=dict(color="#f472b6", width=3),
            yaxis="y2", mode="lines+markers",
            hovertemplate="Day: %{x}<br>Engagement: %{y:.4f}<extra></extra>"))
        fig3.update_layout(
            plot_bgcolor="#0f172a", paper_bgcolor="#0a0a0f", font_color="#94a3b8",
            height=380,
            xaxis=GRID,
            yaxis=dict(title="Avg Views", **GRID),
            yaxis2=dict(title="Engagement Rate", overlaying="y", side="right"),
            legend=dict(bgcolor="#0f172a", bordercolor="#334155"))
        st.plotly_chart(fig3, use_container_width=True)
        st.caption("Is there a relationship between publish day and engagement?")

    with col4:
        st.markdown("#### 🕐 Views by Publish Hour")
        fig4 = px.bar(hour_perf.sort_values("publish_hour"),
            x="publish_hour", y="avg_views",
            color="avg_views", color_continuous_scale="Blues",
            labels={"publish_hour":"Hour of Day (24hr)","avg_views":"Avg Views"})
        fig4.update_layout(**dark_layout(380), showlegend=False)
        st.plotly_chart(fig4, use_container_width=True)
        st.caption("What time of day produces the most viewed videos?")

    st.markdown("#### 🏆 Top 15 Channels by Total Views")
    fig5 = px.bar(channels.sort_values("total_views"),
        x="total_views", y="channel_title", orientation="h",
        color="avg_engagement", color_continuous_scale="Teal",
        labels={"total_views":"Total Views","channel_title":"Channel"})
    fig5.update_layout(**dark_layout(500), showlegend=False)
    st.plotly_chart(fig5, use_container_width=True)
    st.caption("Do top-viewed channels also have high engagement rates?")

# ══════════════════════════════════════════
# TAB 2 — DIAGNOSTIC
# ══════════════════════════════════════════
with tab2:
    st.markdown('<div class="section-header"><b>Why is it happening? — Correlation & Relationship Analysis</b></div>',
                unsafe_allow_html=True)

    st.markdown("#### 🔥 Full Correlation Matrix — All Key Variables")
    fig_corr = px.imshow(corr,
        color_continuous_scale="RdBu_r", zmin=-1, zmax=1,
        text_auto=True, aspect="auto")
    fig_corr.update_layout(
        plot_bgcolor="#0f172a", paper_bgcolor="#0a0a0f",
        font_color="#94a3b8", height=500)
    fig_corr.update_traces(textfont_size=11)
    st.plotly_chart(fig_corr, use_container_width=True)
    st.caption("How do views, likes, comments, title length, tags, and sentiment relate to each other?")

    st.markdown("""
    <div class="insight-box">
        <div class="insight-title">🔍 Key Correlation Insights</div>
        <div class="insight-text">
        • <b>Views ↔ Likes:</b> Strong positive — more views almost always means more likes<br>
        • <b>Views ↔ Comments:</b> Strong positive — popular videos drive discussion<br>
        • <b>Title Length ↔ Engagement:</b> Negative — shorter titles perform better<br>
        • <b>Tag Count ↔ Views:</b> Near zero — tag count barely affects viewership<br>
        • <b>Sentiment ↔ Engagement:</b> Near zero — emotional tone has minimal effect on engagement
        </div>
    </div>""", unsafe_allow_html=True)

    st.markdown("---")
    col1, col2 = st.columns(2)

    with col1:
        st.markdown("#### 🔥 Engagement Heatmap — Category × Day")
        eng_f = eng_hmap[eng_hmap["category"].isin(sel_cats) & eng_hmap["day"].isin(sel_days)]
        if len(eng_f) > 0:
            eng_pivot = eng_f.pivot(index="category", columns="day", values="engagement_rate").fillna(0)
            day_cols  = [d for d in day_order if d in eng_pivot.columns]
            eng_pivot = eng_pivot[day_cols]
            fig_heat  = px.imshow(eng_pivot, color_continuous_scale="Blues",
                                  text_auto=".3f", aspect="auto")
            fig_heat.update_layout(
                plot_bgcolor="#0f172a", paper_bgcolor="#0a0a0f",
                font_color="#94a3b8", height=450)
            st.plotly_chart(fig_heat, use_container_width=True)
        st.caption("Which Category + Day combination maximises engagement?")

    with col2:
        st.markdown("#### ⏰ Views Heatmap — Hour × Day")
        views_f = views_hmap[views_hmap["day"].isin(sel_days)]
        if len(views_f) > 0:
            views_pivot = views_f.pivot(index="hour", columns="day", values="avg_views").fillna(0)
            day_cols2   = [d for d in day_order if d in views_pivot.columns]
            views_pivot = views_pivot[day_cols2]
            fig_heat2   = px.imshow(views_pivot, color_continuous_scale="Reds",
                                    aspect="auto",
                                    labels=dict(x="Day", y="Hour", color="Avg Views"))
            fig_heat2.update_layout(
                plot_bgcolor="#0f172a", paper_bgcolor="#0a0a0f",
                font_color="#94a3b8", height=450)
            st.plotly_chart(fig_heat2, use_container_width=True)
        st.caption("What hour + day combination gets most views?")

    col3, col4 = st.columns(2)
    with col3:
        st.markdown("#### 📝 Title Length vs Engagement Rate")
        fig_title = go.Figure()
        fig_title.add_trace(go.Bar(
            x=title_eng["title_length_bucket"].astype(str),
            y=title_eng["avg_engagement"],
            name="Engagement", marker_color="#38bdf8", yaxis="y",
            hovertemplate="Length: %{x}<br>Engagement: %{y:.4f}<extra></extra>"))
        fig_title.add_trace(go.Scatter(
            x=title_eng["title_length_bucket"].astype(str),
            y=title_eng["avg_views"],
            name="Avg Views", yaxis="y2",
            line=dict(color="#f472b6", width=2), mode="lines+markers"))
        fig_title.update_layout(
            plot_bgcolor="#0f172a", paper_bgcolor="#0a0a0f", font_color="#94a3b8",
            height=350, xaxis=dict(title="Title Length (chars)", **GRID),
            yaxis=dict(title="Avg Engagement Rate", **GRID),
            yaxis2=dict(title="Avg Views", overlaying="y", side="right"),
            legend=dict(bgcolor="#0f172a"))
        st.plotly_chart(fig_title, use_container_width=True)
        st.caption("Does title length affect video performance?")

    with col4:
        st.markdown("#### 🏷️ Tag Count vs Avg Views")
        fig_tags = go.Figure()
        fig_tags.add_trace(go.Bar(
            x=tag_eng["tag_bucket"].astype(str),
            y=tag_eng["avg_views"],
            name="Avg Views", marker_color="#34d399", yaxis="y",
            hovertemplate="Tags: %{x}<br>Avg Views: %{y:,.0f}<extra></extra>"))
        fig_tags.add_trace(go.Scatter(
            x=tag_eng["tag_bucket"].astype(str),
            y=tag_eng["avg_engagement"],
            name="Engagement", yaxis="y2",
            line=dict(color="#fb923c", width=2), mode="lines+markers"))
        fig_tags.update_layout(
            plot_bgcolor="#0f172a", paper_bgcolor="#0a0a0f", font_color="#94a3b8",
            height=350, xaxis=dict(title="Number of Tags", **GRID),
            yaxis=dict(title="Avg Views", **GRID),
            yaxis2=dict(title="Avg Engagement", overlaying="y", side="right"),
            legend=dict(bgcolor="#0f172a"))
        st.plotly_chart(fig_tags, use_container_width=True)
        st.caption("Does using more tags improve discoverability?")

    st.markdown("#### 🔵 Views vs Likes vs Engagement — 3-Way Relationship")
    scatter_f = scatter[scatter["category"].isin(sel_cats) & scatter["sentiment"].isin(sel_sentiment)]
    fig_scatter = px.scatter(scatter_f,
        x="views", y="likes", color="category", size="engagement_rate",
        hover_data=["title","channel_title","comment_count"],
        color_discrete_sequence=COLORS, opacity=0.7,
        labels={"views":"Views","likes":"Likes","category":"Category"})
    fig_scatter.update_layout(**dark_layout(450))
    st.plotly_chart(fig_scatter, use_container_width=True)
    st.caption("Do high-view videos always have proportionally high likes? Which categories break this pattern?")

    st.markdown("#### ❤️ Like Ratio by Category (Audience Approval Score)")
    like_f = like_ratio[like_ratio["category"].isin(sel_cats)]
    fig_like = px.bar(like_f.sort_values("avg_like_ratio", ascending=False),
        x="category", y="avg_like_ratio",
        color="avg_like_ratio", color_continuous_scale="Greens",
        labels={"avg_like_ratio":"Like Ratio","category":"Category"})
    fig_like.update_layout(**dark_layout(350), showlegend=False)
    st.plotly_chart(fig_like, use_container_width=True)
    st.caption("Which categories have the most audience approval (fewest dislikes relative to likes)?")

# ══════════════════════════════════════════
# TAB 3 — PREDICTIVE
# ══════════════════════════════════════════
with tab3:
    st.markdown('<div class="section-header"><b>What will happen? — Sentiment & Trend Prediction</b></div>',
                unsafe_allow_html=True)

    col1, col2 = st.columns(2)
    with col1:
        st.markdown("#### 😊 Sentiment Distribution")
        fig_sent = px.pie(sent_dist, values="count", names="sentiment", hole=0.5,
            color="sentiment",
            color_discrete_map={"positive":"#34d399","neutral":"#94a3b8","negative":"#f87171"})
        fig_sent.update_layout(
            plot_bgcolor="#0f172a", paper_bgcolor="#0a0a0f",
            font_color="#94a3b8", height=350)
        fig_sent.update_traces(hovertemplate="%{label}: %{value:,} (%{percent})<extra></extra>")
        st.plotly_chart(fig_sent, use_container_width=True)
        st.caption("What is the overall emotional tone of trending YouTube titles?")

    with col2:
        st.markdown("#### 📊 Sentiment vs Engagement — Does Tone Matter?")
        fig_se = go.Figure()
        fig_se.add_trace(go.Bar(
            x=sent_eng["sentiment"], y=sent_eng["avg_views"],
            name="Avg Views", marker_color="#38bdf8", yaxis="y"))
        fig_se.add_trace(go.Scatter(
            x=sent_eng["sentiment"], y=sent_eng["avg_engagement"],
            name="Avg Engagement", yaxis="y2",
            line=dict(color="#f472b6", width=3), mode="lines+markers+text",
            text=[f"{v:.4f}" for v in sent_eng["avg_engagement"]],
            textposition="top center"))
        fig_se.update_layout(
            plot_bgcolor="#0f172a", paper_bgcolor="#0a0a0f", font_color="#94a3b8",
            height=350,
            xaxis=GRID,
            yaxis=dict(title="Avg Views", **GRID),
            yaxis2=dict(title="Avg Engagement", overlaying="y", side="right"),
            legend=dict(bgcolor="#0f172a"))
        st.plotly_chart(fig_se, use_container_width=True)
        st.caption("Do positive/negative titles drive more engagement than neutral ones?")

    st.markdown("#### 🎭 Sentiment Distribution by Category")
    sent_cat_f = sent_cat[sent_cat["category"].isin(sel_cats)]
    fig_scat = px.bar(sent_cat_f, x="category", y="count", color="sentiment",
        barmode="stack",
        color_discrete_map={"positive":"#34d399","neutral":"#94a3b8","negative":"#f87171"},
        labels={"count":"Number of Videos","category":"Category"})
    fig_scat.update_layout(**dark_layout(380),
        legend=dict(bgcolor="#0f172a", bordercolor="#334155"))
    st.plotly_chart(fig_scat, use_container_width=True)
    st.caption("Which categories tend to use more emotional/negative titles?")

    col3, col4 = st.columns(2)
    with col3:
        st.markdown("#### ☁️ Trending Keywords Wordcloud")
        kw_dict = dict(zip(keywords["keyword"], keywords["count"]))
        if kw_dict:
            wc = WordCloud(width=700, height=350, background_color="#0a0a0f",
                           colormap="cool", max_words=50).generate_from_frequencies(kw_dict)
            fig_wc, ax = plt.subplots(figsize=(7, 3.5))
            fig_wc.patch.set_facecolor("#0a0a0f")
            ax.imshow(wc, interpolation="bilinear")
            ax.axis("off")
            st.pyplot(fig_wc)
        st.caption("What topics dominate trending YouTube content?")

    with col4:
        st.markdown("#### 🏆 Top Trending Videos (Composite Score)")
        top10 = trending.head(10)[["title","category","views","engagement_rate","trending_score"]].copy()
        top10["title"]          = top10["title"].str[:40] + "..."
        top10["views"]          = top10["views"].apply(lambda x: f"{int(x):,}")
        top10["trending_score"] = top10["trending_score"].round(3)
        top10.columns = ["Title","Category","Views","Engagement","Score"]
        st.dataframe(top10, use_container_width=True, hide_index=True)
        st.caption("Score = 40% views + 30% engagement + 20% like ratio + 10% comment rate")

# ══════════════════════════════════════════
# TAB 4 — PRESCRIPTIVE
# ══════════════════════════════════════════
with tab4:
    st.markdown('<div class="section-header"><b>What should be done? — Data-Backed Recommendations</b></div>',
                unsafe_allow_html=True)

    row = prescr.iloc[0]
    st.markdown("### 🎯 Upload Strategy Recommendations")

    c1, c2, c3 = st.columns(3)
    with c1:
        st.markdown(f'<div class="rec-box"><div class="insight-title">📁 Best Category</div><div class="insight-text">{row["rec_category"]}</div></div>', unsafe_allow_html=True)
        st.markdown(f'<div class="rec-box"><div class="insight-title">📅 Best Day</div><div class="insight-text">{row["rec_day"]}</div></div>', unsafe_allow_html=True)
    with c2:
        st.markdown(f'<div class="rec-box"><div class="insight-title">⏰ Best Upload Time</div><div class="insight-text">{row["rec_hour"]}</div></div>', unsafe_allow_html=True)
        st.markdown(f'<div class="rec-box"><div class="insight-title">📝 Title Length</div><div class="insight-text">{row["rec_title_length"]}</div></div>', unsafe_allow_html=True)
    with c3:
        st.markdown(f'<div class="rec-box"><div class="insight-title">🏷️ Tag Strategy</div><div class="insight-text">{row["rec_tags"]}</div></div>', unsafe_allow_html=True)
        st.markdown(f'<div class="rec-box"><div class="insight-title">😊 Title Sentiment</div><div class="insight-text">{row["rec_sentiment"]}</div></div>', unsafe_allow_html=True)

    st.markdown("---")
    st.markdown("### 📊 Key Correlation Findings")

    i1, i2, i3, i4 = st.columns(4)
    metrics = [
        ("Views ↔ Likes",          row["insight_1"].split(": ")[1], "Very strong positive correlation",        "#38bdf8"),
        ("Title Length ↔ Engagement", row["insight_2"].split(": ")[1], "Shorter titles = better performance",  "#34d399"),
        ("Tags ↔ Views",           row["insight_3"].split(": ")[1], "Tag count barely affects viewership",     "#fb923c"),
        ("Sentiment ↔ Engagement", row["insight_4"].split(": ")[1], "Tone has minimal effect on engagement",   "#f472b6"),
    ]
    for col, (title, val, desc, color) in zip([i1,i2,i3,i4], metrics):
        col.markdown(f"""
        <div class="insight-box">
            <div class="insight-title">{title}</div>
            <div style="font-size:26px; color:{color}; font-weight:800;">{val}</div>
            <div class="insight-text">{desc}</div>
        </div>""", unsafe_allow_html=True)

    st.markdown("---")
    st.markdown("### 🗺️ Optimal Upload Strategy Summary")
    st.markdown(f"""
    <div class="insight-box" style="padding:20px;">
        <div class="insight-text" style="font-size:14px; line-height:2.2;">
        Based on analysis of <b style="color:#38bdf8;">{int(kpi['total_videos']):,} trending YouTube videos</b>:<br>
        ✅ <b>Category:</b> {row['rec_category']}<br>
        ✅ <b>Publish Day:</b> {row['rec_day']}<br>
        ✅ <b>Upload Time:</b> {row['rec_hour']}<br>
        ✅ <b>Title Length:</b> {row['rec_title_length']}<br>
        ✅ <b>Tags:</b> {row['rec_tags']}<br>
        ✅ <b>Tone:</b> {row['rec_sentiment']}
        </div>
    </div>""", unsafe_allow_html=True)

# ══════════════════════════════════════════
# TAB 5 — LIVE DATA
# ══════════════════════════════════════════
with tab5:
    st.markdown('<div class="section-header"><b>🔴 Live YouTube API Data — Real-Time Trending</b></div>',
                unsafe_allow_html=True)

    if len(live_df) == 0:
        st.warning("No live data yet. Run scheduler.py to start collecting.")
    else:
        total_runs = live_df["fetch_time"].nunique()
        l1,l2,l3,l4 = st.columns(4)
        l1.metric("Videos Collected", f"{len(live_df):,}")
        l2.metric("Collection Runs",  f"{total_runs}")
        l3.metric("Avg Live Views",   f"{int(live_df['views'].mean()):,}")
        l4.metric("Last Fetch",       live_df["fetch_time"].iloc[-1])

        st.markdown("---")
        col1, col2 = st.columns(2)

        with col1:
            st.markdown("#### 📈 View Count Growth — Top 5 Videos")
            if total_runs > 1:
                top5    = live_df.groupby("title")["views"].max().nlargest(5).index.tolist()
                growth  = live_df[live_df["title"].isin(top5)].copy()
                growth["short"] = growth["title"].str[:30] + "..."
                fig_g = px.line(growth.sort_values("fetch_time"),
                    x="fetch_time", y="views", color="short",
                    markers=True, color_discrete_sequence=COLORS,
                    labels={"fetch_time":"Time","views":"Views","short":"Video"})
                fig_g.update_layout(**dark_layout(380))
                st.plotly_chart(fig_g, use_container_width=True)
            else:
                st.info("Need 2+ collection runs to show growth. Scheduler running...")
                top5_df = live_df.nlargest(5,"views")[["title","category","views","likes"]]
                top5_df["title"] = top5_df["title"].str[:40] + "..."
                st.dataframe(top5_df, hide_index=True, use_container_width=True)

        with col2:
            st.markdown("#### 🏆 Live Categories by Avg Views")
            live_cat = live_df.groupby("category")["views"].mean().reset_index()
            live_cat.columns = ["Category","Avg Views"]
            fig_lc = px.bar(live_cat.sort_values("Avg Views"),
                x="Avg Views", y="Category", orientation="h",
                color="Avg Views", color_continuous_scale="Blues")
            fig_lc.update_layout(**dark_layout(380), showlegend=False)
            st.plotly_chart(fig_lc, use_container_width=True)

        st.markdown("#### 📋 Current Top 20 Trending Videos")
        live_top = live_df.nlargest(20,"views")[
            ["title","category","views","likes","comments","sentiment","fetch_time"]].copy()
        live_top["title"] = live_top["title"].str[:50] + "..."
        live_top["views"] = live_top["views"].apply(lambda x: f"{int(x):,}")
        live_top["likes"] = live_top["likes"].apply(lambda x: f"{int(x):,}")
        st.dataframe(live_top, hide_index=True, use_container_width=True)

# ── Footer ────────────────────────────────────────────────────
st.markdown("---")
st.markdown("""
<div style='text-align:center; color:#334155; font-size:11px; padding:10px;'>
InsightFlow · AI528 Big Data Analytics · IIT Ropar · Pipeline: Kafka → PySpark 4.1 → Delta Lake → Streamlit
</div>""", unsafe_allow_html=True)