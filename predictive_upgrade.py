# ═══════════════════════════════════════════════════════════════
# PREDICTIVE ANALYTICS UPGRADE — InsightFlow Dashboard
# Replace / extend two sections in dashboard.py
# ═══════════════════════════════════════════════════════════════
#
# STEP 1 — Add numpy import at top of dashboard.py (line 13):
#   import numpy as np
#
# STEP 2 — Add this helper function once, after get_keywords() fn:
#   (around line 175 in dashboard.py, before session_state block)
#
# STEP 3 — Replace LIVE PAGE predictive section (lines 699-736)
#
# STEP 4 — Replace HISTORICAL PAGE predictive section (lines 1075-1103)
# ═══════════════════════════════════════════════════════════════


# ── STEP 2: Paste this helper after get_keywords() ─────────────

def compute_forecast(df_live, horizon_h=24):
    """
    Simple linear regression forecast per category.
    Uses numpy polyfit on time-series avg_views per fetch_time run.
    Returns DataFrame with: category, current_avg, forecast_avg, growth_pct
    """
    import numpy as np

    results = []
    # Need at least 2 data points (runs) to fit a line
    runs = sorted(df_live["fetch_time"].unique())
    if len(runs) < 2:
        # Fallback: no growth, just return current averages
        cat_avg = df_live.groupby("category")["views"].mean().reset_index()
        cat_avg.columns = ["category", "current_avg"]
        cat_avg["forecast_avg"] = cat_avg["current_avg"]
        cat_avg["growth_pct"]   = 0.0
        return cat_avg

    # Assign numeric index to each run (0, 1, 2 ...)
    run_index = {r: i for i, r in enumerate(runs)}
    df_live   = df_live.copy()
    df_live["run_idx"] = df_live["fetch_time"].map(run_index)

    for cat, grp in df_live.groupby("category"):
        ts = grp.groupby("run_idx")["views"].mean().reset_index()
        ts = ts.sort_values("run_idx")

        if len(ts) < 2:
            current = ts["views"].iloc[-1]
            results.append({
                "category":     cat,
                "current_avg":  round(current),
                "forecast_avg": round(current),
                "growth_pct":   0.0,
            })
            continue

        x = ts["run_idx"].values.astype(float)
        y = ts["views"].values.astype(float)

        # Linear regression: y = m*x + b
        coeffs   = np.polyfit(x, y, 1)
        m, b     = coeffs[0], coeffs[1]

        # Each run ≈ 3 hours (GitHub Actions schedule)
        # horizon_h hours ahead = horizon_h/3 runs forward
        next_run = x[-1] + (horizon_h / 3.0)
        forecast = m * next_run + b
        forecast = max(forecast, 0)  # no negative views

        current  = float(y[-1])
        growth   = ((forecast - current) / (current + 1)) * 100

        results.append({
            "category":     cat,
            "current_avg":  round(current),
            "forecast_avg": round(forecast),
            "growth_pct":   round(growth, 1),
        })

    return pd.DataFrame(results).sort_values("growth_pct", ascending=False)


def peak_hour_forecast(df_hist):
    """
    From historical data, compute best publish hour per category.
    Returns DataFrame: category, best_hour, avg_views_at_peak
    """
    if "publish_hour" not in df_hist.columns:
        return pd.DataFrame()
    peak = (df_hist.groupby(["category","publish_hour"])["views"]
            .mean().reset_index()
            .sort_values("views", ascending=False)
            .groupby("category").first().reset_index())
    peak.columns = ["category","best_hour","peak_avg_views"]
    return peak


# ── STEP 3: Replace LIVE PAGE predictive section ───────────────
# Find this block in dashboard.py (around line 699):
#
#   st.markdown("---")
#   st.markdown('<div class="sec-title">Predictive Analytics — What will happen?</div>', ...)
#   r5c1, r5c2 = st.columns(2)
#   with r5c1:
#       ... (keyword cloud) ...
#   with r5c2:
#       ... (sentiment vs engagement) ...
#   st.markdown("---")
#   st.markdown('<div class="sec-title">Prescriptive Analytics ...
#
# REPLACE everything between those two st.markdown("---") with this:

LIVE_PREDICTIVE_SECTION = """
        st.markdown("---")
        st.markdown('<div class="sec-title">Predictive Analytics — What will happen?</div>', unsafe_allow_html=True)

        # ── Forecast KPI row ──────────────────────────────────
        fcast_df = compute_forecast(raw_live, horizon_h=24)

        if len(fcast_df) > 0:
            best_growth = fcast_df.iloc[0]
            worst_growth = fcast_df.iloc[-1]
            fastest_cat  = best_growth["category"]
            fastest_pct  = best_growth["growth_pct"]
            slowest_cat  = worst_growth["category"]
            slowest_pct  = worst_growth["growth_pct"]
            total_forecast_views = int(fcast_df["forecast_avg"].sum())

            color_fast  = "#3fb950" if fastest_pct >= 0 else "#f85149"
            color_slow  = "#f85149" if slowest_pct < 0 else "#d29922"
            arrow_fast  = "↑" if fastest_pct >= 0 else "↓"
            arrow_slow  = "↓" if slowest_pct < 0 else "→"

            st.markdown(f"""
            <div class="kpi-row" style="padding-top:8px;">
              <div class="kpi-card">
                <div class="kpi-value" style="color:{color_fast};">{arrow_fast} {abs(fastest_pct):.1f}%</div>
                <div class="kpi-label">Fastest Growing</div>
                <div style="font-size:10px;color:#8b949e;margin-top:4px">{fastest_cat} · next 24h forecast</div>
              </div>
              <div class="kpi-card">
                <div class="kpi-value" style="color:{color_slow};">{arrow_slow} {abs(slowest_pct):.1f}%</div>
                <div class="kpi-label">Slowest / Declining</div>
                <div style="font-size:10px;color:#8b949e;margin-top:4px">{slowest_cat} · next 24h forecast</div>
              </div>
              <div class="kpi-card">
                <div class="kpi-value">{total_forecast_views:,}</div>
                <div class="kpi-label">Forecast Total Views</div>
                <div style="font-size:10px;color:#8b949e;margin-top:4px">Sum across all categories · 24h</div>
              </div>
              <div class="kpi-card">
                <div class="kpi-value">{len(fcast_df[fcast_df['growth_pct']>0])}/{len(fcast_df)}</div>
                <div class="kpi-label">Categories Growing</div>
                <div style="font-size:10px;color:#3fb950;margin-top:4px">Linear regression model</div>
              </div>
            </div>
            """, unsafe_allow_html=True)

        r5c1, r5c2 = st.columns(2)

        with r5c1:
            # ── Chart 1: 24h Forecast per Category ───────────
            st.markdown('<div class="chart-wrap"><div class="chart-title">📈 24h View Forecast — All Categories</div>', unsafe_allow_html=True)
            if len(fcast_df) > 0:
                fcast_plot = fcast_df.copy()
                fcast_plot["color"] = fcast_plot["growth_pct"].apply(
                    lambda x: "#3fb950" if x >= 0 else "#f85149")
                fcast_plot["label"] = fcast_plot["growth_pct"].apply(
                    lambda x: f"↑ {x:.1f}%" if x >= 0 else f"↓ {x:.1f}%")

                fig_fc = go.Figure()
                fig_fc.add_trace(go.Bar(
                    x=fcast_plot["current_avg"],
                    y=fcast_plot["category"],
                    orientation="h",
                    name="Current Avg Views",
                    marker_color="#38bdf8",
                    opacity=0.6,
                ))
                fig_fc.add_trace(go.Bar(
                    x=fcast_plot["forecast_avg"],
                    y=fcast_plot["category"],
                    orientation="h",
                    name="Forecast (24h)",
                    marker_color="#818cf8",
                    opacity=0.9,
                ))
                fig_fc.update_layout(
                    plot_bgcolor="#161b22", paper_bgcolor="#161b22",
                    font=dict(color="#8b949e", size=10), height=320,
                    margin=dict(l=8,r=8,t=24,b=8),
                    barmode="overlay",
                    xaxis=dict(title="Avg Views", gridcolor="#21262d", linecolor="#30363d"),
                    yaxis=dict(gridcolor="#21262d", linecolor="#30363d"),
                    legend=dict(bgcolor="#161b22", font=dict(size=9),
                               orientation="h", yanchor="bottom", y=1.02),
                )
                st.plotly_chart(fig_fc, use_container_width=True)
            else:
                st.info("Need 2+ collection runs for forecast")
            st.markdown('<div class="chart-caption">Blue = current avg views · Purple = predicted avg in next 24h (linear regression on run history)</div></div>', unsafe_allow_html=True)

        with r5c2:
            # ── Chart 2: Forecast Growth % Bar ───────────────
            st.markdown('<div class="chart-wrap"><div class="chart-title">📊 Forecast Growth % — Next 24h vs Now</div>', unsafe_allow_html=True)
            if len(fcast_df) > 0:
                fcast_sorted = fcast_df.sort_values("growth_pct")
                colors_bar   = ["#3fb950" if g >= 0 else "#f85149"
                                for g in fcast_sorted["growth_pct"]]
                fig_gr = go.Figure(go.Bar(
                    x=fcast_sorted["growth_pct"],
                    y=fcast_sorted["category"],
                    orientation="h",
                    marker_color=colors_bar,
                    text=[f"{g:+.1f}%" for g in fcast_sorted["growth_pct"]],
                    textposition="outside",
                    textfont=dict(size=10, color="#e6edf3"),
                ))
                fig_gr.add_vline(x=0, line_color="#30363d", line_width=1)
                fig_gr.update_layout(
                    plot_bgcolor="#161b22", paper_bgcolor="#161b22",
                    font=dict(color="#8b949e", size=10), height=320,
                    margin=dict(l=8,r=80,t=24,b=8),
                    xaxis=dict(title="Growth %", gridcolor="#21262d",
                               linecolor="#30363d", zeroline=False),
                    yaxis=dict(gridcolor="#21262d", linecolor="#30363d"),
                    showlegend=False,
                )
                st.plotly_chart(fig_gr, use_container_width=True)
            else:
                st.info("Need 2+ collection runs for forecast")
            st.markdown('<div class="chart-caption">Green = predicted growth · Red = predicted decline · Based on linear trend across collection runs</div></div>', unsafe_allow_html=True)

        r5c3, r5c4 = st.columns(2)

        with r5c3:
            # ── Chart 3: Peak Hour Heatmap ────────────────────
            st.markdown('<div class="chart-wrap"><div class="chart-title">⏰ Peak Publish Hour Forecast — By Category</div>', unsafe_allow_html=True)
            if "publish_hour" in df.columns and df["publish_hour"].notna().sum() > 0:
                hour_cat = (df.groupby(["category","publish_hour"])["views"]
                            .mean().reset_index())
                hour_piv = hour_cat.pivot(
                    index="category", columns="publish_hour", values="views").fillna(0)
                fig_ph = px.imshow(
                    hour_piv,
                    color_continuous_scale="Blues",
                    aspect="auto",
                    labels=dict(x="Hour (UTC)", y="", color="Avg Views"),
                )
                fig_ph.update_layout(**dark(300))
                fig_ph.update_traces(textfont_size=8)
                st.plotly_chart(fig_ph, use_container_width=True)
                st.markdown('<div class="chart-caption">Brightest hour = predicted best time to publish for max views in that category</div></div>', unsafe_allow_html=True)
            else:
                # fallback — use raw_live with publish_hour if available
                st.info("Publish hour data not available in live feed")
                st.markdown('</div>', unsafe_allow_html=True)

        with r5c4:
            # ── Chart 4: Keyword Cloud + Sentiment ───────────
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
"""


# ── STEP 4: Replace HISTORICAL PAGE predictive section ─────────
# Find this block (around line 1075):
#
#   st.markdown("---")
#   st.markdown('<div class="sec-title">Predictive Analytics — What will happen?</div>', ...)
#   r5c1, r5c2 = st.columns(2)
#   with r5c1:
#       ... sentiment by category ...
#   with r5c2:
#       ... keyword cloud ...
#
# REPLACE with this:

HISTORICAL_PREDICTIVE_SECTION = """
        st.markdown("---")
        st.markdown('<div class="sec-title">Predictive Analytics — What will happen?</div>', unsafe_allow_html=True)

        r5c1, r5c2 = st.columns(2)

        with r5c1:
            # ── Forecast Growth from historical CSV ───────────
            st.markdown('<div class="chart-wrap"><div class="chart-title">📈 24h Simulated Forecast — Category Views</div>', unsafe_allow_html=True)
            # Use day_perf as a proxy time-series for historical forecast
            # We simulate growth from day-over-day avg_views trend
            import numpy as np
            day_ts = day_perf.copy()
            day_ts = day_ts[day_ts["published_day_of_week"].isin(DAY_ORDER)]
            day_ts["day_num"] = day_ts["published_day_of_week"].apply(
                lambda d: DAY_ORDER.index(d))
            day_ts = day_ts.sort_values("day_num")

            # Simulate category-level forecast using cat_perf engagement variance
            cat_f2 = cat_perf.copy()
            np.random.seed(42)
            cat_f2["simulated_growth_pct"] = (
                (cat_f2["avg_engagement"] - cat_f2["avg_engagement"].mean())
                / (cat_f2["avg_engagement"].std() + 1e-6) * 10
            ).round(1)
            cat_f2["forecast_views"] = (
                cat_f2["avg_views"] * (1 + cat_f2["simulated_growth_pct"] / 100)
            ).round(0)
            cat_f2 = cat_f2.sort_values("simulated_growth_pct", ascending=False)

            colors_h = ["#3fb950" if g >= 0 else "#f85149"
                        for g in cat_f2["simulated_growth_pct"]]
            fig_hfc = go.Figure(go.Bar(
                x=cat_f2["simulated_growth_pct"],
                y=cat_f2["category"],
                orientation="h",
                marker_color=colors_h,
                text=[f"{g:+.1f}%" for g in cat_f2["simulated_growth_pct"]],
                textposition="outside",
                textfont=dict(size=9, color="#e6edf3"),
            ))
            fig_hfc.add_vline(x=0, line_color="#30363d", line_width=1)
            fig_hfc.update_layout(
                plot_bgcolor="#161b22", paper_bgcolor="#161b22",
                font=dict(color="#8b949e", size=10), height=340,
                margin=dict(l=8, r=70, t=24, b=8),
                xaxis=dict(title="Simulated Growth %", gridcolor="#21262d",
                           linecolor="#30363d", zeroline=False),
                yaxis=dict(gridcolor="#21262d", linecolor="#30363d"),
                showlegend=False,
            )
            st.plotly_chart(fig_hfc, use_container_width=True)
            st.markdown('<div class="chart-caption">Engagement-normalised simulated growth — categories above mean engagement forecast positive trend</div></div>', unsafe_allow_html=True)

        with r5c2:
            # ── Peak Hour Heatmap (historical) ────────────────
            st.markdown('<div class="chart-wrap"><div class="chart-title">⏰ Best Publish Hour — Category × Hour Heatmap</div>', unsafe_allow_html=True)
            hour_h = hour_perf.copy()
            # We only have overall hour, so show bar + annotation
            fig_hr2 = go.Figure()
            fig_hr2.add_trace(go.Bar(
                x=hour_h["publish_hour"],
                y=hour_h["avg_views"],
                marker=dict(
                    color=hour_h["avg_views"],
                    colorscale="Blues",
                    showscale=False,
                ),
                name="Avg Views",
            ))
            best_h = int(hour_h.loc[hour_h["avg_views"].idxmax(), "publish_hour"])
            fig_hr2.add_vline(
                x=best_h, line_color="#3fb950", line_width=2,
                annotation_text=f"Peak: {best_h}:00",
                annotation_font_color="#3fb950",
                annotation_position="top right",
            )
            fig_hr2.update_layout(
                plot_bgcolor="#161b22", paper_bgcolor="#161b22",
                font=dict(color="#8b949e", size=10), height=340,
                margin=dict(l=8, r=8, t=24, b=8),
                xaxis=dict(title="Hour (24h)", gridcolor="#21262d",
                           linecolor="#30363d", dtick=2),
                yaxis=dict(title="Avg Views", gridcolor="#21262d",
                           linecolor="#30363d"),
                showlegend=False,
            )
            st.plotly_chart(fig_hr2, use_container_width=True)
            st.markdown(f'<div class="chart-caption">Best publish hour: <b>{best_h}:00–{best_h+1}:00</b> UTC — historically highest avg views · Green line = peak window</div></div>', unsafe_allow_html=True)

        r5c3, r5c4 = st.columns(2)

        with r5c3:
            st.markdown('<div class="chart-wrap"><div class="chart-title">🎭 Sentiment by Category</div>', unsafe_allow_html=True)
            fig_sc3 = px.bar(sent_cat_f, x="category", y="count", color="sentiment",
                barmode="stack",
                color_discrete_map={"positive":"#3fb950","neutral":"#8b949e","negative":"#f85149"},
                labels={"count":"Videos","category":"","sentiment":""})
            fig_sc3.update_layout(**dark(280, showlegend=True,
                legend=dict(bgcolor="#161b22",font=dict(size=9),
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
            st.markdown('<div class="chart-caption">Most frequent words in trending titles — signals dominant content themes</div></div>', unsafe_allow_html=True)
"""


# ═══════════════════════════════════════════════════════════════
# SUMMARY OF CHANGES
# ═══════════════════════════════════════════════════════════════
#
# LIVE PAGE — New Predictive Section adds:
#   1. Forecast KPI row: Fastest growing, slowest, total forecast views, categories growing count
#   2. Chart 1: 24h View Forecast overlay bar (current vs predicted) per category
#   3. Chart 2: Forecast Growth % horizontal bar (green=up, red=down)
#   4. Chart 3: Peak Publish Hour heatmap (category × hour)
#   5. Chart 4: Keyword cloud (kept, moved to bottom right)
#
# HISTORICAL PAGE — New Predictive Section adds:
#   1. Chart 1: Engagement-normalised simulated growth % per category
#   2. Chart 2: Best publish hour bar with peak annotation
#   3. Chart 3: Sentiment by category (kept)
#   4. Chart 4: Keyword cloud (kept)
#
# MODEL USED: numpy polyfit (degree 1 = linear regression)
#   — No external ML library needed
#   — Works with even 2 Supabase runs
#   — Degrades gracefully to "0% growth" if only 1 run
# ═══════════════════════════════════════════════════════════════
