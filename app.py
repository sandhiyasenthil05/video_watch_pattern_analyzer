import os
import tempfile
import numpy as np
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
import streamlit as st
from pyspark.sql import SparkSession, functions as F


st.set_page_config(page_title="Video Watch Pattern Analysis", layout="wide")


# ---------- Spark helpers ----------
@st.cache_resource
def get_spark():
    spark = (
        SparkSession.builder.appName("Video Watch Pattern Analysis")
        .master("local[*]")
        .config("spark.ui.showConsoleProgress", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


def to_spark(df: pd.DataFrame):
    spark = get_spark()
    return spark.createDataFrame(df)


@st.cache_data
def generate_sample_data(rows: int = 200, seed: int = 42) -> pd.DataFrame:
    rng = np.random.default_rng(seed)
    user_ids = rng.integers(100, 140, size=rows)
    video_ids = rng.choice([f"V{v:04d}" for v in range(1001, 1011)], size=rows)
    watch_time = rng.integers(180, 900, size=rows)  # seconds
    skip_rate = rng.integers(5, 35, size=rows)      # percentage
    dates = pd.date_range("2025-02-01", periods=rows, freq="D")
    data = pd.DataFrame(
        {
            "UserID": user_ids,
            "VideoID": video_ids,
            "WatchTime_sec": watch_time,
            "SkipRate_pct": skip_rate,
            "Date": rng.choice(dates.strftime("%Y-%m-%d"), size=rows),
        }
    )
    return data


def decorate_ui():
    st.markdown(
        """
        <style>
            :root { --primary:#0b8bd9; --accent:#ffb347; --card:#ffffff; }
            .block-container { padding-top:1rem; }
            .metric-card {
                background: var(--card);
                padding: 1rem 1.25rem;
                border-radius: 14px;
                box-shadow: 0 4px 18px rgba(0,0,0,0.06);
                border: 1px solid rgba(0,0,0,0.04);
            }
            .section-title {
                font-size: 1.05rem;
                font-weight: 700;
                color: #0f172a;
                margin-bottom: 0.4rem;
            }
            .subtle {
                color: #475569;
                font-size: 0.9rem;
                margin-bottom: 0.6rem;
            }
            .spark-btn {
                display:block;
                text-align:center;
                background: #facc15;
                color:#0f172a !important;
                padding: 0.65rem 0.8rem;
                border-radius: 12px;
                font-weight: 700;
                text-decoration:none;
                border: 1px solid #eab308;
                box-shadow: 0 3px 10px rgba(250,204,21,0.35);
            }
            .data-preview {
                background: var(--card);
                padding: 0.75rem 1rem;
                border-radius: 12px;
                box-shadow: inset 0 0 0 1px rgba(15,23,42,0.04);
            }
        </style>
        """,
        unsafe_allow_html=True,
    )


def build_heatmap(pdf: pd.DataFrame, x: str, y: str, value: str, title: str):
    if pdf.empty:
        st.info(f"No data available for {title}.")
        return
    fig, ax = plt.subplots(figsize=(8, 4))
    pivot = pdf.pivot_table(index=y, columns=x, values=value, aggfunc="mean").fillna(0)
    sns.heatmap(pivot, cmap="Blues", annot=True, fmt=".1f", linewidths=0.4, ax=ax, cbar_kws={"label": value})
    ax.set_title(title, fontsize=12, fontweight="bold")
    ax.set_xlabel(x)
    ax.set_ylabel(y)
    st.pyplot(fig)


def render_line_chart(pdf: pd.DataFrame, x: str, y: str, title: str):
    if pdf.empty:
        st.info(f"No data available for {title}.")
        return
    fig, ax = plt.subplots(figsize=(7, 3))
    sns.lineplot(data=pdf, x=x, y=y, marker="o", ax=ax, color="#0b8bd9")
    ax.set_title(title, fontsize=11, fontweight="bold")
    ax.set_xlabel(x)
    ax.set_ylabel(y)
    st.pyplot(fig)


def render_bar(pdf: pd.DataFrame, x: str, y: str, title: str, color: str = "#0b8bd9"):
    if pdf.empty:
        st.info(f"No data available for {title}.")
        return
    fig, ax = plt.subplots(figsize=(7, 3))
    sns.barplot(data=pdf, x=x, y=y, ax=ax, color=color, edgecolor="#0f172a", linewidth=0.6)
    ax.set_title(title, fontsize=11, fontweight="bold")
    ax.set_xlabel(x)
    ax.set_ylabel(y)
    plt.xticks(rotation=20)
    st.pyplot(fig)


def render_hist(pdf: pd.DataFrame, column: str, bins: int, title: str, color: str = "#0b8bd9"):
    if pdf.empty:
        st.info(f"No data available for {title}.")
        return
    fig, ax = plt.subplots(figsize=(7, 3))
    sns.histplot(pdf[column], bins=bins, kde=True, color=color, ax=ax)
    ax.set_title(title, fontsize=11, fontweight="bold")
    ax.set_xlabel(column)
    ax.set_ylabel("Count")
    st.pyplot(fig)


def render_scatter(pdf: pd.DataFrame, x: str, y: str, hue: str, title: str):
    if pdf.empty:
        st.info(f"No data available for {title}.")
        return
    fig, ax = plt.subplots(figsize=(7, 3))
    sns.scatterplot(data=pdf, x=x, y=y, hue=hue, palette="viridis", ax=ax, s=50, edgecolor="white")
    ax.set_title(title, fontsize=11, fontweight="bold")
    ax.set_xlabel(x)
    ax.set_ylabel(y)
    ax.legend(title=hue, bbox_to_anchor=(1.02, 1), loc="upper left")
    st.pyplot(fig)


def main():
    decorate_ui()
    st.title("🔵 Video Watch Pattern Analysis")
    st.write("🔥📊 Understand how users engage, skip, and replay your videos in one place.")

    required_cols = {"UserID", "VideoID", "WatchTime_sec", "SkipRate_pct", "Date"}

    with st.sidebar:
        st.header("🟣 Controls")
        uploaded = st.file_uploader("Upload CSV dataset", type=["csv"])
        use_spark = st.checkbox("Use Spark (requires Java/Spark)", value=False)
        spark_link = st.text_input(
            "Spark Web UI link",
            value="",
            placeholder="Paste Spark UI link (optional)",
            help="Leave empty to hide the button.",
        )
        if spark_link.strip():
            st.markdown(f'<a class="spark-btn" href="{spark_link}" target="_blank" rel="noopener noreferrer">🚀 Open Spark Web UI</a>', unsafe_allow_html=True)

        # Load data with validation + safe fallback to generated sample
        if uploaded:
            pdf = pd.read_csv(uploaded)
            missing = required_cols.difference(set(pdf.columns))
            if missing:
                st.warning(
                    f"Uploaded file is missing columns: {', '.join(sorted(missing))}. "
                    f"Falling back to generated sample dataset.",
                    icon="⚠️",
                )
                pdf = generate_sample_data()
        else:
            pdf = generate_sample_data()

        # Final validation; stop only if even the fallback is wrong
        missing = required_cols.difference(set(pdf.columns))
        if missing:
            st.error(
                "Dataset invalid. Ensure these columns exist exactly: "
                + ", ".join(sorted(required_cols))
            )
            st.stop()

        user_options = ["All Users"] + sorted(pdf["UserID"].astype(str).unique().tolist())
        selected_user = st.selectbox("🎥 Choose UserID", options=user_options, index=0)

    spark_available = False
    spark_df = None
    if use_spark:
        try:
            spark_df = to_spark(pdf)
            spark_available = True
        except Exception:
            spark_available = False

    def compute_with_pandas(base_df: pd.DataFrame):
        df = base_df.copy()
        df["WatchSegment"] = ((df["WatchTime_sec"] / 60) % 10).astype(int)
        df["SkipSegment"] = (df["SkipRate_pct"] / 10).astype(int)
        df["EngagementScore"] = df["WatchTime_sec"] - df["SkipRate_pct"] * 2
        video_avg = df.groupby("VideoID")["WatchTime_sec"].mean().rename("avg_watch")
        df = df.merge(video_avg, on="VideoID", how="left")
        df["ReplayScore"] = df["WatchTime_sec"] / df["avg_watch"]
        if selected_user != "All Users":
            df = df[df["UserID"] == int(selected_user)]
        preview = df.head(5)
        stats = (
            df.groupby("VideoID")["WatchTime_sec"]
            .agg(avg_watch="mean", min_watch="min", max_watch="max")
            .reset_index()
            .sort_values("avg_watch", ascending=False)
            .head(5)
        )
        user_stats = (
            df.groupby("UserID")
            .agg(avg_engagement=("EngagementScore", "mean"), avg_skip=("SkipRate_pct", "mean"))
            .reset_index()
            .sort_values("avg_engagement", ascending=False)
            .head(5)
        )
        watch_heat = (
            df.groupby(["VideoID", "WatchSegment"])["WatchTime_sec"].mean().reset_index(name="AvgWatch")
        )
        skip_heat = (
            df.groupby(["VideoID", "SkipSegment"])["SkipRate_pct"].mean().reset_index(name="AvgSkip")
        )
        replay_heat = (
            df.groupby(["VideoID", "WatchSegment"])["ReplayScore"].mean().reset_index(name="ReplayScore")
        )
        trends = (
            df.groupby("Date")
            .agg(avg_watch=("WatchTime_sec", "mean"), avg_skip=("SkipRate_pct", "mean"), avg_engagement=("EngagementScore", "mean"))
            .reset_index()
            .sort_values("Date")
        )
        video_stats = (
            df.groupby("VideoID")
            .agg(
                avg_watch=("WatchTime_sec", "mean"),
                avg_skip=("SkipRate_pct", "mean"),
                replay_freq=("ReplayScore", "mean"),
            )
            .reset_index()
        )
        return preview, stats, user_stats, watch_heat, skip_heat, replay_heat, trends, video_stats

    if spark_available:
        try:
            # Feature engineering for segments and replay score
            enriched = (
                spark_df.withColumn("WatchSegment", F.floor((F.col("WatchTime_sec") / 60) % 10).cast("int"))
                .withColumn("SkipSegment", F.floor(F.col("SkipRate_pct") / 10).cast("int"))
                .withColumn("EngagementScore", F.col("WatchTime_sec") - F.col("SkipRate_pct") * 2)
            )

            # Add replay intensity as ratio to video average
            video_avg = enriched.groupBy("VideoID").agg(F.avg("WatchTime_sec").alias("avg_watch"))
            enriched = (
                enriched.join(video_avg, on="VideoID", how="left")
                .withColumn("ReplayScore", F.when(F.col("avg_watch") > 0, F.col("WatchTime_sec") / F.col("avg_watch")).otherwise(0.0))
            )

            if selected_user != "All Users":
                enriched = enriched.filter(F.col("UserID") == F.lit(int(selected_user)))

            preview_pdf = enriched.limit(5).toPandas()
            stats = (
                enriched.groupBy("VideoID")
                .agg(
                    F.avg("WatchTime_sec").alias("avg_watch"),
                    F.min("WatchTime_sec").alias("min_watch"),
                    F.max("WatchTime_sec").alias("max_watch"),
                )
                .orderBy(F.desc("avg_watch"))
                .limit(5)
                .toPandas()
            )
            user_stats = (
                enriched.groupBy("UserID")
                .agg(F.avg("EngagementScore").alias("avg_engagement"), F.avg("SkipRate_pct").alias("avg_skip"))
                .orderBy(F.desc("avg_engagement"))
                .limit(5)
                .toPandas()
            )
            watch_heat = (
                enriched.groupBy("VideoID", "WatchSegment")
                .agg(F.avg("WatchTime_sec").alias("AvgWatch"))
                .toPandas()
            )
            skip_heat = (
                enriched.groupBy("VideoID", "SkipSegment")
                .agg(F.avg("SkipRate_pct").alias("AvgSkip"))
                .toPandas()
            )
            replay_heat = (
                enriched.groupBy("VideoID", "WatchSegment")
                .agg(F.avg("ReplayScore").alias("ReplayScore"))
                .toPandas()
            )
            trends = enriched.groupBy("Date").agg(
                F.avg("WatchTime_sec").alias("avg_watch"),
                F.avg("SkipRate_pct").alias("avg_skip"),
                F.avg("EngagementScore").alias("avg_engagement"),
            ).orderBy("Date").toPandas()
            video_stats = (
                enriched.groupBy("VideoID")
                .agg(
                    F.avg("WatchTime_sec").alias("avg_watch"),
                    F.avg("SkipRate_pct").alias("avg_skip"),
                    F.avg("ReplayScore").alias("replay_freq"),
                )
                .toPandas()
            )
        except Exception:
            spark_available = False
            preview_pdf, stats, user_stats, watch_heat, skip_heat, replay_heat, trends, video_stats = compute_with_pandas(pdf)
    if not spark_available:
        preview_pdf, stats, user_stats, watch_heat, skip_heat, replay_heat, trends, video_stats = compute_with_pandas(pdf)

    # Layout
    if preview_pdf.empty:
        st.warning("No rows after filtering. Please upload data or pick another user.", icon="⚠️")
        st.stop()

    top_cols = st.columns([1, 1, 1.2])
    with top_cols[0]:
        st.markdown('<div class="metric-card">', unsafe_allow_html=True)
        st.markdown('<div class="section-title">📈 First 5 records</div>', unsafe_allow_html=True)
        st.markdown('<div class="data-preview">', unsafe_allow_html=True)
        st.dataframe(preview_pdf, use_container_width=True, hide_index=True)
        st.markdown("</div></div>", unsafe_allow_html=True)

    with top_cols[1]:
        st.markdown('<div class="metric-card">', unsafe_allow_html=True)
        st.markdown('<div class="section-title">🎥 Top Videos by Watch</div>', unsafe_allow_html=True)
        st.dataframe(stats, use_container_width=True, hide_index=True)
        st.markdown("</div>", unsafe_allow_html=True)

    with top_cols[2]:
        st.markdown('<div class="metric-card">', unsafe_allow_html=True)
        st.markdown('<div class="section-title">🙋 User Engagement Leaders</div>', unsafe_allow_html=True)
        st.dataframe(user_stats, use_container_width=True, hide_index=True)
        st.markdown("</div>", unsafe_allow_html=True)

    st.markdown("### 🔥 Engagement Heatmaps")
    heat_cols = st.columns(3)

    with heat_cols[0]:
        build_heatmap(watch_heat, x="VideoID", y="WatchSegment", value="AvgWatch", title="Most-watched segments")
    with heat_cols[1]:
        build_heatmap(skip_heat, x="VideoID", y="SkipSegment", value="AvgSkip", title="Skip hot zones")
    with heat_cols[2]:
        build_heatmap(replay_heat, x="VideoID", y="WatchSegment", value="ReplayScore", title="Replay hotspots")

    st.markdown("### 📈 User-level Trends")
    trend_cols = st.columns(3)
    with trend_cols[0]:
        render_line_chart(trends, x="Date", y="avg_watch", title="WatchTime trend")
    with trend_cols[1]:
        render_line_chart(trends, x="Date", y="avg_skip", title="SkipRate trend")
    with trend_cols[2]:
        render_line_chart(trends, x="Date", y="avg_engagement", title="Engagement score trend")

    st.markdown("### 🎥 Video-level Analytics")
    vid_cols = st.columns(3)
    with vid_cols[0]:
        render_bar(video_stats, x="VideoID", y="avg_watch", title="Avg watch time")
    with vid_cols[1]:
        render_bar(video_stats, x="VideoID", y="avg_skip", title="Skip distribution", color="#ef4444")
    with vid_cols[2]:
        render_bar(video_stats, x="VideoID", y="replay_freq", title="Replay frequency", color="#22c55e")

    st.markdown("### 📊 Additional Visualizations")
    extra_cols = st.columns(3)
    with extra_cols[0]:
        render_hist(preview_pdf, column="WatchTime_sec", bins=15, title="Watch time distribution", color="#0b8bd9")
    with extra_cols[1]:
        render_hist(preview_pdf, column="SkipRate_pct", bins=10, title="Skip rate distribution", color="#ef4444")
    with extra_cols[2]:
        render_scatter(preview_pdf, x="WatchTime_sec", y="SkipRate_pct", hue="VideoID", title="Watch vs Skip by Video")

    if spark_available:
        st.success("Analytics generated successfully with PySpark ✅")
    else:
        st.info("Analytics generated with pandas fallback (Spark unavailable).")


if __name__ == "__main__":
    main()

