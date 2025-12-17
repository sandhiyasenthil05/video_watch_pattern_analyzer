🎥 Streaming Video Watch Pattern Analysis

A comprehensive PySpark- and Pandas-based analytics system that processes video viewing logs to uncover viewer engagement patterns, including watch duration, skip frequency, and replay behavior. The system provides interactive visualizations and a Streamlit dashboard to help content creators optimize videos and improve user engagement.

🎯 Project Objective

Streaming platforms collect large amounts of viewer interaction data, but analyzing it manually is cumbersome. This project aims to:

Analyze video watch patterns, skips, and replays.

Identify top-performing content and engagement trends.

Provide actionable insights to optimize video content and user retention.

✨ Features
Core Functionality

✅ PySpark Data Processing – Scalable processing for large logs.
✅ Data Cleaning – Removes duplicates, null values, and inconsistent entries.
✅ Feature Engineering – Computes watch segments, skip segments, and engagement scores.
✅ Trend Analysis – Daily and user-level trends for watch and skip behavior.
✅ Export Options – Save processed results as CSV/JSON for further analysis.

Visualizations

📊 Heatmaps – Highlight most-watched segments, skip zones, and replay hotspots.
📈 Line Charts – Display watch time, skip rate, and engagement trends over time.
📉 Bar Charts – Top videos by watch duration, skip frequency, and replay count.
📊 Histograms & Scatterplots – Distribution of watch times, skip rates, and engagement.
🌐 Interactive Dashboard – Streamlit interface for real-time exploration.

Web Interface

🚀 Streamlit Dashboard – Multi-page analytics with dynamic filtering.
🔍 User Lookup – Explore individual user engagement.
📊 Video Analytics – Compare video performance metrics.
💾 Data Export – Download filtered insights as CSV.

📁 Project Structure
VideoWatchPatternAnalysis/
│
├── data/                     
│   └── video_logs.csv        # Sample or uploaded dataset
│
├── visualizations/           
│   ├── heatmap_watch.png     
│   ├── heatmap_skip.png      
│   ├── heatmap_replay.png    
│   ├── line_trends.png       
│   └── bar_video_stats.png   
│
├── generate_sample_data.py   # Generates sample viewer logs
├── analytics_engine.py       # PySpark/Pandas processing & feature engineering
├── visualize_results.py      # Generates static and interactive charts
├── app.py                    # Streamlit web dashboard
├── requirements.txt          # Python dependencies
└── README.md                 # This file

🛠️ Technical Stack
Category	Technology
Big Data Processing	PySpark 3.5.0
Data Manipulation	Pandas 2.1.4, NumPy 1.26.2
Static Visualization	Matplotlib 3.8.2, Seaborn 0.13.0
Interactive Visualization	Plotly 5.18.0
Web Dashboard	Streamlit 1.29.0
Data Storage	CSV, JSON
📊 Dataset Schema

Input Columns

Column	Type	Description
UserID	Integer	Unique viewer identifier
VideoID	String	Unique video identifier
WatchTime_sec	Integer	Watch duration in seconds
SkipRate_pct	Integer	Percentage of video skipped
Date	String	Viewing date

Processed Metrics

Metric	Description
WatchSegment	Segment of video watched (binned)
SkipSegment	Segment skip category (binned)
EngagementScore	Custom engagement metric
ReplayScore	Ratio of watch time to average video duration
🚀 Quick Start
1️⃣ Prerequisites

Python 3.8+

Java 8 or 11 (for PySpark)

2️⃣ Installation
cd VideoWatchPatternAnalysis
pip install -r requirements.txt

3️⃣ Running the Project

Step 1: Generate Sample Data

python generate_sample_data.py


Step 2: Run Analytics Engine

python analytics_engine.py


Step 3: Generate Visualizations

python visualize_results.py


Step 4: Launch Streamlit Dashboard

streamlit run app.py


Opens at http://localhost:8501

Supports user-level exploration, trends, heatmaps, and data export.

🧠 Insights & Analysis

Watch Patterns – Identify which parts of videos users watch most.

Skip Zones – Detect sections where viewers tend to drop off.

Replay Hotspots – Segments frequently replayed, indicating high interest.

Engagement Trends – Track watch, skip, and engagement scores over time.

Top Videos & Users – Discover highest performing videos and most engaged viewers.
The Streamlit dashboard offers an immersive, real-time exploration of video watch patterns. Key interactive features include:

1️⃣ User Lookup

Search by UserID to view individual engagement metrics.

Inspect watch duration, skip rates, and replay hotspots for a single viewer.

Compare engagement across videos or dates.

2️⃣ Video Analytics

Select any VideoID to see:

Average watch time per segment.

Skip intensity heatmaps.

Replay frequency charts.

Filter by date ranges for dynamic trend analysis.

3️⃣ Trend Exploration

Line charts for watch time, skip rate, and engagement score over time.

Interactive hover info to pinpoint peaks and dips in engagement.

Zoom in/out and pan over date ranges.

4️⃣ Engagement Heatmaps

Hover over heatmap cells to see exact values for watch, skip, or replay.

Quickly identify “hot zones” where viewers engage most or drop off.

5️⃣ Advanced Filtering & Comparison

Multi-dimensional filtering by UserID, VideoID, Date, WatchSegment, SkipSegment.

Compare multiple users or videos side-by-side.

Sort metrics dynamically for quick insights.

6️⃣ Download & Share Insights

Export filtered data to CSV for further reporting or presentations.

Share dashboards internally or embed HTML visualizations in reports.

7️⃣ Real-time Analytics Feedback

Toggle PySpark mode to analyze large datasets efficiently.

Get instant updates on metrics and visualizations without reloading pages.

Includes Spark Web UI integration for monitoring jobs.

🎯 Interactive Use Cases

Content Optimization: Identify sections where users skip videos frequently.

Viewer Engagement: Highlight top-performing users or videos with high replay rates.

Trend Analysis: Track engagement over time to spot seasonal or time-of-day patterns.

Personalized Recommendations: Suggest edits or highlights for videos based on viewer behavior.

✅ Learning Outcomes

PySpark DataFrame operations, transformations, and caching

Pandas for quick processing and fallbacks

Advanced feature engineering and metric computation

Static and interactive data visualization

Streamlit dashboards for dynamic data exploration

🚀 Future Enhancements

Real-time streaming analysis using Spark Streaming

Machine learning for predicting engagement and retention

Database integration (PostgreSQL/MongoDB) for persistent logs

REST API to provide recommendations for video content

A/B testing for video optimization

📄 License

This project is for educational purposes. Free to use and modify.

👨‍💻 Author

Created as a demonstration of PySpark, Pandas, and interactive video analytics.

🙏 Acknowledgments

Apache Spark community for scalable big data solutions

Pandas and NumPy teams for data manipulation

Streamlit for seamless interactive dashboards

Open-source contributors for visualization libraries

📞 Support

Refer to troubleshooting sections in the code

Inspect sample data to understand processing pipelines

✅ Conclusion
The Streaming Video Watch Pattern Analysis project transforms raw viewing logs into actionable insights. With heatmaps, trend charts, and dashboards, content creators can understand engagement, optimize content, and boost viewer retention.
