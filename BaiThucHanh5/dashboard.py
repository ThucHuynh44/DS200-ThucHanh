import os
import time
import pandas as pd
import streamlit as st
from pymongo import MongoClient
from datetime import datetime

# Page configuration
st.set_page_config(
    page_title="Real-Time Camera Person Counter",
    page_icon="👤",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom Styling (CSS) for premium look
st.markdown("""
    <style>
        .main {
            background-color: #0e1117;
            color: #ffffff;
        }
        .stMetric {
            background-color: #1f2937;
            padding: 15px;
            border-radius: 10px;
            box-shadow: 0 4px 6px rgba(0, 0, 0, 0.1);
            border: 1px solid #374151;
        }
        div[data-testid="metric-container"] {
            color: #ffffff;
        }
        .header-title {
            font-family: 'Outfit', 'Inter', sans-serif;
            font-weight: 700;
            color: #10b981;
            margin-bottom: 2rem;
            text-align: center;
        }
    </style>
""", unsafe_allow_html=True)

st.markdown("<h1 class='header-title'>👤 Real-time Person Counting & Analytics Dashboard</h1>", unsafe_allow_html=True)

# Sidebar settings
st.sidebar.title("⚙️ System Configuration")
mongo_uri = st.sidebar.text_input("MongoDB URI", "mongodb://localhost:27017/")
db_name = st.sidebar.text_input("Database Name", "camera_analytics")
coll_name = st.sidebar.text_input("Collection Name", "detections")
refresh_interval = st.sidebar.slider("Refresh Interval (seconds)", 0.2, 5.0, 1.0)
frame_path = "latest_processed.jpg"

# Database Connection Helper
@st.cache_resource
def get_mongo_client(uri):
    try:
        client = MongoClient(uri, serverSelectionTimeoutMS=2000)
        client.server_info() # trigger connection check
        return client
    except Exception as e:
        st.sidebar.error(f"Failed to connect to MongoDB: {e}")
        return None

client = get_mongo_client(mongo_uri)

# Layout: Main panel split into two columns
col_video, col_stats = st.columns([3, 2])

with col_video:
    st.subheader("📷 Live Camera Stream (Annotated)")
    video_placeholder = st.empty()

with col_stats:
    st.subheader("📊 Live Metrics")
    metric_count = st.empty()
    metric_latency = st.empty()
    metric_total = st.empty()
    
    st.subheader("📈 Trend Analysis")
    chart_placeholder = st.empty()

st.subheader("📋 Latest Database Log Records (MongoDB)")
table_placeholder = st.empty()

# Dashboard Loop for continuous updating
while True:
    try:
        # 1. Update Video Frame
        if os.path.exists(frame_path):
            # Read and display the latest frame processed by YOLO
            # We use use_container_width=True to fit the column width
            # We add a small timestamp parameter to force cache busting
            video_placeholder.image(frame_path, use_column_width=True)
        else:
            video_placeholder.info("Waiting for processing server to output frames...")
            
        # 2. Fetch data from MongoDB
        if client is not None:
            db = client[db_name]
            coll = db[coll_name]
            
            # Fetch last 50 records for plotting
            cursor = coll.find().sort("timestamp", -1).limit(50)
            records = list(cursor)
            
            if len(records) > 0:
                df = pd.DataFrame(records)
                # Sort chronological
                df = df.iloc[::-1].reset_index(drop=True)
                
                # Extract latest record details
                latest_record = records[0]
                current_count = latest_record.get("person_count", 0)
                proc_latency = latest_record.get("process_latency_seconds", 0) * 1000 # ms
                total_records = coll.count_documents({})
                
                # Format datetime for display
                latest_time_str = datetime.fromtimestamp(latest_record.get("timestamp", time.time())).strftime("%H:%M:%S")
                
                # Write live metrics
                with col_stats:
                    metric_count.metric(label="Current Person Count", value=f"{current_count} people", delta=f"Latest frame at {latest_time_str}")
                    metric_latency.metric(label="YOLO Processing Latency", value=f"{proc_latency:.1f} ms")
                    metric_total.metric(label="Total Logged Frames", value=f"{total_records} records")
                
                # Write trend chart
                # Format timestamp as time string for clean x-axis
                df['Time'] = df['timestamp'].apply(lambda x: datetime.fromtimestamp(x).strftime("%H:%M:%S"))
                chart_df = df.set_index('Time')[['person_count']]
                chart_placeholder.line_chart(chart_df, color="#10b981")
                
                # Display database records (clean table)
                display_cols = ["frame_id", "timestamp", "person_count", "process_latency_seconds"]
                table_df = df[display_cols].copy()
                table_df["timestamp"] = table_df["timestamp"].apply(lambda x: datetime.fromtimestamp(x).strftime("%Y-%m-%d %H:%M:%S"))
                table_df.columns = ["Frame ID", "Timestamp", "Person Count", "Latency (s)"]
                table_placeholder.dataframe(table_df.iloc[::-1], use_container_width=True) # reverse back to see newest at top
            else:
                metric_count.info("No records found in database yet.")
        else:
            st.error("MongoDB Client not connected. Check server status.")
            
    except Exception as e:
        st.warning(f"Error updating dashboard: {e}")
        
    time.sleep(refresh_interval)
