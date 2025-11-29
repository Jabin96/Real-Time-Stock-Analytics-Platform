import streamlit as st
import pandas as pd
import glob
import os
import time
import plotly.graph_objects as go
from datetime import datetime

RAW_DATA_DIR = "./outputs/streaming_data/raw_ticks"
ANOMALY_DIR = "./outputs/anomalies"

st.set_page_config(
    page_title="Real-Time Stock Analytics",
    layout="wide",
    initial_sidebar_state="expanded"
)

st.markdown("""
<style>
    /* Global Light Theme */
    .stApp {
        background-color: #f5f7fa;
    }
    
    [data-testid="stAppViewContainer"] {
        background-color: #f5f7fa;
    }
    
    /* Hide Streamlit branding */
    #MainMenu {visibility: hidden;}
    footer {visibility: hidden;}
    
    /* Metric Cards */
    .metric-card {
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        padding: 1.5rem;
        border-radius: 16px;
        color: white;
        box-shadow: 0 4px 20px rgba(102, 126, 234, 0.3);
    }
    
    .metric-value {
        font-size: 2.5rem;
        font-weight: 800;
        margin: 0.5rem 0;
    }
    
    .metric-label {
        font-size: 0.9rem;
        opacity: 0.9;
        text-transform: uppercase;
        letter-spacing: 1px;
    }
    
    .metric-change {
        font-size: 1rem;
        margin-top: 0.5rem;
    }
    
    /* Stock List Item */
    .stock-item {
        background: white;
        padding: 1rem 1.5rem;
        border-radius: 12px;
        margin-bottom: 0.5rem;
        display: flex;
        align-items: center;
        justify-content: space-between;
        box-shadow: 0 2px 8px rgba(0,0,0,0.06);
        transition: all 0.2s;
    }
    
    .stock-item:hover {
        box-shadow: 0 4px 12px rgba(0,0,0,0.1);
        transform: translateY(-2px);
    }
    
    .stock-info {
        display: flex;
        align-items: center;
        gap: 1rem;
    }
    
    .stock-icon {
        width: 40px;
        height: 40px;
        border-radius: 8px;
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        display: flex;
        align-items: center;
        justify-content: center;
        color: white;
        font-weight: 700;
        font-size: 1.1rem;
    }
    
    .stock-name {
        font-weight: 700;
        font-size: 1.1rem;
        color: #1f2937;
    }
    
    .stock-price {
        font-size: 1.3rem;
        font-weight: 700;
        color: #111827;
    }
    
    .change-positive {
        color: #10b981;
        font-weight: 600;
    }
    
    .change-negative {
        color: #ef4444;
        font-weight: 600;
    }
    
    /* Chart Container */
    .chart-container {
        background: white;
        padding: 2rem;
        border-radius: 16px;
        box-shadow: 0 2px 12px rgba(0,0,0,0.06);
    }
    
    .section-title {
        font-size: 1.5rem;
        font-weight: 700;
        color: #1f2937;
        margin-bottom: 1rem;
    }
</style>
""", unsafe_allow_html=True)

@st.cache_data(ttl=1)
def load_spark_data(folder_path):
    """Cached data loader - Optimized to read only recent files."""
    try:
        # Get all CSV files
        all_files = glob.glob(os.path.join(folder_path, "*.csv"))
        
        if not all_files:
            return pd.DataFrame()
            
        # Sort by modification time (newest first)
        all_files.sort(key=os.path.getmtime, reverse=True)
        
        # Filter files by modification time (keep last 30 minutes of data)
        current_time = time.time()
        # 30 minutes buffer to ensure we have enough data for the window
        cutoff_time = current_time - (30 * 60) 
        
        recent_files = [f for f in all_files if os.path.getmtime(f) > cutoff_time]
        
        # If still too many files, cap at 500 to prevent performance issues
        if len(recent_files) > 500:
            recent_files = recent_files[:500]
        
        df_list = []
        for f in recent_files:
            try:
                temp_df = pd.read_csv(f)
                df_list.append(temp_df)
            except Exception as e:
                continue
        
        if df_list:
            full_df = pd.concat(df_list, ignore_index=True)
            
            if 'timestamp' in full_df.columns:
                # Handle potential parsing errors (e.g. malformed strings)
                full_df['timestamp'] = pd.to_datetime(full_df['timestamp'], errors='coerce')
                full_df = full_df.dropna(subset=['timestamp'])
                full_df = full_df.sort_values('timestamp')
                
                # Handle boolean conversion for is_anomaly if it exists
                if 'is_anomaly' in full_df.columns:
                    full_df['is_anomaly'] = full_df['is_anomaly'].astype(str).str.lower() == 'true'
                
                if len(full_df) > 0:
                    start_time = full_df['timestamp'].min()
                    # Use minutes for better granularity
                    full_df['minutes_elapsed'] = (full_df['timestamp'] - start_time).dt.total_seconds() / 60
            return full_df
    except Exception as e:
        # Silently handle errors to prevent UI clutter
        pass
    return pd.DataFrame()

with st.sidebar:
    st.markdown("### The Data Alchemist")
    st.caption("Real-Time Stock Analytics")
    
    st.divider()
    
    # Navigation
    st.markdown("#### Navigation")
    page = st.radio("", ["Dashboard", "Analytics", "Anomalies"], label_visibility="collapsed")
    
    st.divider()
    
    # Settings
    st.markdown("#### Settings")
    time_range = st.select_slider(
        "Time Window",
        options=[1, 3, 5, 10, 15, 30],
        value=3,  # Reduced default to 3 minutes
        format_func=lambda x: f"{x} min"
    )
    
    auto_refresh = st.toggle("Auto-refresh", value=True)  # Enabled by default for real-time updates
    
    if not auto_refresh:
        refresh_button = st.button("Refresh Data", use_container_width=True)

# Main Dashboard Logic
# Main Dashboard Logic
df_raw = load_spark_data(RAW_DATA_DIR)

# Initialize derived dataframes
recent_df = pd.DataFrame()
df_anom = pd.DataFrame()
available_symbols = []

if not df_raw.empty:
    # 1. Derive Anomalies
    if 'is_anomaly' in df_raw.columns:
        df_anom = df_raw[df_raw['is_anomaly'] == True].copy()
    
    # 2. Filter Recent Data
    if 'minutes_elapsed' in df_raw.columns:
        max_minutes = df_raw['minutes_elapsed'].max()
        recent_df = df_raw[df_raw['minutes_elapsed'] >= (max_minutes - time_range)]
    else:
        recent_df = df_raw
        
    # 3. Get Available Symbols
    available_symbols = sorted(recent_df['symbol'].unique())


with st.sidebar:
    st.markdown("### Filter Options")
    
    if available_symbols:
        # Initialize session state for selected stocks
        if 'selected_stocks' not in st.session_state:
            default_stocks = ['EA', 'ETR', 'HOLX', 'IFF', 'K']
            valid_defaults = [s for s in default_stocks if s in available_symbols]
            st.session_state.selected_stocks = valid_defaults if valid_defaults else (available_symbols[:5] if len(available_symbols) >= 5 else available_symbols)
        
        # Validate selection against current data
        valid_selected = [s for s in st.session_state.selected_stocks if s in available_symbols]
        if not valid_selected:
            st.session_state.selected_stocks = available_symbols[:5]
        elif valid_selected:
             st.session_state.selected_stocks = valid_selected
             
        selected_stocks = st.multiselect(
            "Select Stocks to Watch",
            options=available_symbols,
            default=st.session_state.selected_stocks,
            key='selected_stocks'
        )
        st.caption(f"{len(selected_stocks)} stocks selected")
    else:
        selected_stocks = []
        st.warning("No data available for filters")
    
    st.markdown("---")
    st.markdown("---")
    st.markdown("### System Status")
    st.success(f"Consumer Running")
    st.info(f"Last Update: {datetime.now().strftime('%H:%M:%S')}")



if df_raw.empty:
    st.info("Waiting for data stream... (Consumer is processing)")
    if auto_refresh:
        time.sleep(1)
        st.rerun()
    st.stop()

if page == "Dashboard":
    # Deduplicate anomalies for metrics
    if not df_anom.empty:
        if 'z_score' not in df_anom.columns:
            df_anom['z_score'] = 0.0
        df_anom['minute'] = df_anom['timestamp'].dt.floor('T')
        df_anom_dedup = df_anom.drop_duplicates(subset=['symbol', 'minute'], keep='first')
    else:
        df_anom_dedup = pd.DataFrame()

    # Metrics
    col1, col2, col3, col4 = st.columns(4)
    
    total_records = len(df_raw)
    unique_symbols = len(df_raw['symbol'].unique())
    avg_price = recent_df['price'].mean() if not recent_df.empty else 0
    anomaly_count = len(df_anom_dedup)
    
    with col1:
        st.markdown(f"""<div class="metric-card"><div class="metric-label">Total Records</div><div class="metric-value">{total_records:,}</div></div>""", unsafe_allow_html=True)
    with col2:
        st.markdown(f"""<div class="metric-card"><div class="metric-label">Active Stocks</div><div class="metric-value">{unique_symbols}</div></div>""", unsafe_allow_html=True)
    with col3:
        st.markdown(f"""<div class="metric-card"><div class="metric-label">Avg Price</div><div class="metric-value">${avg_price:.2f}</div></div>""", unsafe_allow_html=True)
    with col4:
        st.markdown(f"""<div class="metric-card"><div class="metric-label">Anomalies</div><div class="metric-value">{anomaly_count}</div></div>""", unsafe_allow_html=True)
    
    # Anomaly Alert
    anomaly_alert_container = st.empty()
    if not df_anom_dedup.empty:
        latest_anom = df_anom_dedup.sort_values('timestamp', ascending=False).iloc[0]
        try:
            current_time = pd.Timestamp.now()
            anom_time = pd.to_datetime(latest_anom['timestamp']).replace(tzinfo=None)
            time_diff = (current_time - anom_time).total_seconds()
        except:
            time_diff = 0
        
        if time_diff < 300: # 5 mins
            with anomaly_alert_container.container():
                st.markdown(f"""
                <div style="background-color: #fee2e2; border-left: 5px solid #ef4444; padding: 1rem; border-radius: 8px; color: #7f1d1d; margin-bottom: 1rem; display: flex; align-items: center; gap: 10px;">
                    <span style="font-size: 1.5rem;">!</span>
                    <div>
                        <div style="font-weight: 700; font-size: 1.1rem;">Anomaly Detected: {latest_anom['symbol']}</div>
                        <div style="font-size: 0.9rem;">Price: <b>${latest_anom['price']:.2f}</b> | Time: {latest_anom['timestamp'].strftime('%H:%M:%S')}</div>
                    </div>
                </div>
                """, unsafe_allow_html=True)

    st.markdown("<br>", unsafe_allow_html=True)
    
    # Charts
    col_left, col_right = st.columns([1, 2])
    
    with col_left:
        st.markdown('<div class="section-title">Watchlist</div>', unsafe_allow_html=True)
        if selected_stocks:
            watchlist_df = recent_df[recent_df['symbol'].isin(selected_stocks)]
            if not watchlist_df.empty:
                stock_data = watchlist_df.groupby('symbol').agg({'price': ['last', 'first']}).reset_index()
                stock_data.columns = ['symbol', 'latest_price', 'first_price']
                stock_data['pct_change'] = ((stock_data['latest_price'] - stock_data['first_price']) / stock_data['first_price'] * 100).fillna(0)
                
                for _, row in stock_data.iterrows():
                    symbol = row['symbol']
                    price = row['latest_price']
                    pct = row['pct_change']
                    change_class = "change-positive" if pct >= 0 else "change-negative"
                    arrow = "^" if pct >= 0 else "v"
                    st.markdown(f"""
                    <div class="stock-item">
                        <div class="stock-info"><div class="stock-icon">{symbol[:2]}</div><div class="stock-name">{symbol}</div></div>
                        <div style="text-align: right;"><div class="stock-price">${price:.2f}</div><div class="{change_class}">{arrow} {abs(pct):.2f}%</div></div>
                    </div>
                    """, unsafe_allow_html=True)
            else:
                st.info("No data for selected stocks")
        else:
            st.info("Select stocks from sidebar")

    with col_right:
        st.markdown('<div class="section-title">Price Trends</div>', unsafe_allow_html=True)
        fig = go.Figure()
        if selected_stocks:
            for symbol in selected_stocks[:5]:
                stock_chart_data = recent_df[recent_df['symbol'] == symbol]
                if len(stock_chart_data) >= 2:
                    fig.add_trace(go.Scatter(
                        x=stock_chart_data['minutes_elapsed'], y=stock_chart_data['price'],
                        mode='lines+markers', name=symbol,
                        line=dict(width=3), marker=dict(size=6)
                    ))
        
        fig.update_layout(height=400, template='plotly_white', paper_bgcolor='rgba(0,0,0,1)', plot_bgcolor='rgba(0,0,0,1)', font=dict(color='white'))
        st.plotly_chart(fig, use_container_width=True, config={'displayModeBar': False})

elif page == "Analytics":
    st.markdown('<div class="section-title">Detailed Analytics</div>', unsafe_allow_html=True)
    if not recent_df.empty:
        stats_df = recent_df.groupby('symbol')['price'].agg([
            ('Latest', 'last'), ('Min', 'min'), ('Max', 'max'), 
            ('Average', 'mean'), ('Std Dev', 'std'), ('Count', 'count')
        ]).round(2).sort_values('Count', ascending=False).head(15)
        st.dataframe(stats_df, use_container_width=True, height=600)
    else:
        st.info("No data available for analytics")

elif page == "Anomalies":
    st.markdown('<div class="section-title">Anomaly Detection</div>', unsafe_allow_html=True)
    if not df_anom.empty:
        if 'z_score' not in df_anom.columns: df_anom['z_score'] = 0.0
        df_anom['minute'] = df_anom['timestamp'].dt.floor('T')
        unique_anomalies = df_anom.drop_duplicates(subset=['symbol', 'minute'], keep='first')
        recent_anomalies = unique_anomalies.sort_values('timestamp', ascending=False).head(20)
        
        for _, anom in recent_anomalies.iterrows():
            st.markdown(f"""
            <div style="background-color: white; padding: 1rem; border-radius: 8px; margin-bottom: 0.8rem; border-left: 4px solid #ef4444; display: flex; justify-content: space-between; align-items: center; color: #1f2937;">
                <div style="display: flex; gap: 2rem; align-items: center;">
                    <div style="font-weight: 700; font-size: 1.1rem; width: 60px;">{anom['symbol']}</div>
                    <div style="font-family: monospace; font-size: 1.1rem;">${anom['price']:.2f}</div>
                </div>
                <div style="display: flex; gap: 2rem; align-items: center;">
                    <div style="background: #fee2e2; color: #991b1b; padding: 0.2rem 0.6rem; border-radius: 4px; font-size: 0.9rem; font-weight: 600;">Z-Score: {anom['z_score']:.2f}</div>
                    <div style="color: #6b7280; font-size: 0.9rem;">{anom['timestamp'].strftime('%Y-%m-%d %H:%M:%S')}</div>
                </div>
            </div>
            """, unsafe_allow_html=True)
    else:
        st.success("No anomalies detected - System operating normally")

# Manual Auto-Refresh Logic
if auto_refresh:
    time.sleep(1)
    st.rerun()
