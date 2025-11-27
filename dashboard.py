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
    page_title="The Data Alchemists",
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

@st.cache_data(ttl=3)
def load_spark_data(folder_path):
    """Cached data loader."""
    try:
        all_files = glob.glob(os.path.join(folder_path, "*.csv"))
        if not all_files:
            return pd.DataFrame()
        
        df_list = []
        for f in all_files:
            try:
                temp_df = pd.read_csv(f)
                df_list.append(temp_df)
            except:
                continue
        
        if df_list:
            full_df = pd.concat(df_list, ignore_index=True)
            if 'timestamp' in full_df.columns:
                full_df['timestamp'] = pd.to_datetime(full_df['timestamp'])
                full_df = full_df.sort_values('timestamp')
                if len(full_df) > 0:
                    start_time = full_df['timestamp'].min()
                    # Use minutes for better granularity
                    full_df['minutes_elapsed'] = (full_df['timestamp'] - start_time).dt.total_seconds() / 60
            return full_df
    except:
        pass
    return pd.DataFrame()

with st.sidebar:
    st.markdown("### The Data Alchemists")
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
    
    auto_refresh = st.toggle("Auto-refresh", value=False)  # Disabled by default to prevent flickering
    
    if not auto_refresh:
        refresh_button = st.button("Refresh Data", use_container_width=True)

df_raw = load_spark_data(RAW_DATA_DIR)
df_anom = load_spark_data(ANOMALY_DIR)

if df_raw.empty:
    st.info("Waiting for data stream...")
    st.stop()

# Filter recent data
if 'minutes_elapsed' in df_raw.columns:
    max_minutes = df_raw['minutes_elapsed'].max()
    recent_df = df_raw[df_raw['minutes_elapsed'] >= (max_minutes - time_range)]
else:
    recent_df = df_raw

# Get available symbols for selection
available_symbols = sorted(recent_df['symbol'].unique())

# Sidebar Stock Selector
with st.sidebar:
    st.divider()
    st.markdown("#### Watchlist Stocks")
    
    # Initialize session state for selected stocks
    if 'selected_stocks' not in st.session_state:
        default_stocks = ['EA', 'ETR', 'HOLX', 'IFF', 'K']
        # Only use defaults that are actually available in the data
        valid_defaults = [s for s in default_stocks if s in available_symbols]
        st.session_state.selected_stocks = valid_defaults if valid_defaults else (available_symbols[:5] if len(available_symbols) >= 5 else available_symbols)
    
    # Filter session state to only include stocks that exist in current data
    valid_selected = [s for s in st.session_state.selected_stocks if s in available_symbols]
    if not valid_selected and available_symbols:
        # Fallback if selection becomes invalid
        default_stocks = ['EA', 'ETR', 'HOLX', 'IFF', 'K']
        valid_defaults = [s for s in default_stocks if s in available_symbols]
        valid_selected = valid_defaults if valid_defaults else (available_symbols[:5] if len(available_symbols) >= 5 else available_symbols)
    
    selected_stocks = st.multiselect(
        "Select stocks to monitor",
        options=available_symbols,
        default=valid_selected,
        help="Choose stocks to display in watchlist",
        key="stock_selector"
    )
    
    # Update session state
    if selected_stocks:
        st.session_state.selected_stocks = selected_stocks
    
    st.caption(f"{len(selected_stocks)} stocks selected")

if page == "Dashboard":
    @st.fragment(run_every=3 if auto_refresh else None)
    def render_dashboard():
        # Reload data inside fragment for updates
        df_raw_frag = load_spark_data(RAW_DATA_DIR)
        df_anom_frag = load_spark_data(ANOMALY_DIR)
        
        if df_raw_frag.empty:
            st.info("Waiting for data stream...")
            return

        # Filter recent data
        if 'minutes_elapsed' in df_raw_frag.columns:
            max_minutes = df_raw_frag['minutes_elapsed'].max()
            recent_df_frag = df_raw_frag[df_raw_frag['minutes_elapsed'] >= (max_minutes - time_range)]
        else:
            recent_df_frag = df_raw_frag

        # Top Metrics Row
        col1, col2, col3, col4 = st.columns(4)
        
        total_records = len(df_raw_frag)
        unique_symbols = len(df_raw_frag['symbol'].unique())
        avg_price = recent_df_frag['price'].mean() if not recent_df_frag.empty else 0
        anomaly_count = len(df_anom_frag) if not df_anom_frag.empty else 0
        
        with col1:
            st.markdown(f"""
            <div class="metric-card">
                <div class="metric-label">Total Records</div>
                <div class="metric-value">{total_records:,}</div>
            </div>
            """, unsafe_allow_html=True)
        
        with col2:
            st.markdown(f"""
            <div class="metric-card">
                <div class="metric-label">Active Stocks</div>
                <div class="metric-value">{unique_symbols}</div>
            </div>
            """, unsafe_allow_html=True)
        
        with col3:
            st.markdown(f"""
            <div class="metric-card">
                <div class="metric-label">Avg Price</div>
                <div class="metric-value">${avg_price:.2f}</div>
            </div>
            """, unsafe_allow_html=True)
        
        with col4:
            st.markdown(f"""
            <div class="metric-card">
                <div class="metric-label">Anomalies</div>
                <div class="metric-value">{anomaly_count}</div>
            </div>
            """, unsafe_allow_html=True)
        
        st.markdown("<br>", unsafe_allow_html=True)
        
        # Main Content - Two Columns
        col_left, col_right = st.columns([1, 2])
        
        with col_left:
            st.markdown('<div class="section-title">Watchlist</div>', unsafe_allow_html=True)
            
            if selected_stocks:
                watchlist_df = recent_df_frag[recent_df_frag['symbol'].isin(selected_stocks)]
                
                stock_data = watchlist_df.groupby('symbol').agg({
                    'price': ['last', 'first', 'count']
                }).reset_index()
                stock_data.columns = ['symbol', 'latest_price', 'first_price', 'count']
                stock_data['pct_change'] = ((stock_data['latest_price'] - stock_data['first_price']) / stock_data['first_price'] * 100).fillna(0)
                
                stock_data['symbol'] = pd.Categorical(stock_data['symbol'], categories=selected_stocks, ordered=True)
                stock_data = stock_data.sort_values('symbol')
                
                for _, row in stock_data.iterrows():
                    symbol = row['symbol']
                    price = row['latest_price']
                    pct = row['pct_change']
                    
                    change_class = "change-positive" if pct >= 0 else "change-negative"
                    arrow = "▲" if pct >= 0 else "▼"
                    
                    st.markdown(f"""
                    <div class="stock-item">
                        <div class="stock-info">
                            <div class="stock-icon">{symbol[:2]}</div>
                            <div class="stock-name">{symbol}</div>
                        </div>
                        <div style="text-align: right;">
                            <div class="stock-price">${price:.2f}</div>
                            <div class="{change_class}">{arrow} {abs(pct):.2f}%</div>
                        </div>
                    </div>
                    """, unsafe_allow_html=True)
            else:
                st.info("Select stocks from the sidebar to build your watchlist")
        
        with col_right:
            st.markdown('<div class="chart-container">', unsafe_allow_html=True)
            st.markdown('<div class="section-title">Price Trends</div>', unsafe_allow_html=True)
            
            fig = go.Figure()
            
            if selected_stocks:
                all_prices = []
                
                for symbol in selected_stocks[:5]:
                    stock_chart_data = recent_df_frag[recent_df_frag['symbol'] == symbol]
                    if len(stock_chart_data) >= 2:
                        all_prices.extend(stock_chart_data['price'].tolist())
                        fig.add_trace(go.Scatter(
                            x=stock_chart_data['minutes_elapsed'],
                            y=stock_chart_data['price'],
                            mode='lines+markers',
                            name=symbol,
                            line=dict(width=3),
                            marker=dict(size=6),
                            hovertemplate=f'<b>{symbol}</b><br>Price: $%{{y:.2f}}<br>Time: %{{x:.1f}} min<extra></extra>'
                        ))
                
                if all_prices:
                    min_price = min(all_prices)
                    max_price = max(all_prices)
                    price_range = max_price - min_price
                    
                    y_min = min_price - (price_range * 0.1)
                    y_max = max_price + (price_range * 0.1)
                    
                    if price_range < 1:
                        y_min = min_price - 0.5
                        y_max = max_price + 0.5
                else:
                    y_min = None
                    y_max = None
            else:
                y_min = None
                y_max = None
            
            fig.update_layout(
                height=400,
                template='plotly_white',
                hovermode='x unified',
                xaxis_title="Minutes Elapsed",
                yaxis_title="Price ($)",
                yaxis=dict(
                    range=[y_min, y_max] if y_min is not None else None,
                    fixedrange=False
                ),
                legend=dict(
                    orientation="h",
                    yanchor="bottom",
                    y=1.02,
                    xanchor="right",
                    x=1
                ),
                margin=dict(l=0, r=0, t=40, b=0)
            )
            
            st.plotly_chart(fig, use_container_width=True, config={'displayModeBar': False})
            st.markdown('</div>', unsafe_allow_html=True)

    # Call the fragment function
    render_dashboard()

elif page == "Analytics":
    st.markdown('<div class="section-title">Detailed Analytics</div>', unsafe_allow_html=True)
    
    stats_df = recent_df.groupby('symbol')['price'].agg([
        ('Latest', 'last'),
        ('Min', 'min'),
        ('Max', 'max'),
        ('Average', 'mean'),
        ('Std Dev', 'std'),
        ('Count', 'count')
    ]).round(2).sort_values('Count', ascending=False).head(15)
    
    st.dataframe(stats_df, use_container_width=True, height=600)

elif page == "Anomalies":
    st.markdown('<div class="section-title">Anomaly Detection</div>', unsafe_allow_html=True)
    
    if not df_anom.empty:
        recent_anomalies = df_anom.sort_values('timestamp', ascending=False).head(20)
        
        for _, anom in recent_anomalies.iterrows():
            col1, col2, col3, col4 = st.columns([2, 2, 2, 3])
            
            with col1:
                st.markdown(f"**{anom['symbol']}**")
            with col2:
                st.markdown(f"**${anom['price']:.2f}**")
            with col3:
                st.markdown(f"**Z-Score: {anom['z_score']:.2f}**")
            with col4:
                st.caption(anom['timestamp'].strftime('%Y-%m-%d %H:%M:%S'))
    else:
        st.success("No anomalies detected - System operating normally")
