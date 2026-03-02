"""
Pipeline Stress Analysis Dashboard
Interactive web dashboard for pipeline monitoring and stress analysis
Author: Andrey Totuan
Date: January 2026

Run: streamlit run dashboard.py
"""

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from pipeline_analysis import PipelineAnalyzer

# Page config
st.set_page_config(
    page_title="Pipeline Stress Analysis",
    page_icon="⚡",
    layout="wide"
)

# Custom CSS
st.markdown("""
<style>
.big-metric { font-size: 24px; font-weight: bold; }
.status-good { color: #28a745; }
.status-warning { color: #ffc107; }
.status-danger { color: #dc3545; }
</style>
""", unsafe_allow_html=True)

# Title
st.title("⚡ Pipeline Stress Analysis Dashboard")
st.markdown("Real-time monitoring and predictive maintenance for industrial pipeline operations")

# Load data
@st.cache_data
def load_data():
    analyzer = PipelineAnalyzer('pipeline_monitoring_data.csv')
    return analyzer

try:
    analyzer = load_data()
    df = analyzer.df
    stats = analyzer.calculate_statistics()
    
    # Sidebar filters
    st.sidebar.header("📊 Filters")
    
    # Date range
    date_range = st.sidebar.date_input(
        "Date Range",
        value=(df['timestamp'].min(), df['timestamp'].max()),
        min_value=df['timestamp'].min(),
        max_value=df['timestamp'].max()
    )
    
    # Segment filter
    segments = ['All'] + sorted(df['pipeline_segment'].unique().tolist())
    selected_segment = st.sidebar.selectbox("Pipeline Segment", segments)
    
    # Filter data
    mask = (df['timestamp'].dt.date >= date_range[0]) & (df['timestamp'].dt.date <= date_range[1])
    if selected_segment != 'All':
        mask = mask & (df['pipeline_segment'] == selected_segment)
    
    filtered_df = df[mask]
    
    # KPIs
    st.header("📈 Key Performance Indicators")
    col1, col2, col3, col4, col5 = st.columns(5)
    
    with col1:
        st.metric(
            "Avg Pressure",
            f"{stats['avg_pressure_psi']:.0f} psi",
            f"±{stats['pressure_std']:.0f}"
        )
    
    with col2:
        st.metric(
            "Avg Temperature",
            f"{stats['avg_temperature_c']:.1f}°C",
            f"Range: {stats['temp_range']:.1f}°C"
        )
    
    with col3:
        anomaly_status = "🟢" if stats['anomaly_rate_pct'] < 1 else "🔴"
        st.metric(
            "Anomaly Rate",
            f"{stats['anomaly_rate_pct']:.2f}%",
            f"{stats['total_anomalies']} events {anomaly_status}"
        )
    
    with col4:
        stress_status = "🟢" if stats['max_stress_factor'] < 0.5 else "🟡" if stats['max_stress_factor'] < 0.7 else "🔴"
        st.metric(
            "Max Stress",
            f"{stats['max_stress_factor']:.3f}",
            f"Avg: {stats['avg_stress_factor']:.3f} {stress_status}"
        )
    
    with col5:
        st.metric(
            "Min Wall Thickness",
            f"{stats['min_wall_thickness']:.2f} mm",
            "⚠️" if stats['min_wall_thickness'] < 12 else "✅"
        )
    
    # Time series charts
    st.header("📊 Operational Metrics Over Time")
    
    # Pressure trend
    fig_pressure = go.Figure()
    fig_pressure.add_trace(go.Scatter(
        x=filtered_df['timestamp'],
        y=filtered_df['pressure_psi'],
        mode='lines',
        name='Pressure',
        line=dict(color='#1f77b4', width=1)
    ))
    fig_pressure.add_hline(
        y=stats['avg_pressure_psi'],
        line_dash="dash",
        line_color="gray",
        annotation_text="Avg"
    )
    fig_pressure.update_layout(
        title="Pressure Trends",
        xaxis_title="Time",
        yaxis_title="Pressure (psi)",
        height=300
    )
    st.plotly_chart(fig_pressure, use_container_width=True)
    
    # Temperature and Flow
    col1, col2 = st.columns(2)
    
    with col1:
        fig_temp = px.line(
            filtered_df,
            x='timestamp',
            y='temperature_c',
            title="Temperature Profile",
            labels={'temperature_c': 'Temperature (°C)'}
        )
        fig_temp.update_layout(height=300)
        st.plotly_chart(fig_temp, use_container_width=True)
    
    with col2:
        fig_flow = px.line(
            filtered_df,
            x='timestamp',
            y='flow_rate_m3h',
            title="Flow Rate",
            labels={'flow_rate_m3h': 'Flow Rate (m³/h)'}
        )
        fig_flow.update_layout(height=300)
        st.plotly_chart(fig_flow, use_container_width=True)
    
    # Stress analysis
    st.header("⚡ Stress Factor Analysis")
    
    fig_stress = go.Figure()
    fig_stress.add_trace(go.Scatter(
        x=filtered_df['timestamp'],
        y=filtered_df['stress_factor'],
        mode='lines',
        name='Stress Factor',
        line=dict(color='orange', width=1)
    ))
    fig_stress.add_hline(y=0.5, line_dash="dash", line_color="yellow", annotation_text="Medium Stress")
    fig_stress.add_hline(y=0.7, line_dash="dash", line_color="red", annotation_text="High Stress")
    fig_stress.update_layout(
        title="Stress Factor Over Time",
        xaxis_title="Time",
        yaxis_title="Stress Factor",
        height=350
    )
    st.plotly_chart(fig_stress, use_container_width=True)
    
    # Anomaly detection
    st.header("🚨 Anomaly Detection")
    
    anomalies = filtered_df[filtered_df['anomaly_flag'] == 1]
    
    fig_anomaly = px.scatter(
        filtered_df,
        x='timestamp',
        y='pressure_psi',
        color='anomaly_flag',
        title=f"Detected Anomalies ({len(anomalies)} events in selected period)",
        labels={'anomaly_flag': 'Anomaly'},
        color_continuous_scale=['lightblue', 'red']
    )
    fig_anomaly.update_layout(height=350)
    st.plotly_chart(fig_anomaly, use_container_width=True)
    
    if len(anomalies) > 0:
        st.subheader("Recent Anomaly Events")
        anomaly_display = anomalies[['timestamp', 'pressure_psi', 'temperature_c', 
                                     'flow_rate_m3h', 'anomaly_type', 'pipeline_segment']].tail(10)
        st.dataframe(anomaly_display, use_container_width=True)
    
    # Segment comparison
    st.header("📊 Pipeline Segment Analysis")
    
    segment_stats = analyzer.segment_analysis()
    
    col1, col2 = st.columns(2)
    
    with col1:
        fig_segment_stress = px.bar(
            segment_stats.reset_index(),
            x='pipeline_segment',
            y='stress_factor_mean',
            title="Average Stress by Segment",
            labels={'stress_factor_mean': 'Avg Stress Factor'},
            color='stress_factor_mean',
            color_continuous_scale='Reds'
        )
        st.plotly_chart(fig_segment_stress, use_container_width=True)
    
    with col2:
        fig_segment_anomalies = px.bar(
            segment_stats.reset_index(),
            x='pipeline_segment',
            y='anomaly_flag_sum',
            title="Anomalies by Segment",
            labels={'anomaly_flag_sum': 'Total Anomalies'},
            color='anomaly_flag_sum',
            color_continuous_scale='Oranges'
        )
        st.plotly_chart(fig_segment_anomalies, use_container_width=True)
    
    # Maintenance priorities
    st.header("🔧 Predictive Maintenance Recommendations")
    
    maintenance = analyzer.predict_maintenance_needs()
    
    # Color code risk levels
    def color_risk(val):
        if val == 'High':
            return 'background-color: #ffcccc'
        elif val == 'Medium':
            return 'background-color: #fff3cd'
        else:
            return 'background-color: #d4edda'
    
    styled_maintenance = maintenance.style.applymap(
        color_risk,
        subset=['Risk Level']
    )
    
    st.dataframe(styled_maintenance, use_container_width=True)
    
    # High stress events
    st.header("⏰ High Stress Events")
    
    events = analyzer.identify_high_stress_periods()
    if len(events) > 0:
        st.write(f"Found **{len(events)}** high-stress events (stress factor > 0.5)")
        st.dataframe(events.head(15), use_container_width=True)
    else:
        st.success("✅ No significant high-stress events detected in selected period")
    
    # Download section
    st.header("📥 Export Data")
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        csv = filtered_df.to_csv(index=False)
        st.download_button(
            "Download Filtered Data (CSV)",
            csv,
            "pipeline_data_filtered.csv",
            "text/csv"
        )
    
    with col2:
        maintenance_csv = maintenance.to_csv()
        st.download_button(
            "Download Maintenance Report (CSV)",
            maintenance_csv,
            "maintenance_priorities.csv",
            "text/csv"
        )
    
    with col3:
        if len(events) > 0:
            events_csv = events.to_csv(index=False)
            st.download_button(
                "Download Stress Events (CSV)",
                events_csv,
                "high_stress_events.csv",
                "text/csv"
            )

except FileNotFoundError:
    st.error("⚠️ Data file not found. Please run `python generate_pipeline_data.py` first.")
except Exception as e:
    st.error(f"An error occurred: {str(e)}")

# Footer
st.markdown("---")
st.markdown("""
**Pipeline Stress Analysis Dashboard**  
Developed by: Andrey Totuan | Business Analyst Portfolio  
GitHub: github.com/andreyeramte/business-analyst-portfolio
""")