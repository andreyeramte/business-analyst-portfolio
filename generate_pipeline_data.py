"""
Pipeline Monitoring Data Generator
Generates 90 days of realistic pipeline operational data
Author: Andrey Totuan
Date: January 2026
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta

def generate_pipeline_data(days=90):
    """
    Generates synthetic pipeline monitoring data
    Simulates: Natural gas pipeline with seasonal variations and anomalies
    """
    
    # Time range
    start_date = datetime(2025, 10, 1)  # October 2025
    timestamps = pd.date_range(start=start_date, periods=days*24, freq='H')
    
    # Base parameters
    base_pressure = 1200  # psi
    base_temp = 15  # Celsius
    base_flow = 50000  # cubic meters/hour
    
    data = []
    
    for i, ts in enumerate(timestamps):
        hour = ts.hour
        day_of_year = ts.timetuple().tm_yday
        
        # Seasonal temperature variation
        seasonal_temp = 10 * np.sin(2 * np.pi * day_of_year / 365)
        
        # Daily temperature cycle
        daily_temp = 5 * np.sin(2 * np.pi * hour / 24)
        
        # Random noise
        noise_temp = np.random.normal(0, 2)
        noise_pressure = np.random.normal(0, 50)
        noise_flow = np.random.normal(0, 2000)
        
        # Occasional anomalies (pressure spikes, temperature drops)
        anomaly = 0
        anomaly_type = None
        
        if np.random.random() < 0.02:  # 2% chance of anomaly
            anomaly_type = np.random.choice(['pressure_spike', 'temp_drop', 'flow_surge'])
            if anomaly_type == 'pressure_spike':
                noise_pressure += np.random.uniform(200, 400)
                anomaly = 1
            elif anomaly_type == 'temp_drop':
                noise_temp -= np.random.uniform(10, 20)
                anomaly = 1
            elif anomaly_type == 'flow_surge':
                noise_flow += np.random.uniform(10000, 20000)
                anomaly = 1
        
        # Calculate values
        temperature = base_temp + seasonal_temp + daily_temp + noise_temp
        pressure = base_pressure + noise_pressure
        flow_rate = base_flow + noise_flow
        
        # Calculate stress indicator (simplified engineering model)
        # Real stress calculation would be much more complex
        pressure_stress = abs(pressure - base_pressure) / base_pressure * 0.4
        temp_stress = abs(temperature - base_temp) / 20 * 0.3
        flow_stress = abs(flow_rate - base_flow) / base_flow * 0.3
        
        stress_factor = pressure_stress + temp_stress + flow_stress
        
        # Vibration (correlated with flow rate)
        vibration = 0.5 + (flow_rate / base_flow - 1) * 0.3 + np.random.normal(0, 0.1)
        vibration = max(0, vibration)
        
        # Wall thickness (degrades over time with stress)
        initial_thickness = 12.7  # mm
        degradation = stress_factor * 0.001 * (i / 24)  # Cumulative degradation
        wall_thickness = initial_thickness - degradation + np.random.normal(0, 0.05)
        
        data.append({
            'timestamp': ts,
            'pressure_psi': round(pressure, 2),
            'temperature_c': round(temperature, 2),
            'flow_rate_m3h': round(flow_rate, 2),
            'stress_factor': round(stress_factor, 4),
            'vibration_mm_s': round(vibration, 3),
            'wall_thickness_mm': round(wall_thickness, 3),
            'anomaly_flag': anomaly,
            'anomaly_type': anomaly_type if anomaly else None,
            'pipeline_segment': f'Segment_{np.random.randint(1, 6)}',
            'location_km': np.random.choice([0, 25, 50, 75, 100])  # Distance markers
        })
    
    df = pd.DataFrame(data)
    
    # Save to CSV
    df.to_csv('pipeline_monitoring_data.csv', index=False)
    
    print("✅ DATA GENERATION COMPLETE")
    print("=" * 50)
    print(f"📊 Total records: {len(df):,}")
    print(f"📅 Date range: {df['timestamp'].min()} to {df['timestamp'].max()}")
    print(f"⚠️ Anomalies detected: {df['anomaly_flag'].sum()}")
    print(f"📈 Avg pressure: {df['pressure_psi'].mean():.1f} psi")
    print(f"🌡️ Avg temperature: {df['temperature_c'].mean():.1f}°C")
    print(f"💨 Avg flow rate: {df['flow_rate_m3h'].mean():.1f} m³/h")
    print(f"⚡ Max stress factor: {df['stress_factor'].max():.3f}")
    print("=" * 50)
    print("\n📁 Saved to: pipeline_monitoring_data.csv")
    
    return df

if __name__ == "__main__":
    print("🔧 PIPELINE MONITORING DATA GENERATOR")
    print("=" * 50)
    df = generate_pipeline_data(days=90)
    
    print("\n📊 SAMPLE DATA:")
    print(df.head(10))
    
    print("\n📈 STATISTICAL SUMMARY:")
    print(df.describe())
    
    print("\n🎯 ANOMALY BREAKDOWN:")
    if df['anomaly_flag'].sum() > 0:
        anomaly_counts = df[df['anomaly_flag'] == 1]['anomaly_type'].value_counts()
        print(anomaly_counts)