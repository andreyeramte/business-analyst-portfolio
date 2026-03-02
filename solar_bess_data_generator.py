# solar_bess_data_generator.py
# Generate realistic solar + battery storage operational data

import pandas as pd
import numpy as np
from datetime import datetime, timedelta

def generate_solar_bess_data(days=90):
    """
    Generates solar PV + BESS operational data
    Includes: solar generation, battery charge/discharge, grid export/import
    """
    
    start_date = datetime(2025, 10, 1)  # Start in October (autumn data)
    timestamps = pd.date_range(start=start_date, periods=days*24*4, freq='15min')
    
    data = []
    battery_soc = 50  # State of charge starts at 50%
    
    for i, ts in enumerate(timestamps):
        hour = ts.hour
        day_of_year = ts.timetuple().tm_yday
        
        # Solar generation (simplified model)
        # Peak at noon, zero at night, seasonal variation
        if 6 <= hour <= 20:
            solar_factor = np.sin((hour - 6) * np.pi / 14)  # 0 to 1 to 0 curve
            seasonal = 0.7 + 0.3 * np.sin(2 * np.pi * day_of_year / 365)
            cloud_factor = np.random.uniform(0.7, 1.0)  # Random clouds
            solar_generation = 100 * solar_factor * seasonal * cloud_factor  # kW
        else:
            solar_generation = 0
        
        # Demand profile (residential/commercial mix)
        if 0 <= hour < 6:
            base_demand = 30  # Night base load
        elif 6 <= hour < 9:
            base_demand = 80  # Morning peak
        elif 9 <= hour < 17:
            base_demand = 60  # Daytime
        elif 17 <= hour < 22:
            base_demand = 90  # Evening peak
        else:
            base_demand = 40  # Late evening
        
        demand = base_demand + np.random.normal(0, 5)
        
        # Battery logic
        net_generation = solar_generation - demand
        
        if net_generation > 0 and battery_soc < 95:
            # Excess solar, charge battery
            charge_rate = min(net_generation, 50, (95 - battery_soc) * 2)
            battery_charge = charge_rate
            battery_discharge = 0
            battery_soc += charge_rate / 200 * 100  # 200 kWh capacity
            grid_export = max(0, net_generation - charge_rate)
            grid_import = 0
        elif net_generation < 0 and battery_soc > 10:
            # Deficit, discharge battery
            discharge_rate = min(abs(net_generation), 50, (battery_soc - 10) * 2)
            battery_discharge = discharge_rate
            battery_charge = 0
            battery_soc -= discharge_rate / 200 * 100
            grid_import = max(0, abs(net_generation) - discharge_rate)
            grid_export = 0
        else:
            # Battery at limits
            battery_charge = 0
            battery_discharge = 0
            if net_generation > 0:
                grid_export = net_generation
                grid_import = 0
            else:
                grid_import = abs(net_generation)
                grid_export = 0
        
        # Clamp SOC
        battery_soc = max(5, min(95, battery_soc))
        
        # Battery temperature (affected by charge/discharge rate)
        ambient_temp = 20 + 10 * np.sin(2 * np.pi * hour / 24)
        temp_rise = (battery_charge + battery_discharge) * 0.1
        battery_temp = ambient_temp + temp_rise + np.random.normal(0, 1)
        
        # System efficiency
        system_efficiency = 0.95 - (battery_temp - 25) * 0.001  # Decreases with temp
        
        # Anomaly injection (battery issues, inverter faults)
        anomaly = 0
        if np.random.random() < 0.01:  # 1% anomaly rate
            anomaly_type = np.random.choice(['overtemp', 'low_efficiency', 'connection_loss'])
            if anomaly_type == 'overtemp':
                battery_temp += np.random.uniform(10, 20)
                anomaly = 1
            elif anomaly_type == 'low_efficiency':
                system_efficiency *= 0.7
                anomaly = 1
            elif anomaly_type == 'connection_loss':
                grid_export = 0
                grid_import = demand
                anomaly = 1
        
        data.append({
            'timestamp': ts,
            'solar_generation_kw': round(solar_generation, 2),
            'demand_kw': round(demand, 2),
            'battery_charge_kw': round(battery_charge, 2),
            'battery_discharge_kw': round(battery_discharge, 2),
            'battery_soc_percent': round(battery_soc, 2),
            'battery_temp_c': round(battery_temp, 2),
            'grid_export_kw': round(grid_export, 2),
            'grid_import_kw': round(grid_import, 2),
            'system_efficiency': round(system_efficiency, 4),
            'anomaly_flag': anomaly
        })
    
    df = pd.DataFrame(data)
    df.to_csv('solar_bess_data.csv', index=False)
    
    print(f"✅ Generated {len(df)} records")
    print(f"📅 Date range: {df['timestamp'].min()} to {df['timestamp'].max()}")
    print(f"⚡ Total solar generated: {df['solar_generation_kw'].sum():.0f} kWh")
    print(f"🔋 Avg battery SOC: {df['battery_soc_percent'].mean():.1f}%")
    print(f"🚨 Anomalies detected: {df['anomaly_flag'].sum()}")
    
    return df

if __name__ == "__main__":
    df = generate_solar_bess_data(days=90)
    print("\n📊 Sample data:")
    print(df.head(10))
    print("\n📈 Summary statistics:")
    print(df.describe())