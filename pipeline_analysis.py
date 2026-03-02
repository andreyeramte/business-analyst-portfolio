"""
Pipeline Stress Analysis Module
Analyzes operational data for stress patterns, anomalies, and maintenance needs
Author: Andrey Totuan
Date: January 2026
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta

class PipelineAnalyzer:
    """
    Pipeline operational data analyzer
    Identifies stress patterns, anomalies, and maintenance priorities
    """
    
    def __init__(self, data_file):
        """Load pipeline data from CSV"""
        self.df = pd.read_csv(data_file)
        self.df['timestamp'] = pd.to_datetime(self.df['timestamp'])
        print(f"✅ Loaded {len(self.df):,} records from {data_file}")
    
    def calculate_statistics(self):
        """Calculate key operational statistics"""
        stats = {
            'total_records': len(self.df),
            'date_range_days': (self.df['timestamp'].max() - self.df['timestamp'].min()).days,
            'avg_pressure_psi': self.df['pressure_psi'].mean(),
            'max_pressure_psi': self.df['pressure_psi'].max(),
            'min_pressure_psi': self.df['pressure_psi'].min(),
            'pressure_std': self.df['pressure_psi'].std(),
            'avg_temperature_c': self.df['temperature_c'].mean(),
            'temp_range': self.df['temperature_c'].max() - self.df['temperature_c'].min(),
            'avg_flow_rate': self.df['flow_rate_m3h'].mean(),
            'total_anomalies': self.df['anomaly_flag'].sum(),
            'anomaly_rate_pct': (self.df['anomaly_flag'].sum() / len(self.df)) * 100,
            'avg_stress_factor': self.df['stress_factor'].mean(),
            'max_stress_factor': self.df['stress_factor'].max(),
            'high_stress_events': len(self.df[self.df['stress_factor'] > 0.5]),
            'avg_vibration': self.df['vibration_mm_s'].mean(),
            'min_wall_thickness': self.df['wall_thickness_mm'].min()
        }
        return stats
    
    def identify_high_stress_periods(self, threshold=0.5):
        """
        Identify time periods with elevated stress factors
        Threshold: 0.5 = medium stress, 0.7 = high stress, 0.9 = critical
        """
        high_stress = self.df[self.df['stress_factor'] > threshold].copy()
        
        if len(high_stress) == 0:
            return pd.DataFrame()
        
        # Calculate duration of stress events
        high_stress = high_stress.sort_values('timestamp')
        high_stress['time_diff'] = high_stress['timestamp'].diff().dt.total_seconds() / 3600
        
        # Group consecutive hours into events
        high_stress['event_group'] = (high_stress['time_diff'] > 2).cumsum()
        
        # Aggregate by event
        events = high_stress.groupby('event_group').agg({
            'timestamp': ['first', 'last', 'count'],
            'stress_factor': ['mean', 'max'],
            'pressure_psi': 'max',
            'temperature_c': ['min', 'max'],
            'pipeline_segment': lambda x: x.mode()[0] if len(x) > 0 else None
        }).reset_index(drop=True)
        
        events.columns = ['start_time', 'end_time', 'duration_hours', 
                         'avg_stress', 'max_stress', 'max_pressure',
                         'min_temp', 'max_temp', 'segment']
        
        return events.sort_values('max_stress', ascending=False)
    
    def segment_analysis(self):
        """
        Analyze performance and risk by pipeline segment
        """
        segment_stats = self.df.groupby('pipeline_segment').agg({
            'pressure_psi': ['mean', 'std', 'max', 'min'],
            'temperature_c': ['mean', 'std'],
            'stress_factor': ['mean', 'max', 'count'],
            'anomaly_flag': 'sum',
            'vibration_mm_s': 'mean',
            'wall_thickness_mm': 'min'
        }).round(2)
        
        segment_stats.columns = ['_'.join(col).strip() for col in segment_stats.columns.values]
        
        # Calculate risk score per segment
        segment_stats['risk_score'] = (
            segment_stats['stress_factor_mean'] * 40 +
            segment_stats['anomaly_flag_sum'] * 5 +
            (13 - segment_stats['wall_thickness_mm_min']) * 10  # Thinner wall = higher risk
        ).round(2)
        
        segment_stats['risk_level'] = pd.cut(
            segment_stats['risk_score'],
            bins=[0, 30, 60, float('inf')],
            labels=['Low', 'Medium', 'High']
        )
        
        return segment_stats.sort_values('risk_score', ascending=False)
    
    def predict_maintenance_needs(self):
        """
        Prioritize segments for maintenance based on multiple factors
        """
        segment_risk = self.segment_analysis()
        
        # Create maintenance priority
        maintenance = segment_risk[['stress_factor_mean', 'anomaly_flag_sum', 
                                   'wall_thickness_mm_min', 'risk_score', 'risk_level']].copy()
        
        maintenance.columns = ['Avg Stress', 'Anomalies', 'Min Wall Thickness (mm)', 
                              'Risk Score', 'Risk Level']
        
        # Add recommendation
        def get_recommendation(row):
            if row['Risk Level'] == 'High':
                return 'URGENT: Schedule inspection within 7 days'
            elif row['Risk Level'] == 'Medium':
                return 'Schedule inspection within 30 days'
            else:
                return 'Routine monitoring'
        
        maintenance['Recommendation'] = maintenance.apply(get_recommendation, axis=1)
        
        return maintenance
    
    def detect_anomalies_statistical(self):
        """
        Detect anomalies using statistical methods (z-score)
        Complements the injected anomalies
        """
        # Calculate z-scores
        self.df['pressure_zscore'] = np.abs(
            (self.df['pressure_psi'] - self.df['pressure_psi'].mean()) / 
            self.df['pressure_psi'].std()
        )
        
        self.df['temp_zscore'] = np.abs(
            (self.df['temperature_c'] - self.df['temperature_c'].mean()) / 
            self.df['temperature_c'].std()
        )
        
        self.df['flow_zscore'] = np.abs(
            (self.df['flow_rate_m3h'] - self.df['flow_rate_m3h'].mean()) / 
            self.df['flow_rate_m3h'].std()
        )
        
        # Flag statistical anomalies (z-score > 3 = outlier)
        self.df['statistical_anomaly'] = (
            (self.df['pressure_zscore'] > 3) |
            (self.df['temp_zscore'] > 3) |
            (self.df['flow_zscore'] > 3)
        ).astype(int)
        
        anomaly_summary = {
            'total_statistical_anomalies': self.df['statistical_anomaly'].sum(),
            'pressure_outliers': (self.df['pressure_zscore'] > 3).sum(),
            'temperature_outliers': (self.df['temp_zscore'] > 3).sum(),
            'flow_outliers': (self.df['flow_zscore'] > 3).sum()
        }
        
        return anomaly_summary
    
    def generate_report(self):
        """Generate comprehensive analysis report"""
        print("\n" + "=" * 70)
        print("PIPELINE STRESS ANALYSIS REPORT")
        print("=" * 70)
        
        stats = self.calculate_statistics()
        
        print("\n📊 OPERATIONAL STATISTICS")
        print("-" * 70)
        print(f"Analysis Period: {stats['date_range_days']} days ({stats['total_records']:,} measurements)")
        print(f"Average Pressure: {stats['avg_pressure_psi']:.1f} psi (Range: {stats['min_pressure_psi']:.1f} - {stats['max_pressure_psi']:.1f})")
        print(f"Pressure Variability: {stats['pressure_std']:.1f} psi std dev")
        print(f"Average Temperature: {stats['avg_temperature_c']:.1f}°C (Range: {stats['temp_range']:.1f}°C)")
        print(f"Average Flow Rate: {stats['avg_flow_rate']:.1f} m³/h")
        
        print("\n⚡ STRESS ANALYSIS")
        print("-" * 70)
        print(f"Average Stress Factor: {stats['avg_stress_factor']:.3f}")
        print(f"Maximum Stress Factor: {stats['max_stress_factor']:.3f}")
        print(f"High Stress Events (>0.5): {stats['high_stress_events']}")
        
        print("\n⚠️ ANOMALY DETECTION")
        print("-" * 70)
        print(f"Detected Anomalies: {stats['total_anomalies']}")
        print(f"Anomaly Rate: {stats['anomaly_rate_pct']:.2f}%")
        
        anomaly_stats = self.detect_anomalies_statistical()
        print(f"Statistical Outliers: {anomaly_stats['total_statistical_anomalies']}")
        
        print("\n🔧 MAINTENANCE PRIORITIES")
        print("-" * 70)
        maintenance = self.predict_maintenance_needs()
        print(maintenance.to_string())
        
        print("\n⏰ HIGH STRESS EVENTS")
        print("-" * 70)
        events = self.identify_high_stress_periods()
        if len(events) > 0:
            print(f"Found {len(events)} high-stress events:")
            print(events.head(10).to_string())
        else:
            print("No significant high-stress events detected")
        
        print("\n" + "=" * 70)

# Usage example
if __name__ == "__main__":
    analyzer = PipelineAnalyzer('pipeline_monitoring_data.csv')
    analyzer.generate_report()