# Integrates preprocessing, feature engineering, and model training into a unified scikit-learn or TensorFlow pipeline. This file ensures your data flows seamlessly through each stage before predictions.

import pandas as pd
from sklearn.preprocessing import StandardScaler, LabelEncoder
import os

print("Current working directory:", os.getcwd())

# --------------------------------------------
# Helper Function: Safe CSV Reading
# --------------------------------------------
def safe_read_csv(file_path):
    """
    Safely read a CSV file. If the file is empty or doesn't exist,
    print a warning and return None.
    """
    if not os.path.exists(file_path) or os.path.getsize(file_path) == 0:
        print(f"Warning: {file_path} is empty or does not exist. Skipping this file.")
        return None
    try:
        return pd.read_csv(file_path)
    except Exception as e:
        print(f"Error reading {file_path}: {e}")
        return None

# --------------------------------------------
# Feature Engineering Functions
# --------------------------------------------
def add_time_features(df):
    """
    Add time-based features to the DataFrame based on the 'date' column.
    Assumes 'date' is in datetime format.
    
    Adds:
      - 'year': Year of the race.
      - 'month': Month of the race.
      - 'day_of_week': Day of the week (Monday=0, Sunday=6).
    """
    df['year'] = df['date'].dt.year
    df['month'] = df['date'].dt.month
    df['day_of_week'] = df['date'].dt.dayofweek
    return df

def add_performance_features(df):
    """
    Derive performance-related features.
    
    Assumes:
      - 'num_pit_stops': Total pit stops per driver per race.
      - 'num_laps': Total laps completed by the driver.
      - 'avg_lap_ms': Average lap time (in ms).
    
    Adds:
      - 'pit_stop_frequency': Pit stops divided by laps.
      - 'normalized_lap_time': Driver's avg lap time divided by the race's avg lap time.
    """
    df['num_laps'].replace(0, pd.NA, inplace=True)
    df['pit_stop_frequency'] = df['num_pit_stops'] / df['num_laps']
    race_avg = df.groupby('race_id')['avg_lap_ms'].transform('mean')
    df['normalized_lap_time'] = df['avg_lap_ms'] / race_avg
    return df

def scale_and_encode_features(df):
    """
    Scale numerical features and encode categorical features.
    
    Scales: 'lap_time' and 'avg_lap_ms'
    Encodes: 'driver_name' (if available)
    """
    scaler = StandardScaler()
    if 'lap_time' in df.columns and 'avg_lap_ms' in df.columns:
        df[['lap_time', 'avg_lap_ms']] = scaler.fit_transform(df[['lap_time', 'avg_lap_ms']])
    if 'driver_name' in df.columns:
        le = LabelEncoder()
        df['driver_encoded'] = le.fit_transform(df['driver_name'])
    return df

def calculate_additional_features(df):
    """
    Create additional features such as race duration.
    
    Placeholder example: sets 'race_duration' to 1.0 if not already present.
    """
    if 'race_duration' not in df.columns and 'race_date' in df.columns:
        df['race_duration'] = 1.0  # Replace with real logic if available.
    return df

# --------------------------------------------
# Main Pipeline
# --------------------------------------------
if __name__ == "__main__":
    # Step 1: Load the master dataset (pre-feature engineered merged CSV)
    master_df = pd.read_csv('data/processed/merged_f1_data.csv', parse_dates=['date'])
    
    # Step 2: Feature Engineering and Preprocessing
    master_df = add_time_features(master_df)
    master_df = add_performance_features(master_df)
    master_df = calculate_additional_features(master_df)
    master_df = scale_and_encode_features(master_df)
    
    # Save the feature-engineered master dataset
    master_df.to_csv('data/processed/merged_f1_features.csv', index=False)
    print("Feature engineering and preprocessing completed. Saved to merged_f1_features.csv")
    
    # Step 3: Load API Data Files (example: OpenF1 car telemetry data)
    car_data_path = 'data/processed/openf1/openf1_car_data.csv'
    car_data = safe_read_csv(car_data_path)
    
    # Step 4: Merge API Data with the Master Dataset
    if car_data is not None:
        # Ensure common keys are aligned: convert master_df 'driver_id' and API 'driver_number' to integers.
        master_df['driver_id'] = master_df['driver_id'].astype(int)
        car_data['driver_number'] = car_data['driver_number'].astype(int)
    
        merged_with_api = pd.merge(master_df, car_data, left_on='driver_id', right_on='driver_number', how='left')
        # Save the final merged dataset with API data
        merged_with_api.to_csv('data/processed/merged_with_api.csv', index=False)
        print("Merged master dataset with API data. Shape:", merged_with_api.shape)
    else:
        print("No API car telemetry data available. Master dataset remains unchanged.")
