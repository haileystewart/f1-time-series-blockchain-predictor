# Loads the processed data and trains machine learning models. This script encapsulates the model training routine, including splitting data, training, and saving model checkpoints.

import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestRegressor
from sklearn.metrics import mean_squared_error
import numpy as np

# Load the processed dataset (ensure dates are parsed)
df = pd.read_csv('../../data/processed/merged_f1_features.csv', parse_dates=['date'])

# Define your target variable; for example, let's assume you are predicting 'points'
target = 'points'
df = df.dropna(subset=[target])

# Select features to use in the model (adjust based on your dataset)
features = ['avg_lap_ms', 'pit_stop_frequency', 'normalized_lap_time', 'year', 'month', 'day_of_week']
X = df[features]
y = df[target]

# Split into training and testing sets
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

# Train a Random Forest model (example)
model = RandomForestRegressor(n_estimators=100, random_state=42)
model.fit(X_train, y_train)

# Predict and evaluate the model
y_pred = model.predict(X_test)
rmse = np.sqrt(mean_squared_error(y_test, y_pred))
print("Random Forest RMSE:", rmse)
