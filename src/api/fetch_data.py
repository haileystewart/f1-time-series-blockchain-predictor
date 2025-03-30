# Contains helper functions to query external APIs (e.g., OpenF1, fastf1, jolpica-f1). This file is responsible for fetching, processing, and converting JSON responses into Pandas DataFrames for further integration.

import requests
import pandas as pd
import os

def safe_save(df, path, description):
    """
    Save the DataFrame to CSV if it is not empty.
    Otherwise, print a warning message.
    """
    if df.empty:
        print(f"Warning: {description} returned no data. File not saved to {path}.")
    else:
        df.to_csv(path, index=False)
        print(f"{description} fetched and saved. Records: {len(df)}.")

# Car Data
def fetch_openf1_car_data(driver_number, session_key, speed_threshold=100):
    """
    Fetch car telemetry data from the OpenF1 API for a specific driver and session.
    Lowered speed_threshold to 100 to potentially return more data.
    """
    base_url = "https://api.openf1.org/v1/car_data"
    params = {
        "driver_number": driver_number,
        "session_key": session_key,
        "speed>=": speed_threshold
    }
    response = requests.get(base_url, params=params)
    response.raise_for_status()
    data = response.json()
    print(f"Fetched {len(data)} car data records.")
    return pd.DataFrame(data)

# Drivers
def fetch_openf1_drivers(driver_number, session_key):
    """
    Fetch driver information from the OpenF1 API.
    """
    base_url = "https://api.openf1.org/v1/drivers"
    params = {
        "driver_number": driver_number,
        "session_key": session_key
    }
    response = requests.get(base_url, params=params)
    response.raise_for_status()
    data = response.json()
    print(f"Fetched {len(data)} driver info records.")
    return pd.DataFrame(data)

# Weather
def fetch_openf1_weather(meeting_key, track_temperature_threshold=30):
    """
    Fetch weather data from the OpenF1 API for a given meeting.
    Lowered track_temperature_threshold to 30 to capture more records.
    """
    base_url = "https://api.openf1.org/v1/weather"
    params = {
        "meeting_key": meeting_key,
        "track_temperature>=": track_temperature_threshold
    }
    response = requests.get(base_url, params=params)
    response.raise_for_status()
    data = response.json()
    print(f"Fetched {len(data)} weather records.")
    return pd.DataFrame(data)

# Intervals
def fetch_openf1_intervals(session_key, interval_threshold=0.005):
    """
    Fetch real-time interval data from the OpenF1 API.
    """
    base_url = "https://api.openf1.org/v1/intervals"
    params = {"session_key": session_key, "interval<": interval_threshold}
    response = requests.get(base_url, params=params)
    response.raise_for_status()
    data = response.json()
    print(f"Fetched {len(data)} interval records.")
    return pd.DataFrame(data)

# Laps
def fetch_openf1_laps(session_key, driver_number, lap_number):
    """
    Fetch detailed lap information for a specific driver and lap.
    """
    base_url = "https://api.openf1.org/v1/laps"
    params = {"session_key": session_key, "driver_number": driver_number, "lap_number": lap_number}
    response = requests.get(base_url, params=params)
    response.raise_for_status()
    data = response.json()
    print(f"Fetched {len(data)} lap records for driver {driver_number}, lap {lap_number}.")
    return pd.DataFrame(data)

# Location
def fetch_openf1_location(session_key, driver_number, date_from, date_to):
    """
    Fetch location data for a driver within a specified time range.
    """
    base_url = "https://api.openf1.org/v1/location"
    params = {
        "session_key": session_key,
        "driver_number": driver_number,
        "date>": date_from,
        "date<": date_to
    }
    response = requests.get(base_url, params=params)
    response.raise_for_status()
    data = response.json()
    print(f"Fetched {len(data)} location records for driver {driver_number}.")
    return pd.DataFrame(data)

# Meetings
def fetch_openf1_meetings(year=None, country_name=None, session_name=None):
    """
    Fetch meeting information from the OpenF1 API.
    """
    base_url = "https://api.openf1.org/v1/meetings"
    params = {}
    if year:
        params["year"] = year
    if country_name:
        params["country_name"] = country_name
    if session_name:
        params["session_name"] = session_name
    response = requests.get(base_url, params=params)
    response.raise_for_status()
    data = response.json()
    print(f"Fetched {len(data)} meeting records.")
    return pd.DataFrame(data)

# Pit Stops
def fetch_openf1_pit(session_key, pit_duration_threshold=31):
    """
    Fetch pit data from the OpenF1 API.
    """
    base_url = "https://api.openf1.org/v1/pit"
    params = {"session_key": session_key, "pit_duration<": pit_duration_threshold}
    response = requests.get(base_url, params=params)
    response.raise_for_status()
    data = response.json()
    print(f"Fetched {len(data)} pit stop records.")
    return pd.DataFrame(data)

# Position
def fetch_openf1_position(meeting_key, driver_number, position_threshold=3):
    """
    Fetch driver position data from the OpenF1 API.
    """
    base_url = "https://api.openf1.org/v1/position"
    params = {"meeting_key": meeting_key, "driver_number": driver_number, "position<=": position_threshold}
    response = requests.get(base_url, params=params)
    response.raise_for_status()
    data = response.json()
    print(f"Fetched {len(data)} position records for driver {driver_number}.")
    return pd.DataFrame(data)

# Race Control
def fetch_openf1_race_control(flag=None, driver_number=None, date_from=None, date_to=None):
    """
    Fetch race control information (flags, incidents) from the OpenF1 API.
    """
    base_url = "https://api.openf1.org/v1/race_control"
    params = {}
    if flag:
        params["flag"] = flag
    if driver_number:
        params["driver_number"] = driver_number
    if date_from:
        params["date>="] = date_from
    if date_to:
        params["date<="] = date_to
    response = requests.get(base_url, params=params)
    response.raise_for_status()
    data = response.json()
    print(f"Fetched {len(data)} race control records.")
    return pd.DataFrame(data)

# Sessions
def fetch_openf1_sessions(**kwargs):
    """
    Fetch session data from the OpenF1 API.
    """
    base_url = "https://api.openf1.org/v1/sessions"
    response = requests.get(base_url, params=kwargs)
    response.raise_for_status()
    data = response.json()
    print(f"Fetched {len(data)} session records with parameters {kwargs}.")
    return pd.DataFrame(data)

# Stints
def fetch_openf1_stints(session_key, tyre_age_at_start_threshold=3):
    """
    Fetch stints data from the OpenF1 API.
    """
    base_url = "https://api.openf1.org/v1/stints"
    params = {"session_key": session_key, "tyre_age_at_start>=": tyre_age_at_start_threshold}
    response = requests.get(base_url, params=params)
    response.raise_for_status()
    data = response.json()
    print(f"Fetched {len(data)} stint records.")
    return pd.DataFrame(data)

# Team Radio
def fetch_openf1_team_radio(session_key, driver_number):
    """
    Fetch team radio communications from the OpenF1 API.
    """
    base_url = "https://api.openf1.org/v1/team_radio"
    params = {"session_key": session_key, "driver_number": driver_number}
    response = requests.get(base_url, params=params)
    response.raise_for_status()
    data = response.json()
    print(f"Fetched {len(data)} team radio records for driver {driver_number}.")
    return pd.DataFrame(data)

if __name__ == "__main__":
    try:
        # Car telemetry
        car_data_df = fetch_openf1_car_data(driver_number=55, session_key=9159)
        safe_save(car_data_df, 'data/processed/openf1/openf1_car_data.csv', "Car data")
        
        # Driver information
        driver_info_df = fetch_openf1_drivers(driver_number=1, session_key=9158)
        safe_save(driver_info_df, 'data/processed/openf1/openf1_driver_info.csv', "Driver information")
        
        # Weather data
        weather_df = fetch_openf1_weather(meeting_key=1208)
        safe_save(weather_df, 'data/processed/openf1/openf1_weather.csv', "Weather data")
        
        # Intervals
        intervals_df = fetch_openf1_intervals(session_key=9165, interval_threshold=0.005)
        safe_save(intervals_df, 'data/processed/openf1/openf1_intervals.csv', "Intervals data")
        
        # Laps data
        laps_df = fetch_openf1_laps(session_key=9161, driver_number=63, lap_number=8)
        safe_save(laps_df, 'data/processed/openf1/openf1_laps.csv', "Laps data")
        
        # Location data
        location_df = fetch_openf1_location(session_key=9161, driver_number=81,
                                            date_from="2023-09-16T13:03:35.200",
                                            date_to="2023-09-16T13:03:35.800")
        safe_save(location_df, 'data/processed/openf1/openf1_location.csv', "Location data")
        
        # Meetings
        meetings_df = fetch_openf1_meetings(year=2023, country_name="Singapore")
        safe_save(meetings_df, 'data/processed/openf1/openf1_meetings.csv', "Meetings data")
        
        # Pit stops
        pit_df = fetch_openf1_pit(session_key=9158, pit_duration_threshold=31)
        safe_save(pit_df, 'data/processed/openf1/openf1_pit.csv', "Pit data")
        
        # Position data
        position_df = fetch_openf1_position(meeting_key=1217, driver_number=40, position_threshold=3)
        safe_save(position_df, 'data/processed/openf1/openf1_position.csv', "Position data")
        
        # Race control
        race_control_df = fetch_openf1_race_control(flag="BLACK AND WHITE", driver_number=1,
                                                    date_from="2023-01-01", date_to="2023-09-01")
        safe_save(race_control_df, 'data/processed/openf1/openf1_race_control.csv', "Race control data")
        
        # Sessions
        sessions_df = fetch_openf1_sessions(country_name="Belgium", session_name="Sprint", year=2023)
        safe_save(sessions_df, 'data/processed/openf1/openf1_sessions.csv', "Sessions data")
        
        # Stints
        stints_df = fetch_openf1_stints(session_key=9165, tyre_age_at_start_threshold=3)
        safe_save(stints_df, 'data/processed/openf1/openf1_stints.csv', "Stints data")
        
        # Team radio
        team_radio_df = fetch_openf1_team_radio(session_key=9158, driver_number=11)
        safe_save(team_radio_df, 'data/processed/openf1/openf1_team_radio.csv', "Team radio data")
        
    except Exception as e:
        print("Error fetching API data:", e)
