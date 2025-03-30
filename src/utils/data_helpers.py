import pandas as pd

def clean_circuits(df):
    """
    Clean and standardize the circuits dataset.
    Standardized columns: circuit_id, circuit_ref, name, location, country, lat, lng, alt, url
    """
    df.columns = [col.strip().lower() for col in df.columns]
    df.rename(columns={
        'circuitid': 'circuit_id',
        'circuitref': 'circuit_ref'
    }, inplace=True)
    df['lat'] = pd.to_numeric(df['lat'], errors='coerce')
    df['lng'] = pd.to_numeric(df['lng'], errors='coerce')
    df['alt'] = pd.to_numeric(df['alt'], errors='coerce')
    return df

def clean_constructor_results(df):
    """
    Clean and standardize the constructor_results dataset.
    Standardized columns: constructor_results_id, race_id, constructor_id, points, status
    """
    df.columns = [col.strip().lower() for col in df.columns]
    df.rename(columns={
        'constructorresultsid': 'constructor_results_id',
        'raceid': 'race_id',
        'constructorid': 'constructor_id'
    }, inplace=True)
    df['points'] = pd.to_numeric(df['points'], errors='coerce')
    return df

def clean_constructor_standings(df):
    """
    Clean and standardize the constructor_standings dataset.
    Standardized columns: constructor_standings_id, race_id, constructor_id, points, position, position_text, wins
    """
    df.columns = [col.strip().lower() for col in df.columns]
    df.rename(columns={
        'constructorstandingsid': 'constructor_standings_id',
        'raceid': 'race_id',
        'constructorid': 'constructor_id',
        'positiontext': 'position_text'
    }, inplace=True)
    df['points'] = pd.to_numeric(df['points'], errors='coerce')
    df['position'] = pd.to_numeric(df['position'], errors='coerce')
    df['wins'] = pd.to_numeric(df['wins'], errors='coerce')
    return df

def clean_constructors(df):
    """
    Clean and standardize the constructors dataset.
    Standardized columns: constructor_id, constructor_ref, name, nationality, url
    """
    df.columns = [col.strip().lower() for col in df.columns]
    df.rename(columns={
        'constructorid': 'constructor_id',
        'constructorref': 'constructor_ref'
    }, inplace=True)
    return df

def clean_driver_standings(df):
    """
    Clean and standardize the driver_standings dataset.
    Standardized columns: driver_standings_id, race_id, driver_id, points, position, position_text, wins
    """
    df.columns = [col.strip().lower() for col in df.columns]
    df.rename(columns={
        'driverstandingsid': 'driver_standings_id',
        'raceid': 'race_id',
        'driverid': 'driver_id',
        'positiontext': 'position_text'
    }, inplace=True)
    df['points'] = pd.to_numeric(df['points'], errors='coerce')
    df['position'] = pd.to_numeric(df['position'], errors='coerce')
    df['wins'] = pd.to_numeric(df['wins'], errors='coerce')
    return df

def clean_drivers(df):
    """
    Clean and standardize the drivers dataset.
    Standardized columns: driver_id, driver_ref, number, code, forename, surname, dob, nationality, url
    """
    df.columns = [col.strip().lower() for col in df.columns]
    df.rename(columns={
        'driverid': 'driver_id',
        'driverref': 'driver_ref'
    }, inplace=True)
    # Convert date of birth to datetime
    df['dob'] = pd.to_datetime(df['dob'], errors='coerce')
    return df

def clean_lap_times(df):
    """
    Clean and standardize the lap_times dataset.
    Standardized columns: race_id, driver_id, lap, position, time, milliseconds
    """
    df.columns = [col.strip().lower() for col in df.columns]
    df.rename(columns={
        'raceid': 'race_id',
        'driverid': 'driver_id'
    }, inplace=True)
    df['lap'] = pd.to_numeric(df['lap'], errors='coerce')
    df['position'] = pd.to_numeric(df['position'], errors='coerce')
    df['milliseconds'] = pd.to_numeric(df['milliseconds'], errors='coerce')
    return df

def clean_pit_stops(df):
    """
    Clean and standardize the pit_stops dataset.
    Standardized columns: race_id, driver_id, stop, lap, time, duration, milliseconds
    """
    df.columns = [col.strip().lower() for col in df.columns]
    df.rename(columns={
        'raceid': 'race_id',
        'driverid': 'driver_id'
    }, inplace=True)
    df['lap'] = pd.to_numeric(df['lap'], errors='coerce')
    df['duration'] = pd.to_numeric(df['duration'], errors='coerce')
    df['milliseconds'] = pd.to_numeric(df['milliseconds'], errors='coerce')
    return df

def clean_qualifying(df):
    """
    Clean and standardize the qualifying dataset.
    Standardized columns: qualify_id, race_id, driver_id, constructor_id, number, position, q1, q2, q3
    """
    df.columns = [col.strip().lower() for col in df.columns]
    df.rename(columns={
        'qualifyid': 'qualify_id',
        'raceid': 'race_id',
        'driverid': 'driver_id',
        'constructorid': 'constructor_id'
    }, inplace=True)
    df['number'] = pd.to_numeric(df['number'], errors='coerce')
    df['position'] = pd.to_numeric(df['position'], errors='coerce')
    return df

def clean_races(df):
    """
    Clean and standardize the races dataset.
    Standardized columns: race_id, year, round, circuit_id, name, date, time, url, 
    fp1_date, fp1_time, fp2_date, fp2_time, fp3_date, fp3_time, quali_date, quali_time, sprint_date, sprint_time.
    Converts date and session date fields to datetime.
    """
    df.columns = [col.strip().lower() for col in df.columns]
    df.rename(columns={
        'raceid': 'race_id',
        'circuitid': 'circuit_id'
    }, inplace=True)
    if 'date' in df.columns:
        df['date'] = pd.to_datetime(df['date'], errors='coerce')
    # Convert session date fields to datetime if they exist
    date_cols = ['fp1_date', 'fp2_date', 'fp3_date', 'quali_date', 'sprint_date']
    for col in date_cols:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors='coerce')
    return df

def clean_results(df):
    """
    Clean and standardize the results dataset.
    Standardized columns: result_id, race_id, driver_id, constructor_id, number, grid, position, 
    position_text, position_order, points, laps, time, milliseconds, fastest_lap, rank, fastest_lap_time, 
    fastest_lap_speed, status_id.
    """
    df.columns = [col.strip().lower() for col in df.columns]
    df.rename(columns={
        'resultid': 'result_id',
        'raceid': 'race_id',
        'driverid': 'driver_id',
        'constructorid': 'constructor_id',
        'positiontext': 'position_text',
        'positionorder': 'position_order',
        'fastestlap': 'fastest_lap',
        'fastestlaptime': 'fastest_lap_time',
        'fastestlapspeed': 'fastest_lap_speed',
        'statusid': 'status_id'
    }, inplace=True)
    df['number'] = pd.to_numeric(df['number'], errors='coerce')
    df['grid'] = pd.to_numeric(df['grid'], errors='coerce')
    df['position'] = pd.to_numeric(df['position'], errors='coerce')
    df['points'] = pd.to_numeric(df['points'], errors='coerce')
    df['laps'] = pd.to_numeric(df['laps'], errors='coerce')
    df['milliseconds'] = pd.to_numeric(df['milliseconds'], errors='coerce')
    df['rank'] = pd.to_numeric(df['rank'], errors='coerce')
    return df

def clean_seasons(df):
    """
    Clean and standardize the seasons dataset.
    Standardized columns: year, url.
    """
    df.columns = [col.strip().lower() for col in df.columns]
    return df

def clean_sprint_results(df):
    """
    Clean and standardize the sprint_results dataset.
    Standardized columns: result_id, race_id, driver_id, constructor_id, number, grid, position, 
    position_text, position_order, points, laps, time, milliseconds, fastest_lap, fastest_lap_time, status_id.
    """
    df.columns = [col.strip().lower() for col in df.columns]
    df.rename(columns={
        'resultid': 'result_id',
        'raceid': 'race_id',
        'driverid': 'driver_id',
        'constructorid': 'constructor_id',
        'positiontext': 'position_text',
        'positionorder': 'position_order'
    }, inplace=True)
    df['number'] = pd.to_numeric(df['number'], errors='coerce')
    df['grid'] = pd.to_numeric(df['grid'], errors='coerce')
    df['position'] = pd.to_numeric(df['position'], errors='coerce')
    df['points'] = pd.to_numeric(df['points'], errors='coerce')
    df['laps'] = pd.to_numeric(df['laps'], errors='coerce')
    df['milliseconds'] = pd.to_numeric(df['milliseconds'], errors='coerce')
    return df

def clean_status(df):
    """
    Clean and standardize the status dataset.
    Standardized columns: status_id, status.
    """
    df.columns = [col.strip().lower() for col in df.columns]
    df.rename(columns={
        'statusid': 'status_id'
    }, inplace=True)
    return df
