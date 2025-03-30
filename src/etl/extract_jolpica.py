import pandas as pd
from src.api.fetch_jolpica import (
    fetch_jolpica_circuits,
    fetch_jolpica_constructors,
    fetch_jolpica_seasons,
    fetch_jolpica_races,
    fetch_jolpica_results,
    fetch_jolpica_driverstandings,
    fetch_jolpica_laps,
    fetch_jolpica_pitstops,
    fetch_jolpica_qualifying,
    fetch_jolpica_sprint,
    fetch_jolpica_status,
    fetch_jolpica_drivers
)

def extract_circuits():
    # Extraction: Fetch circuits raw JSON data.
    raw_json = fetch_jolpica_circuits().to_dict(orient='records')  # if your function returns a DataFrame, convert to list of dicts
    # Normalization: Flatten the nested structure using pd.json_normalize.
    circuits_df = pd.json_normalize(raw_json, record_path=['CircuitTable', 'Circuits'])
    return circuits_df

def extract_constructors():
    # Similarly, for constructors endpoint.
    raw_json = fetch_jolpica_constructors(season="2024").to_dict(orient='records')
    constructors_df = pd.json_normalize(raw_json, record_path=['ConstructorTable', 'Constructors'])
    return constructors_df

def extract_seasons():
    raw_json = fetch_jolpica_seasons().to_dict(orient='records')
    seasons_df = pd.json_normalize(raw_json, record_path=['SeasonTable', 'Seasons'])
    return seasons_df

def extract_races():
    raw_json = fetch_jolpica_races(season="2024").to_dict(orient='records')
    races_df = pd.json_normalize(raw_json, record_path=['RaceTable', 'Races'])
    return races_df

def extract_results():
    raw_json = fetch_jolpica_results(season="2024").to_dict(orient='records')
    # Note: Results are nested within each Race.
    results_df = pd.json_normalize(raw_json, record_path=['RaceTable', 'Races', 'Results'])
    return results_df

def extract_driver_standings():
    raw_json = fetch_jolpica_driverstandings(season="2024").to_dict(orient='records')
    # Normalization path may vary depending on how the JSON is structured.
    standings_df = pd.json_normalize(raw_json, record_path=['StandingsTable', 'StandingsLists', 'DriverStandings'])
    return standings_df

def extract_laps(season="2024", round_num="1"):
    raw_json = fetch_jolpica_laps(season=season, round_num=round_num).to_dict(orient='records')
    laps_df = pd.json_normalize(raw_json, record_path=['RaceTable', 'Races', 'Laps'])
    return laps_df

def extract_pitstops(season="2024", round_num="1"):
    raw_json = fetch_jolpica_pitstops(season=season, round_num=round_num).to_dict(orient='records')
    pitstops_df = pd.json_normalize(raw_json, record_path=['RaceTable', 'Races', 'PitStops'])
    return pitstops_df

def extract_qualifying(season="2024"):
    raw_json = fetch_jolpica_qualifying(season=season).to_dict(orient='records')
    qualifying_df = pd.json_normalize(raw_json, record_path=['RaceTable', 'Races', 'QualifyingResults'])
    return qualifying_df

def extract_sprint(season="2024"):
    raw_json = fetch_jolpica_sprint(season=season).to_dict(orient='records')
    sprint_df = pd.json_normalize(raw_json, record_path=['RaceTable', 'Races'])
    return sprint_df

def extract_status():
    raw_json = fetch_jolpica_status().to_dict(orient='records')
    # Assuming the status records are nested under a key (adjust if necessary)
    status_df = pd.json_normalize(raw_json)
    return status_df

def extract_drivers():
    raw_json = fetch_jolpica_drivers(season="2024").to_dict(orient='records')
    drivers_df = pd.json_normalize(raw_json, record_path=['DriverTable', 'Drivers'])
    return drivers_df

if __name__ == "__main__":
    # Example: Extract and print record counts for each endpoint
    circuits_df = extract_circuits()
    print("Circuits:", len(circuits_df))
    
    constructors_df = extract_constructors()
    print("Constructors:", len(constructors_df))
    
    seasons_df = extract_seasons()
    print("Seasons:", len(seasons_df))
    
    races_df = extract_races()
    print("Races:", len(races_df))
    
    results_df = extract_results()
    print("Results:", len(results_df))
    
    driver_standings_df = extract_driver_standings()
    print("Driver Standings:", len(driver_standings_df))
    
    laps_df = extract_laps(season="2024", round_num="1")
    print("Laps:", len(laps_df))
    
    pitstops_df = extract_pitstops(season="2024", round_num="1")
    print("Pitstops:", len(pitstops_df))
    
    qualifying_df = extract_qualifying(season="2024")
    print("Qualifying:", len(qualifying_df))
    
    sprint_df = extract_sprint(season="2024")
    print("Sprint:", len(sprint_df))
    
    status_df = extract_status()
    print("Status:", len(status_df))
    
    drivers_df = extract_drivers()
    print("Drivers:", len(drivers_df))