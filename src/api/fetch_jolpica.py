import requests
import pandas as pd
from tenacity import retry, wait_fixed, stop_after_attempt

BASE_URL = "http://api.jolpi.ca/ergast/f1"

@retry(wait=wait_fixed(1), stop=stop_after_attempt(5))
def get_with_retry(url, params=None):
    response = requests.get(url, params=params)
    if response.status_code == 429:
        print("Rate limit exceeded; retrying...")
        response.raise_for_status()
    return response

def fetch_jolpica_seasons():
    url = f"{BASE_URL}/seasons"
    response = get_with_retry(url)
    data = response.json()
    return pd.json_normalize(data, record_path=['MRData', 'SeasonTable', 'Seasons'])

def fetch_jolpica_circuits():
    url = f"{BASE_URL}/circuits"
    response = get_with_retry(url)
    data = response.json()
    return pd.json_normalize(data, record_path=['MRData', 'CircuitTable', 'Circuits'])

def fetch_jolpica_races(season="2024"):
    url = f"{BASE_URL}/{season}/races"
    response = get_with_retry(url)
    data = response.json()
    return pd.json_normalize(data, record_path=['MRData', 'RaceTable', 'Races'])

def fetch_jolpica_constructors(season="2024"):
    url = f"{BASE_URL}/{season}/constructors"
    response = get_with_retry(url)
    data = response.json()
    return pd.json_normalize(data, record_path=['MRData', 'ConstructorTable', 'Constructors'])

def fetch_jolpica_drivers(season="2024"):
    url = f"{BASE_URL}/{season}/drivers"
    response = get_with_retry(url)
    data = response.json()
    return pd.json_normalize(data, record_path=['MRData', 'DriverTable', 'Drivers'])

def fetch_jolpica_results(season="2024"):
    url = f"{BASE_URL}/{season}/results"
    response = get_with_retry(url)
    data = response.json()
    # Note: Results are nested inside each Race.
    return pd.json_normalize(
        data,
        record_path=['MRData', 'RaceTable', 'Races', 'Results'],
        meta=['season', 'round', ['Circuit', 'circuitId'], ['Circuit', 'circuitName']],
        errors='ignore'
    )

def fetch_jolpica_sprint(season="2024"):
    url = f"{BASE_URL}/{season}/sprint"
    response = get_with_retry(url)
    data = response.json()
    return pd.json_normalize(data, record_path=['MRData', 'RaceTable', 'Races'])

def fetch_jolpica_qualifying(season="2024"):
    url = f"{BASE_URL}/{season}/qualifying"
    response = get_with_retry(url)
    data = response.json()
    return pd.json_normalize(data, record_path=['MRData', 'RaceTable', 'Races', 'QualifyingResults'])

def fetch_jolpica_pitstops(season="2024", round_num="1"):
    url = f"{BASE_URL}/{season}/{round_num}/pitstops"
    response = get_with_retry(url)
    data = response.json()
    return pd.json_normalize(data, record_path=['MRData', 'RaceTable', 'Races', 'PitStops'])

def fetch_jolpica_laps(season="2024", round_num="1"):
    url = f"{BASE_URL}/{season}/{round_num}/laps"
    response = get_with_retry(url)
    data = response.json()
    return pd.json_normalize(data, record_path=['MRData', 'RaceTable', 'Races', 'Laps'])

def fetch_jolpica_driverstandings(season="2024"):
    url = f"{BASE_URL}/{season}/driverstandings"
    response = get_with_retry(url)
    data = response.json()
    return pd.json_normalize(data, record_path=['MRData', 'StandingsTable', 'StandingsLists', 'DriverStandings'])

def fetch_jolpica_constructorstandings(season="2024"):
    url = f"{BASE_URL}/{season}/constructorstandings"
    response = get_with_retry(url)
    data = response.json()
    return pd.json_normalize(data, record_path=['MRData', 'StandingsTable', 'StandingsLists', 'ConstructorStandings'])

def fetch_jolpica_status():
    url = f"{BASE_URL}/status"
    response = get_with_retry(url)
    data = response.json()
    try:
        return pd.json_normalize(data, record_path=['MRData', 'StatusTable', 'Statuses'])
    except KeyError:
        print("Warning: 'Statuses' key not found in response.")
        return pd.DataFrame()

if __name__ == "__main__":
    try:
        seasons_df = fetch_jolpica_seasons()
        print("Seasons fetched:", len(seasons_df))
        
        circuits_df = fetch_jolpica_circuits()
        print("Circuits fetched:", len(circuits_df))
        
        races_df = fetch_jolpica_races()
        print("Races fetched:", len(races_df))
        
        constructors_df = fetch_jolpica_constructors()
        print("Constructors fetched:", len(constructors_df))
        
        drivers_df = fetch_jolpica_drivers()
        print("Drivers fetched:", len(drivers_df))
        
        results_df = fetch_jolpica_results()
        print("Results fetched:", len(results_df))
        
        sprint_df = fetch_jolpica_sprint()
        print("Sprint results fetched:", len(sprint_df))
        
        qualifying_df = fetch_jolpica_qualifying()
        print("Qualifying fetched:", len(qualifying_df))
        
        pitstops_df = fetch_jolpica_pitstops()
        print("Pitstops fetched:", len(pitstops_df))
        
        laps_df = fetch_jolpica_laps()
        print("Laps fetched:", len(laps_df))
        
        driver_standings_df = fetch_jolpica_driverstandings()
        print("Driver Standings fetched:", len(driver_standings_df))
        
        constructor_standings_df = fetch_jolpica_constructorstandings()
        print("Constructor Standings fetched:", len(constructor_standings_df))
        
        status_df = fetch_jolpica_status()
        print("Status fetched:", len(status_df))
        
    except Exception as e:
        print("Error fetching Jolpica API data:", e)
