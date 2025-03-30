import sqlite3

DB_PATH = 'src/db/jolpica_f1.db'

def create_base_team_table(conn):
    conn.execute('''
        CREATE TABLE IF NOT EXISTS BaseTeam (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT UNIQUE
        )
    ''')

def create_season_table(conn):
    """
    Creates the Season table.
    Fields:
      - id: auto-increment PK.
      - championship_system_id: INTEGER (FK to ChampionshipSystem.id), nullable.
      - year: INTEGER NOT NULL UNIQUE.
      - wikipedia: TEXT.
    """
    conn.execute('''
        CREATE TABLE IF NOT EXISTS season (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            championship_system_id INTEGER,
            year INTEGER NOT NULL UNIQUE,
            wikipedia TEXT,
            FOREIGN KEY (championship_system_id) REFERENCES ChampionshipSystem(id)
        )
    ''')

def create_team_table(conn):
    """
    Creates the Team table.
    Fields:
      - id: auto-increment PK.
      - base_team_id: INTEGER (FK to BaseTeam.id), nullable.
      - reference: TEXT UNIQUE, nullable.
      - name: TEXT NOT NULL.
      - nationality: TEXT,
      - country_code: TEXT,
      - wikipedia: TEXT.
    """
    conn.execute('''
        CREATE TABLE IF NOT EXISTS team (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            base_team_id INTEGER,
            reference TEXT UNIQUE,
            name TEXT NOT NULL,
            nationality TEXT,
            country_code TEXT,
            wikipedia TEXT,
            FOREIGN KEY (base_team_id) REFERENCES BaseTeam(id)
        )
    ''')

def create_championship_system_table(conn):
    conn.execute('''
        CREATE TABLE IF NOT EXISTS ChampionshipSystem (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            reference TEXT UNIQUE,
            name TEXT,
            eligibility INTEGER NOT NULL,
            driver_season_split INTEGER NOT NULL,
            driver_best_results INTEGER NOT NULL,
            team_season_split INTEGER NOT NULL,
            team_best_results INTEGER NOT NULL
        )
    ''')

def create_championship_adjustment_table(conn):
    conn.execute('''
        CREATE TABLE IF NOT EXISTS ChampionshipAdjustment (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            season_id INTEGER NOT NULL,
            driver_id INTEGER,
            team_id INTEGER,
            adjustment INTEGER NOT NULL DEFAULT 0,
            points REAL,
            FOREIGN KEY (season_id) REFERENCES season(id),
            FOREIGN KEY (driver_id) REFERENCES driver(id),
            FOREIGN KEY (team_id) REFERENCES team(id)
        )
    ''')

def create_circuits_table(conn):
    conn.execute('''
        CREATE TABLE IF NOT EXISTS circuits (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            reference TEXT UNIQUE NOT NULL,
            name TEXT NOT NULL,
            locality TEXT,
            country TEXT,
            country_code TEXT,
            latitude REAL,
            longitude REAL,
            altitude REAL,
            wikipedia TEXT
        )
    ''')

def create_driver_table(conn):
    conn.execute('''
        CREATE TABLE IF NOT EXISTS driver (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            reference TEXT UNIQUE,
            forename TEXT NOT NULL,
            surname TEXT NOT NULL,
            abbreviation TEXT,
            nationality TEXT,
            country_code TEXT,
            permanent_car_number INTEGER,
            date_of_birth DATE,
            wikipedia TEXT
        )
    ''')

def create_driver_championship_table(conn):
    conn.execute('''
        CREATE TABLE IF NOT EXISTS DriverChampionship (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            session_id INTEGER NOT NULL,
            driver_id INTEGER NOT NULL,
            year INTEGER NOT NULL,
            round_number INTEGER NOT NULL,
            session_number INTEGER NOT NULL,
            position INTEGER,
            points REAL NOT NULL,
            win_count INTEGER NOT NULL,
            highest_finish INTEGER,
            is_eligible BOOLEAN NOT NULL DEFAULT 0,
            adjustment_type INTEGER NOT NULL DEFAULT 0,
            season_id INTEGER,
            round_id INTEGER,
            FOREIGN KEY (session_id) REFERENCES session(id),
            FOREIGN KEY (driver_id) REFERENCES driver(id),
            FOREIGN KEY (season_id) REFERENCES season(id),
            FOREIGN KEY (round_id) REFERENCES round(id)
        )
    ''')

def create_lap_table(conn):
    conn.execute('''
        CREATE TABLE IF NOT EXISTS lap (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            session_entry_id INTEGER NOT NULL,
            number INTEGER,
            position INTEGER,
            time TEXT,
            average_speed REAL,
            is_entry_fastest_lap BOOLEAN NOT NULL DEFAULT 0,
            is_deleted BOOLEAN NOT NULL DEFAULT 0,
            FOREIGN KEY (session_entry_id) REFERENCES SessionEntry(id)
        )
    ''')

def create_penalty_table(conn):
    conn.execute('''
        CREATE TABLE IF NOT EXISTS penalty (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            earned_id INTEGER,
            served_id INTEGER,
            license_points INTEGER,
            position INTEGER,
            time TEXT,
            is_time_served_in_pit BOOLEAN,
            FOREIGN KEY (earned_id) REFERENCES SessionEntry(id),
            FOREIGN KEY (served_id) REFERENCES SessionEntry(id)
        )
    ''')

def create_pitstop_table(conn):
    conn.execute('''
        CREATE TABLE IF NOT EXISTS PitStop (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            session_entry_id INTEGER NOT NULL,
            lap_id INTEGER UNIQUE,
            number INTEGER,
            duration TEXT,
            local_timestamp TEXT,
            FOREIGN KEY (session_entry_id) REFERENCES SessionEntry(id),
            FOREIGN KEY (lap_id) REFERENCES lap(id)
        )
    ''')

def create_pointsystem_table(conn):
    conn.execute('''
        CREATE TABLE IF NOT EXISTS PointSystem (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            reference TEXT UNIQUE,
            name TEXT,
            driver_position_points INTEGER NOT NULL,
            driver_fastest_lap INTEGER NOT NULL DEFAULT 0,
            team_position_points INTEGER NOT NULL,
            team_fastest_lap INTEGER NOT NULL DEFAULT 0,
            partial INTEGER NOT NULL DEFAULT 0,
            shared_drive INTEGER NOT NULL DEFAULT 0,
            is_double_points BOOLEAN NOT NULL DEFAULT 0
        )
    ''')

def create_round_table(conn):
    conn.execute('''
        CREATE TABLE IF NOT EXISTS round (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            season_id INTEGER NOT NULL,
            circuit_id INTEGER NOT NULL,
            number INTEGER,
            name TEXT,
            date DATE,
            race_number INTEGER,
            wikipedia TEXT,
            is_cancelled BOOLEAN NOT NULL DEFAULT 0,
            FOREIGN KEY (season_id) REFERENCES season(id),
            FOREIGN KEY (circuit_id) REFERENCES circuits(id)
        )
    ''')

def create_roundentry_table(conn):
    conn.execute('''
        CREATE TABLE IF NOT EXISTS RoundEntry (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            round_id INTEGER NOT NULL,
            team_driver_id INTEGER NOT NULL,
            car_number INTEGER,
            FOREIGN KEY (round_id) REFERENCES round(id),
            FOREIGN KEY (team_driver_id) REFERENCES TeamDriver(id)
        )
    ''')

def create_session_table(conn):
    conn.execute('''
        CREATE TABLE IF NOT EXISTS session (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            round_id INTEGER NOT NULL,
            number INTEGER,
            point_system_id INTEGER NOT NULL DEFAULT 1,
            type TEXT NOT NULL,
            date DATE,
            time TEXT,
            scheduled_laps INTEGER,
            is_cancelled BOOLEAN NOT NULL DEFAULT 0,
            FOREIGN KEY (round_id) REFERENCES round(id),
            FOREIGN KEY (point_system_id) REFERENCES PointSystem(id)
        )
    ''')

def create_sessionentry_table(conn):
    conn.execute('''
        CREATE TABLE IF NOT EXISTS SessionEntry (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            session_id INTEGER NOT NULL,
            round_entry_id INTEGER NOT NULL,
            position INTEGER,
            is_classified BOOLEAN,
            status INTEGER,
            detail TEXT,
            points REAL,
            is_eligible_for_points BOOLEAN NOT NULL DEFAULT 1,
            grid INTEGER,
            time TEXT,
            fastest_lap_rank INTEGER,
            laps_completed INTEGER,
            FOREIGN KEY (session_id) REFERENCES session(id),
            FOREIGN KEY (round_entry_id) REFERENCES RoundEntry(id)
        )
    ''')

def create_team_championship_table(conn):
    conn.execute('''
        CREATE TABLE IF NOT EXISTS TeamChampionship (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            session_id INTEGER NOT NULL,
            team_id INTEGER NOT NULL,
            year INTEGER NOT NULL,
            round_number INTEGER NOT NULL,
            session_number INTEGER NOT NULL,
            position INTEGER,
            points REAL NOT NULL,
            win_count INTEGER NOT NULL,
            highest_finish INTEGER,
            is_eligible BOOLEAN NOT NULL DEFAULT 0,
            adjustment_type INTEGER NOT NULL DEFAULT 0,
            season_id INTEGER,
            round_id INTEGER,
            FOREIGN KEY (session_id) REFERENCES session(id),
            FOREIGN KEY (team_id) REFERENCES team(id),
            FOREIGN KEY (season_id) REFERENCES season(id),
            FOREIGN KEY (round_id) REFERENCES round(id)
        )
    ''')

def create_teamdriver_table(conn):
    conn.execute('''
        CREATE TABLE IF NOT EXISTS TeamDriver (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            team_id INTEGER NOT NULL,
            driver_id INTEGER NOT NULL,
            season_id INTEGER NOT NULL,
            role INTEGER,
            FOREIGN KEY (team_id) REFERENCES team(id),
            FOREIGN KEY (driver_id) REFERENCES driver(id),
            FOREIGN KEY (season_id) REFERENCES season(id)
        )
    ''')

def create_driver_championship_tables():
    # Create all tables in proper order
    conn = sqlite3.connect(DB_PATH)
    # Enable foreign key constraints
    conn.execute("PRAGMA foreign_keys = ON;")
    
    create_base_team_table(conn)
    create_season_table(conn)
    create_team_table(conn)
    create_championship_system_table(conn)
    create_championship_adjustment_table(conn)
    create_circuits_table(conn)
    create_driver_table(conn)
    create_driver_championship_table(conn)
    create_lap_table(conn)
    create_penalty_table(conn)
    create_pitstop_table(conn)
    create_pointsystem_table(conn)
    create_round_table(conn)
    create_roundentry_table(conn)
    create_session_table(conn)
    create_sessionentry_table(conn)
    create_team_championship_table(conn)
    create_teamdriver_table(conn)
    
    conn.commit()
    conn.close()

if __name__ == "__main__":
    create_driver_championship_tables()
    print("All tables created in", DB_PATH)
