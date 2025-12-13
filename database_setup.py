import sqlite3

def init_db(db_name):
    conn = sqlite3.connect(db_name)
    cur = conn.cursor()
    
    cur.execute('''
        CREATE TABLE IF NOT EXISTS countries (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT UNIQUE NOT NULL,
            alpha_two_code TEXT
        )
    ''')
    
    cur.execute('''
        CREATE TABLE IF NOT EXISTS state_provinces (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT UNIQUE NOT NULL,
            country_id INTEGER,
            FOREIGN KEY (country_id) REFERENCES countries(id)
        )
    ''')
    
    cur.execute('''
        CREATE TABLE IF NOT EXISTS cities (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT UNIQUE NOT NULL
        )
    ''')
    
    cur.execute('''
        CREATE TABLE IF NOT EXISTS colleges (
            id INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            city_id INTEGER,
            state_id INTEGER,
            zip TEXT,
            ownership INTEGER,
            predominant_degree INTEGER,
            lat REAL,
            lon REAL,
            student_size INTEGER,
            in_state_tuition INTEGER,
            out_state_tuition INTEGER,
            academic_year_cost INTEGER,
            completion_rate REAL,
            earnings_10yr INTEGER,
            FOREIGN KEY (city_id) REFERENCES cities(id),
            FOREIGN KEY (state_id) REFERENCES state_provinces(id)
        )
    ''')
    
    cur.execute('''
        CREATE TABLE IF NOT EXISTS daily_weather (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            college_id INTEGER NOT NULL,
            date INTEGER NOT NULL,
            temp_max REAL,
            temp_min REAL,
            precip_sum REAL,
            FOREIGN KEY (college_id) REFERENCES colleges(id),
            UNIQUE(college_id, date)
        )
    ''')
    
    cur.execute('''
        CREATE TABLE IF NOT EXISTS universities_world (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT UNIQUE,
            country_id INTEGER,
            state_province_id INTEGER,
            web_page TEXT,
            domain TEXT,
            FOREIGN KEY (country_id) REFERENCES countries(id),
            FOREIGN KEY (state_province_id) REFERENCES state_provinces(id)
        )
    ''')
    
    conn.commit()
    print("Database initialized successfully!")
    print("Tables created: colleges, daily_weather, universities_world")
    print("Lookup tables: countries, state_provinces, cities")
    
    return conn


def get_or_create_country(conn, country_name, alpha_two_code=None):
    if not country_name:
        return None
    
    cur = conn.cursor()
    cur.execute("SELECT id FROM countries WHERE name = ?", (country_name,))
    result = cur.fetchone()
    
    if result:
        return result[0]
    else:
        cur.execute("INSERT INTO countries (name, alpha_two_code) VALUES (?, ?)", 
                   (country_name, alpha_two_code))
        conn.commit()
        return cur.lastrowid


def get_or_create_state_province(conn, state_name, country_id=None):
    if not state_name:
        return None
    
    cur = conn.cursor()
    cur.execute("SELECT id FROM state_provinces WHERE name = ?", (state_name,))
    result = cur.fetchone()
    
    if result:
        return result[0]
    else:
        cur.execute("INSERT INTO state_provinces (name, country_id) VALUES (?, ?)", 
                   (state_name, country_id))
        conn.commit()
        return cur.lastrowid


def get_or_create_city(conn, city_name):
    if not city_name:
        return None
    
    cur = conn.cursor()
    cur.execute("SELECT id FROM cities WHERE name = ?", (city_name,))
    result = cur.fetchone()
    
    if result:
        return result[0]
    else:
        cur.execute("INSERT INTO cities (name) VALUES (?)", (city_name,))
        conn.commit()
        return cur.lastrowid


def get_table_counts(conn):
    cur = conn.cursor()
    tables = ['colleges', 'daily_weather', 'universities_world', 
              'countries', 'state_provinces', 'cities']
    
    print("\n=== Current Database Status ===")
    for table in tables:
        cur.execute(f"SELECT COUNT(*) FROM {table}")
        count = cur.fetchone()[0]
        print(f"{table}: {count} rows")
    print("================================\n")


if __name__ == "__main__":
    conn = init_db("testing.db")
    get_table_counts(conn)
    
    cur = conn.cursor()
    cur.execute("SELECT id FROM countries WHERE name = 'United States'")
    if not cur.fetchone():
        cur.execute("INSERT INTO countries (name, alpha_two_code) VALUES ('United States', 'US')")
        conn.commit()
        print("Created United States entry in countries table")
    
    print("\nDatabase ready! You can now run:")
    print("  - python gather_colleges.py")
    print("  - python gather_universities.py")
    print("  - python gather_weather.py")
    
    conn.close()
    