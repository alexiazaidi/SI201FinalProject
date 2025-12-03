import sqlite3

def init_db(db_name):
    """
    Creates (or opens) the SQLite database and creates all required tables.
    """
    conn = sqlite3.connect(db_name)
    cur = conn.cursor()
    
    # TABLE 1: us_colleges
    cur.execute('''
        CREATE TABLE IF NOT EXISTS us_colleges (
            id INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            city TEXT,
            state TEXT,
            zip TEXT,
            ownership INTEGER,
            predominant_degree INTEGER,
            lat REAL,
            lon REAL,
            student_size INTEGER
        )
    ''')
    
    # TABLE 2: college_financials (shares id with us_colleges)
    cur.execute('''
        CREATE TABLE IF NOT EXISTS college_financials (
            id INTEGER PRIMARY KEY,
            in_state_tuition INTEGER,
            out_state_tuition INTEGER,
            academic_year_cost INTEGER,
            completion_rate REAL,
            earnings_10yr INTEGER,
            FOREIGN KEY (id) REFERENCES us_colleges(id)
        )
    ''')
    
    # TABLE 3: daily_weather
    cur.execute('''
        CREATE TABLE IF NOT EXISTS daily_weather (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            college_id INTEGER NOT NULL,
            date TEXT NOT NULL,
            temp_max REAL,
            temp_min REAL,
            precip_sum REAL,
            FOREIGN KEY (college_id) REFERENCES us_colleges(id),
            UNIQUE(college_id, date)
        )
    ''')
    
    # TABLE 4: universities_world
    cur.execute('''
        CREATE TABLE IF NOT EXISTS universities_world (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT UNIQUE,
            country TEXT,
            alpha_two_code TEXT,
            state_province TEXT,
            web_page TEXT,
            domain TEXT
        )
    ''')
    
    conn.commit()
    print("Database initialized successfully!")
    print("Tables created: us_colleges, college_financials, daily_weather, universities_world")
    
    return conn


def get_table_counts(conn):
    """
    Utility function to check how many rows are in each table.
    """
    cur = conn.cursor()
    
    tables = ['us_colleges', 'college_financials', 'daily_weather', 'universities_world']
    
    print("\n=== Current Database Status ===")
    for table in tables:
        cur.execute(f"SELECT COUNT(*) FROM {table}")
        count = cur.fetchone()[0]
        print(f"{table}: {count} rows")
    print("================================\n")


if __name__ == "__main__":
    conn = init_db("project.db")
    get_table_counts(conn)
    conn.close()