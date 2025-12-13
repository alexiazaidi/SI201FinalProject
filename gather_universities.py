import requests
from requests.exceptions import ChunkedEncodingError, RequestException

import sqlite3
from database_setup import init_db, get_table_counts

BASE_URL = "http://universities.hipolabs.com/search"
DATABASE_NAME = "testing.db"
MAX_INSERT_PER_RUN = 25

# Collecting from multiple countries to get diversity
COUNTRIES_TO_FETCH = [
    "United States",
    "Canada", 
    "United Kingdom",
    "Germany",
    "France",
    "Australia",
    "Japan",
    "China",
    "India",
    "Brazil"
]

def fetch_universities_by_country(country):
    """
    Fetches all universities for a given country from the Hipo API.
    
    Parameters:
        country (str): Country name (e.g., "United States", "Canada")
    
    Returns:
        list: List of dictionaries with university data
    """
    params = {"country": country}
    print(f"Fetching universities from {country}...")
    
    try:
        # timeout just avoids hanging forever if the API is slow
        response = requests.get(BASE_URL, params=params, timeout=20)
        response.raise_for_status()
    except ChunkedEncodingError as e:
        print(f"Network error while fetching {country}: {e}")
        print("Skipping this country for now.")
        return []
    except RequestException as e:
        # Catches any other request-related error
        print(f"Request failed for {country}: {e}")
        return []

    try:
        universities = response.json()
    except ValueError:
        print(f"Error: Response from {country} was not valid JSON.")
        return []

    print(f"Received {len(universities)} universities from {country}")
    return universities


def store_universities(conn, uni_list):
    """
    Stores university data into the database, respecting the 25-item limit.
    
    Parameters:
        conn: SQLite connection object
        uni_list (list): List of university dictionaries from API
    
    Returns:
        int: Number of new rows actually inserted
    """
    cur = conn.cursor()
    inserted_count = 0
    
    for uni in uni_list:
        if inserted_count >= MAX_INSERT_PER_RUN:
            print(f"Reached {MAX_INSERT_PER_RUN} insertions limit for this run.")
            break
        
        name = uni.get("name")
        if not name:
            continue
        
        # Check if this university already exists (by name)
        cur.execute("SELECT 1 FROM universities_world WHERE name = ?", (name,))
        if cur.fetchone() is not None:
            continue  # Skip duplicate
        
        # Extract fields
        country = uni.get("country")
        alpha_two_code = uni.get("alpha_two_code")
        state_province = uni.get("state-province")  # Note: API uses hyphen
        
        # web_pages and domains are lists - take first element
        web_pages = uni.get("web_pages", [])
        web_page = web_pages[0] if web_pages else None
        
        domains = uni.get("domains", [])
        domain = domains[0] if domains else None
        
        # Insert into database
        try:
            cur.execute('''
                INSERT INTO universities_world 
                (name, country, alpha_two_code, state_province, web_page, domain)
                VALUES (?, ?, ?, ?, ?, ?)
            ''', (name, country, alpha_two_code, state_province, web_page, domain))
            inserted_count += 1
        except sqlite3.IntegrityError:
            # UNIQUE constraint violated - skip this duplicate
            continue
    
    conn.commit()
    print(f"Successfully inserted {inserted_count} new universities into database.")
    return inserted_count


def get_current_count(conn):
    """Returns current number of universities in database."""
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM universities_world")
    return cur.fetchone()[0]


def main():
    """
    Main execution function.
    
    STRATEGY:
    Since different countries have different numbers of universities,
    we cycle through countries until we've collected enough data.
    """
    conn = init_db(DATABASE_NAME)
    
    print("=== BEFORE DATA COLLECTION ===")
    get_table_counts(conn)
    
    current_count = get_current_count(conn)
    
    if current_count >= 100:
        print("✓ Already have 100+ universities. No need to fetch more.")
        conn.close()
        return
    
    # Determine which country to fetch based on progress
    # This creates variety in our data
    country_index = (current_count // 25) % len(COUNTRIES_TO_FETCH)
    country = COUNTRIES_TO_FETCH[country_index]
    
    # Fetch and store
    universities = fetch_universities_by_country(country)
    
    if universities:
        inserted = store_universities(conn, universities)
    else:
        print(f"No data from {country}, trying next country...")
        # Try next country
        next_country = COUNTRIES_TO_FETCH[(country_index + 1) % len(COUNTRIES_TO_FETCH)]
        universities = fetch_universities_by_country(next_country)
        if universities:
            store_universities(conn, universities)
    
    print("\n=== AFTER DATA COLLECTION ===")
    get_table_counts(conn)
    
    total = get_current_count(conn)
    if total >= 100:
        print("✓ SUCCESS: You have collected 100+ universities!")
    else:
        runs_remaining = (100 - total + 24) // 25
        print(f"Progress: {total}/100 universities. Run this script {runs_remaining} more time(s).")
    
    conn.close()


if __name__ == "__main__":
    main()
