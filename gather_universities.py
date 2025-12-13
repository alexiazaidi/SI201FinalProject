import requests
from requests.exceptions import ChunkedEncodingError, RequestException
import sqlite3
from database_setup import init_db, get_table_counts, get_or_create_country, get_or_create_state_province

BASE_URL = "http://universities.hipolabs.com/search"
DATABASE_NAME = "testing.db"
MAX_INSERT_PER_RUN = 25

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
    params = {"country": country}
    print(f"Fetching universities from {country}...")
    
    try:
        response = requests.get(BASE_URL, params=params, timeout=20)
        response.raise_for_status()
    except ChunkedEncodingError as e:
        print(f"Network error while fetching {country}: {e}")
        print("Skipping this country for now.")
        return []
    except RequestException as e:
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
    cur = conn.cursor()
    inserted_count = 0
    
    for uni in uni_list:
        if inserted_count >= MAX_INSERT_PER_RUN:
            print(f"Reached {MAX_INSERT_PER_RUN} insertions limit for this run.")
            break
        
        name = uni.get("name")
        if not name:
            continue
        
        cur.execute("SELECT 1 FROM universities_world WHERE name = ?", (name,))
        if cur.fetchone() is not None:
            continue
        
        country_string = uni.get("country")
        alpha_two_code = uni.get("alpha_two_code")
        state_province_string = uni.get("state-province")
        
        web_pages = uni.get("web_pages", [])
        web_page = web_pages[0] if web_pages else None
        
        domains = uni.get("domains", [])
        domain = domains[0] if domains else None
        
        country_id = get_or_create_country(conn, country_string, alpha_two_code)
        state_province_id = get_or_create_state_province(conn, state_province_string, country_id)
        
        try:
            cur.execute('''
                INSERT INTO universities_world 
                (name, country_id, state_province_id, web_page, domain)
                VALUES (?, ?, ?, ?, ?)
            ''', (name, country_id, state_province_id, web_page, domain))
            inserted_count += 1
        except sqlite3.IntegrityError:
            continue
    
    conn.commit()
    print(f"Successfully inserted {inserted_count} new universities into database.")
    return inserted_count


def get_current_count(conn):
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM universities_world")
    return cur.fetchone()[0]


def main():
    conn = init_db(DATABASE_NAME)
    
    print("=== BEFORE DATA COLLECTION ===")
    get_table_counts(conn)
    
    current_count = get_current_count(conn)
    
    if current_count >= 100:
        print("Already have 100+ universities. No need to fetch more.")
        conn.close()
        return
    
    country_index = (current_count // 25) % len(COUNTRIES_TO_FETCH)
    country = COUNTRIES_TO_FETCH[country_index]
    
    universities = fetch_universities_by_country(country)
    
    if universities:
        inserted = store_universities(conn, universities)
    else:
        print(f"No data from {country}, trying next country...")
        next_country = COUNTRIES_TO_FETCH[(country_index + 1) % len(COUNTRIES_TO_FETCH)]
        universities = fetch_universities_by_country(next_country)
        if universities:
            store_universities(conn, universities)
    
    print("\n=== AFTER DATA COLLECTION ===")
    get_table_counts(conn)
    
    total = get_current_count(conn)
    if total >= 100:
        print("SUCCESS: You have collected 100+ universities!")
    else:
        runs_remaining = (100 - total + 24) // 25
        print(f"Progress: {total}/100 universities. Run this script {runs_remaining} more time(s).")
    
    conn.close()


if __name__ == "__main__":
    main()