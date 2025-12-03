import requests
import sqlite3
from database_setup import init_db, get_table_counts


API_KEY = "u0vFVXgpsddy1j2vde7XyLz7W5TrbEEfaYHsswt6"  #from https://api.data.gov/signup/
BASE_URL = "https://api.data.gov/ed/collegescorecard/v1/schools"
DATABASE_NAME = "project.db"
MAX_INSERT_PER_RUN = 25  


def fetch_college_page(api_key, page, per_page=100):
    """
    Fetches one page of college data from the College Scorecard API.
    
    Parameters:
        api_key (str): Your API key from api.data.gov
        page (int): Which page to fetch (0-indexed)
        per_page (int): Number of results per page (max 100)
    
    Returns:
        list: List of dictionaries, each containing one college's data
    
    How the API pagination works:
        - page=0 returns colleges 0-99
        - page=1 returns colleges 100-199
        - page=2 returns colleges 200-299
        - etc.
    """
    
 
    
    fields = [
        "id",
        "school.name",
        "school.city", 
        "school.state",
        "school.zip",
        "school.ownership",                              
        "school.degrees_awarded.predominant",           
        "location.lat",
        "location.lon",
        "latest.student.size",
        "latest.cost.tuition.in_state",
        "latest.cost.tuition.out_of_state",
        "latest.cost.attendance.academic_year",
        "latest.completion.rate_suppressed.overall",
        "latest.earnings.10_yrs_after_entry.working_not_enrolled.mean_earnings"
    ]
    
    params = {
        "api_key": api_key,
        "page": page,
        "per_page": per_page,
        "fields": ",".join(fields)
    }
    
    print(f"Fetching page {page} from College Scorecard API...")
    response = requests.get(BASE_URL, params=params)
    
    if response.status_code != 200:
        print(f"Error: API returned status code {response.status_code}")
        print(response.text)
        return []
    
    data = response.json()
    results = data.get("results", [])
    
    print(f"Received {len(results)} colleges from API")
    return results


def store_college_page(conn, college_list):
    """
    Stores college data into the database, respecting the 25-item limit.
    
    Parameters:
        conn: SQLite connection object
        college_list (list): List of college dictionaries from fetch_college_page
    
    Returns:
        int: Number of new rows actually inserted
    
    CRITICAL LOGIC:
        1. Check if college already exists (by id) - prevents duplicates
        2. Stop after inserting 25 new records - satisfies the per-run limit
        3. Insert into BOTH us_colleges AND college_financials tables
    """
    cur = conn.cursor()
    inserted_count = 0
    
    for college in college_list:
     
        if inserted_count >= MAX_INSERT_PER_RUN:
            print(f"Reached {MAX_INSERT_PER_RUN} insertions limit for this run.")
            break

        college_id = college.get("id")
        if college_id is None:
            continue
        
        
        cur.execute("SELECT 1 FROM us_colleges WHERE id = ?", (college_id,))
        if cur.fetchone() is not None:
           
            continue
        
       
        name = college.get("school.name", "Unknown")
        city = college.get("school.city")
        state = college.get("school.state")
        zip_code = college.get("school.zip")
        ownership = college.get("school.ownership")
        predominant_degree = college.get("school.degrees_awarded.predominant")
        lat = college.get("location.lat")
        lon = college.get("location.lon")
        student_size = college.get("latest.student.size")
        
        in_state_tuition = college.get("latest.cost.tuition.in_state")
        out_state_tuition = college.get("latest.cost.tuition.out_of_state")
        academic_year_cost = college.get("latest.cost.attendance.academic_year")
        completion_rate = college.get("latest.completion.rate_suppressed.overall")
        earnings_10yr = college.get("latest.earnings.10_yrs_after_entry.working_not_enrolled.mean_earnings")
        
       
        cur.execute('''
            INSERT INTO us_colleges 
            (id, name, city, state, zip, ownership, predominant_degree, lat, lon, student_size)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (college_id, name, city, state, zip_code, ownership, 
              predominant_degree, lat, lon, student_size))
        
       
        cur.execute('''
            INSERT INTO college_financials
            (id, in_state_tuition, out_state_tuition, academic_year_cost, completion_rate, earnings_10yr)
            VALUES (?, ?, ?, ?, ?, ?)
        ''', (college_id, in_state_tuition, out_state_tuition, 
              academic_year_cost, completion_rate, earnings_10yr))
        
        inserted_count += 1
    
    conn.commit()
    print(f"Successfully inserted {inserted_count} new colleges into database.")
    return inserted_count


def get_next_page_to_fetch(conn):
    """
    Determines which API page to fetch next based on current database contents.
    
    This allows the script to be run multiple times without changing any code.
    """
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM us_colleges")
    current_count = cur.fetchone()[0]

    return current_count // 100


def main():
 
    conn = init_db(DATABASE_NAME)
    
    
    print("=== BEFORE DATA COLLECTION ===")
    get_table_counts(conn)
    
   
    page = get_next_page_to_fetch(conn)
    
    
    colleges = fetch_college_page(API_KEY, page=page, per_page=100)
    
    if not colleges:
        print("No data received from API. Check your API key and internet connection.")
        conn.close()
        return
    
    
    inserted = store_college_page(conn, colleges)
    
    
    print("\n=== AFTER DATA COLLECTION ===")
    get_table_counts(conn)
    
   
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM us_colleges")
    total = cur.fetchone()[0]
    
    if total >= 100:
        print("✓ SUCCESS: You have collected 100+ colleges!")
    else:
        runs_remaining = (100 - total + 24) // 25  
        print(f"Progress: {total}/100 colleges. Run this script {runs_remaining} more time(s).")
    
    conn.close()


if __name__ == "__main__":
    main()

