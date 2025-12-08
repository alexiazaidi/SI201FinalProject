import requests
import sqlite3
from datetime import datetime, timedelta
from database_setup import init_db, get_table_counts


BASE_URL = "https://archive-api.open-meteo.com/v1/archive"
DATABASE_NAME = "project.db"
MAX_INSERT_PER_RUN = 25  


END_DATE = (datetime.now() - timedelta(days=2)).strftime("%Y-%m-%d")  # 2 days ago
START_DATE = (datetime.now() - timedelta(days=32)).strftime("%Y-%m-%d")  # 32 days ago


def fetch_weather_for_college(lat, lon, start_date, end_date):
    """
    Fetches daily weather data for a specific location from Open-Meteo API.
    
    FIXED: Added better error handling and validation
    """
    
    params = {
        "latitude": lat,
        "longitude": lon,
        "start_date": start_date,
        "end_date": end_date,
        "daily": ["temperature_2m_max", "temperature_2m_min", "precipitation_sum"],
        "timezone": "America/New_York"
    }
    
    try:
    
        response = requests.get(BASE_URL, params=params, timeout=15)
        

        if response.status_code != 200:
            print(f"  Error: API returned status code {response.status_code}")
            return []
        
  
        data = response.json()
        daily = data.get("daily", {})
        

        times = daily.get("time", [])
        temp_maxs = daily.get("temperature_2m_max", [])
        temp_mins = daily.get("temperature_2m_min", [])
        precips = daily.get("precipitation_sum", [])
        
     
        if not times or len(times) == 0:
            print(f"  Warning: No weather data returned for coordinates ({lat}, {lon})")
            return []
        

        results = []
        for i in range(len(times)):
            results.append({
                "date": times[i],
                "temp_max": temp_maxs[i] if i < len(temp_maxs) else None,
                "temp_min": temp_mins[i] if i < len(temp_mins) else None,
                "precip_sum": precips[i] if i < len(precips) else None
            })
        
        return results
        
    except Exception as e:
        print(f"  Error fetching weather: {e}")
        return []


def store_weather(conn, college_id, weather_list):
    """
    Stores weather data for a specific college into the database.
    Respects the 25-item-per-run limit.
    
    FIXED: Better duplicate handling
    """
    cur = conn.cursor()
    inserted_count = 0
    
    for record in weather_list:
 
        if inserted_count >= MAX_INSERT_PER_RUN:
            break
        
        date = record.get("date")
        if not date:
            continue
        

        cur.execute("""
            SELECT 1 FROM daily_weather 
            WHERE college_id = ? AND date = ?
        """, (college_id, date))
        
        if cur.fetchone() is not None:

            continue
        

        try:
            cur.execute("""
                INSERT INTO daily_weather (college_id, date, temp_max, temp_min, precip_sum)
                VALUES (?, ?, ?, ?, ?)
            """, (college_id, date, record.get("temp_max"), 
                  record.get("temp_min"), record.get("precip_sum")))
            inserted_count += 1
        except sqlite3.IntegrityError:

            continue
    
    conn.commit()
    return inserted_count


def get_colleges_needing_weather(conn, limit=5):
    """
    FIXED: Better query to prioritize colleges without any weather data
    """
    cur = conn.cursor()
    
    cur.execute("""
        SELECT c.id, c.name, c.lat, c.lon, c.state
        FROM us_colleges c
        WHERE c.lat IS NOT NULL 
          AND c.lon IS NOT NULL
          AND c.id NOT IN (SELECT DISTINCT college_id FROM daily_weather)
        LIMIT ?
    """, (limit,))
    
    return cur.fetchall()


def get_colleges_with_partial_weather(conn, limit=5):
    """
    FIXED: Returns colleges with some weather but less than 25 days
    """
    cur = conn.cursor()
    
    cur.execute("""
        SELECT c.id, c.name, c.lat, c.lon, c.state, COUNT(w.id) as weather_count
        FROM us_colleges c
        INNER JOIN daily_weather w ON c.id = w.college_id
        WHERE c.lat IS NOT NULL 
          AND c.lon IS NOT NULL
        GROUP BY c.id
        HAVING weather_count > 0 AND weather_count < 25
        ORDER BY weather_count ASC
        LIMIT ?
    """, (limit,))
    
    return cur.fetchall()


def main():
    """
    Main execution function for weather data collection.
    
    FIXED: Better logic for progressive data collection
    """
    
    # Connect to database
    conn = init_db(DATABASE_NAME)
    
    print("=" * 60)
    print("WEATHER DATA COLLECTION - Eve's Script")
    print("=" * 60)
    
    # Show current status
    print("\n=== BEFORE DATA COLLECTION ===")
    get_table_counts(conn)
    
    # Check if we have college data first
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM us_colleges WHERE lat IS NOT NULL AND lon IS NOT NULL")
    colleges_with_coords = cur.fetchone()[0]
    
    if colleges_with_coords == 0:
        print("=" * 60)
        print("ERROR: No colleges with coordinates found!")
        print("=" * 60)
        print("\nThis script needs college data to work.")
        print("Please run gather_colleges.py first!")
        print("=" * 60)
        conn.close()
        return
    
    print(f"Found {colleges_with_coords} colleges with coordinates.")
    
    # Check current weather data count
    cur.execute("SELECT COUNT(*) FROM daily_weather")
    current_weather_count = cur.fetchone()[0]
    
    if current_weather_count >= 100:
        print("\n✓ Already have 100+ weather records!")
        print("Eve's data collection is COMPLETE.")
        conn.close()
        return
    
    # FIXED: Try colleges without any weather first
    colleges = get_colleges_needing_weather(conn, limit=10)
    
    if not colleges:
        # All colleges have some weather data, add more to existing ones
        colleges_partial = get_colleges_with_partial_weather(conn, limit=10)
        if colleges_partial:
            print("All colleges have some weather. Adding more days...")
            # Convert to same format (without the count)
            colleges = [(c[0], c[1], c[2], c[3], c[4]) for c in colleges_partial]
        else:
            print("All colleges have complete weather data!")
            get_table_counts(conn)
            conn.close()
            return
    
    print(f"Found {len(colleges)} colleges needing weather data.\n")
    
    # Track how many records we've inserted this run
    total_inserted_this_run = 0
    
    # Process each college
    for college_id, name, lat, lon, state in colleges:
        
        # Check if we've hit the limit for this run
        if total_inserted_this_run >= MAX_INSERT_PER_RUN:
            print(f"\nReached {MAX_INSERT_PER_RUN} insertions limit for this run.")
            break
        
        print(f"Processing: {name} ({state})")
        print(f"  Location: ({lat}, {lon})")
        print(f"  Date range: {START_DATE} to {END_DATE}")
        
        # Fetch weather data from API
        weather_data = fetch_weather_for_college(lat, lon, START_DATE, END_DATE)
        
        if not weather_data:
            print(f"  ⚠️ No weather data received, skipping...")
            continue
        
        print(f"  Received {len(weather_data)} days of weather data from API")
        
        # Calculate how many we can still insert
        remaining_quota = MAX_INSERT_PER_RUN - total_inserted_this_run
        
        # Store weather data
        inserted = store_weather(conn, college_id, weather_data[:remaining_quota])
        total_inserted_this_run += inserted
        
        print(f"  ✓ Inserted {inserted} new weather records")
        print(f"  Total this run: {total_inserted_this_run}/{MAX_INSERT_PER_RUN}\n")
    
    # Final report
    print("=" * 60)
    print(f"COMPLETED: Inserted {total_inserted_this_run} weather records this run")
    print("=" * 60)
    
    print("\n=== AFTER DATA COLLECTION ===")
    get_table_counts(conn)
    
    # Check overall progress
    cur.execute("SELECT COUNT(*) FROM daily_weather")
    total_weather = cur.fetchone()[0]
    
    print()
    if total_weather >= 100:
        print("✓ SUCCESS: You have collected 100+ weather records!")
        print("Eve's data collection is COMPLETE!")
    else:
        runs_remaining = (100 - total_weather + 24) // 25
        print(f"Progress: {total_weather}/100 weather records.")
        print(f"Run this script approximately {runs_remaining} more time(s).")
    
    conn.close()


if __name__ == "__main__":
    main()