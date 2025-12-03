import sqlite3
import matplotlib.pyplot as plt
import numpy as np
from database_setup import init_db, get_table_counts

DATABASE_NAME = "project.db"

def calculate_state_level_stats(conn):
    """
   Calculate state-level averages for U.S. colleges.
    """
    cur = conn.cursor()
    
    cur.execute("""
        SELECT 
            c.state,
            COUNT(*) as num_colleges,
            AVG(cf.in_state_tuition) as avg_in_state_tuition,
            AVG(cf.out_state_tuition) as avg_out_state_tuition,
            AVG(cf.academic_year_cost) as avg_total_cost,
            AVG(cf.completion_rate) as avg_completion_rate,
            AVG(cf.earnings_10yr) as avg_earnings,
            SUM(c.student_size) as total_students
        FROM us_colleges c
        JOIN college_financials cf ON c.id = cf.id
        WHERE c.state IS NOT NULL
        GROUP BY c.state
        ORDER BY avg_in_state_tuition DESC
    """)
    
    results = cur.fetchall()
    
    
    columns = ['state', 'num_colleges', 'avg_in_state_tuition', 'avg_out_state_tuition',
               'avg_total_cost', 'avg_completion_rate', 'avg_earnings', 'total_students']
    
    return [dict(zip(columns, row)) for row in results]



