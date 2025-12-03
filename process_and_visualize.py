import sqlite3
import matplotlib.pyplot as plt
import numpy as np
from database_setup import init_db, get_table_counts

DATABASE_NAME = "project.db"


def calculate_country_uni_counts(conn):
    """
    Parker's function: Count universities per country.
    """
    cur = conn.cursor()
    
    cur.execute("""
        SELECT country, COUNT(*) as university_count
        FROM universities_world
        GROUP BY country
        ORDER BY university_count DESC
    """)
    
    results = cur.fetchall()
    return [{'country': row[0], 'count': row[1]} for row in results]


def calculate_correlations(conn):
    """
    Additional calculation: Compute correlations between variables.
    """
    cur = conn.cursor()
    
    # Get data for correlation analysis
    cur.execute("""
        SELECT cf.in_state_tuition, cf.completion_rate, cf.earnings_10yr
        FROM college_financials cf
        WHERE cf.in_state_tuition IS NOT NULL 
          AND cf.completion_rate IS NOT NULL
          AND cf.earnings_10yr IS NOT NULL
    """)
    
    data = cur.fetchall()
    
    if len(data) < 3:
        return {}
    
    tuitions = [row[0] for row in data]
    completions = [row[1] for row in data]
    earnings = [row[2] for row in data]
    
    # Calculate Pearson correlations
    corr_tuition_completion = np.corrcoef(tuitions, completions)[0, 1]
    corr_tuition_earnings = np.corrcoef(tuitions, earnings)[0, 1]
    corr_completion_earnings = np.corrcoef(completions, earnings)[0, 1]
    
    return {
        'tuition_vs_completion': corr_tuition_completion,
        'tuition_vs_earnings': corr_tuition_earnings,
        'completion_vs_earnings': corr_completion_earnings
    }
