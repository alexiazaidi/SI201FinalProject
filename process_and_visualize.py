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

def plot_state_tuition(state_stats, filename='visualizations/state_tuition.png'):
    """
    Alexia's visualization: Bar chart of average tuition by state.
    """
    top_states = sorted(state_stats, key=lambda x: x['avg_in_state_tuition'] or 0, reverse=True)[:15]
    states = [s['state'] for s in top_states]
    tuitions = [s['avg_in_state_tuition'] or 0 for s in top_states]
    fig, ax = plt.subplots(figsize=(12, 6))
    colors = plt.cm.viridis(np.linspace(0.2, 0.8, len(states)))
    bars = ax.bar(states, tuitions, color=colors, edgecolor='black', linewidth=0.5)
    ax.set_xlabel('State', fontsize=12)
    ax.set_ylabel('Average In-State Tuition ($)', fontsize=12)
    ax.set_title('Top 15 States by Average In-State College Tuition', fontsize=14, fontweight='bold')
    
   
    for bar, tuition in zip(bars, tuitions):
        height = bar.get_height()
        ax.annotate(f'${int(height):,}',
                    xy=(bar.get_x() + bar.get_width() / 2, height),
                    xytext=(0, 3),
                    textcoords="offset points",
                    ha='center', va='bottom', fontsize=8, rotation=45)
    
    plt.xticks(rotation=45, ha='right')
    plt.tight_layout()
    plt.savefig(filename, dpi=150, bbox_inches='tight')
    plt.close()
    print(f"Saved: {filename}")


def plot_tuition_vs_completion(conn, filename='visualizations/tuition_vs_completion.png'):
    """
    Alexia's visualization: Scatter plot of tuition vs completion rate.
    """
    cur = conn.cursor()
    cur.execute("""
        SELECT cf.in_state_tuition, cf.completion_rate
        FROM college_financials cf
        WHERE cf.in_state_tuition IS NOT NULL 
          AND cf.completion_rate IS NOT NULL
    """)
    data = cur.fetchall()
    
    tuitions = [row[0] for row in data]
    completions = [row[1] * 100 for row in data]  
    
    fig, ax = plt.subplots(figsize=(10, 6))
    
    scatter = ax.scatter(tuitions, completions, alpha=0.6, c='steelblue', 
                         edgecolors='navy', linewidth=0.5, s=50)
    
    z = np.polyfit(tuitions, completions, 1)
    p = np.poly1d(z)
    x_line = np.linspace(min(tuitions), max(tuitions), 100)
    ax.plot(x_line, p(x_line), "r--", alpha=0.8, linewidth=2, label='Trend Line')
    
    ax.set_xlabel('In-State Tuition ($)', fontsize=12)
    ax.set_ylabel('Completion Rate (%)', fontsize=12)
    ax.set_title('College Tuition vs. Completion Rate', fontsize=14, fontweight='bold')
    ax.legend()
    
    corr = np.corrcoef(tuitions, completions)[0, 1]
    ax.text(0.05, 0.95, f'Correlation: {corr:.3f}', transform=ax.transAxes,
            fontsize=10, verticalalignment='top', bbox=dict(boxstyle='round', facecolor='wheat'))
    
    plt.tight_layout()
    plt.savefig(filename, dpi=150)
    plt.close()
    print(f"Saved: {filename}")






