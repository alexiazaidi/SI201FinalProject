import sqlite3
import matplotlib.pyplot as plt
import numpy as np
import os
from database_setup import init_db, get_table_counts

DATABASE_NAME = "project.db"

def calculate_state_level_stats(conn):
    """
    FIXED: Better NULL handling to prevent $0 tuition bug
    """
    cur = conn.cursor()
    
    cur.execute("""
        SELECT 
            c.state,
            COUNT(*) as num_colleges,
            AVG(CASE WHEN cf.in_state_tuition > 0 THEN cf.in_state_tuition END) as avg_in_state_tuition,
            AVG(CASE WHEN cf.out_state_tuition > 0 THEN cf.out_state_tuition END) as avg_out_state_tuition,
            AVG(CASE WHEN cf.academic_year_cost > 0 THEN cf.academic_year_cost END) as avg_total_cost,
            AVG(cf.completion_rate) as avg_completion_rate,
            AVG(CASE WHEN cf.earnings_10yr > 0 THEN cf.earnings_10yr END) as avg_earnings,
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


def calculate_correlations(conn):
    """
    FIXED: Filter out NULL and zero values properly
    """
    cur = conn.cursor()
    
    cur.execute("""
        SELECT cf.in_state_tuition, cf.completion_rate, cf.earnings_10yr
        FROM college_financials cf
        WHERE cf.in_state_tuition IS NOT NULL 
          AND cf.in_state_tuition > 0
          AND cf.completion_rate IS NOT NULL
          AND cf.completion_rate > 0
          AND cf.earnings_10yr IS NOT NULL
          AND cf.earnings_10yr > 0
    """)
    
    data = cur.fetchall()
    
    if len(data) < 3:
        return {}
    
    tuitions = [row[0] for row in data]
    completions = [row[1] for row in data]
    earnings = [row[2] for row in data]
    
    corr_tuition_completion = np.corrcoef(tuitions, completions)[0, 1]
    corr_tuition_earnings = np.corrcoef(tuitions, earnings)[0, 1]
    corr_completion_earnings = np.corrcoef(completions, earnings)[0, 1]
    
    return {
        'tuition_vs_completion': corr_tuition_completion,
        'tuition_vs_earnings': corr_tuition_earnings,
        'completion_vs_earnings': corr_completion_earnings
    }


def calculate_climate_and_completion(conn):
    """
    FIXED: This is the CRITICAL fix for your Section 3 bug!
    
    The original query was using JOIN which requires matching records.
    If there's no weather data, the JOIN fails and returns nothing.
    """
    cur = conn.cursor()
    
    # FIXED: Added check for weather data existence
    cur.execute("SELECT COUNT(*) FROM daily_weather")
    weather_count = cur.fetchone()[0]
    
    if weather_count == 0:
        print("⚠️ WARNING: No weather data found in database!")
        print("   Please run gather_weather.py multiple times to collect weather data.")
        return []
    
    cur.execute("""
        SELECT 
            c.state,
            c.name,
            cf.in_state_tuition,
            cf.completion_rate,
            cf.earnings_10yr,
            AVG(w.temp_max) as avg_temp_max,
            AVG(w.temp_min) as avg_temp_min,
            AVG(w.precip_sum) as avg_precip
        FROM us_colleges c
        JOIN college_financials cf ON c.id = cf.id
        JOIN daily_weather w ON c.id = w.college_id
        WHERE cf.completion_rate IS NOT NULL
          AND cf.completion_rate > 0
        GROUP BY c.id
        HAVING avg_temp_max IS NOT NULL
    """)
    
    results = cur.fetchall()
    
    if len(results) == 0:
        print("⚠️ WARNING: JOIN returned no results!")
        print("   This means weather data exists but doesn't match college IDs.")
        return []
    
    columns = ['state', 'name', 'in_state_tuition', 'completion_rate', 
               'earnings_10yr', 'avg_temp_max', 'avg_temp_min', 'avg_precip']
    
    return [dict(zip(columns, row)) for row in results]


def calculate_country_uni_counts(conn):
    """Calculate university counts by country"""
    cur = conn.cursor()
    
    cur.execute("""
        SELECT country, COUNT(*) as university_count
        FROM universities_world
        GROUP BY country
        ORDER BY university_count DESC
    """)
    
    results = cur.fetchall()
    return [{'country': row[0], 'count': row[1]} for row in results]


def plot_state_tuition(state_stats, filename='visualizations/state_tuition.png'):
    """FIXED: Filter out states with no valid tuition data"""
    
    # Filter out None values
    valid_states = [s for s in state_stats if s['avg_in_state_tuition'] is not None]
    top_states = sorted(valid_states, key=lambda x: x['avg_in_state_tuition'], reverse=True)[:15]
    
    if len(top_states) == 0:
        print("⚠️ No valid tuition data for visualization")
        return
    
    states = [s['state'] for s in top_states]
    tuitions = [s['avg_in_state_tuition'] for s in top_states]
    
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
    print(f"✓ Saved: {filename}")


def plot_tuition_vs_completion(conn, filename='visualizations/tuition_vs_completion.png'):
    """FIXED: Better data filtering"""
    cur = conn.cursor()
    cur.execute("""
        SELECT cf.in_state_tuition, cf.completion_rate
        FROM college_financials cf
        WHERE cf.in_state_tuition IS NOT NULL 
          AND cf.in_state_tuition > 0
          AND cf.completion_rate IS NOT NULL
          AND cf.completion_rate > 0
    """)
    data = cur.fetchall()
    
    if len(data) < 2:
        print("⚠️ Not enough data for tuition vs completion plot")
        return
    
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
    print(f"✓ Saved: {filename}")


def plot_climate_vs_completion(climate_stats, filename='visualizations/climate_vs_completion.png'):
    """FIXED: Better validation and error messages"""
    
    if not climate_stats or len(climate_stats) == 0:
        print("⚠️ No climate data available for visualization")
        print("   ACTION REQUIRED: Run gather_weather.py multiple times!")
        return
    
    valid_data = [(s['avg_temp_max'], (s['completion_rate'] or 0) * 100) 
                  for s in climate_stats 
                  if s['avg_temp_max'] is not None and s['completion_rate'] is not None]
    
    if len(valid_data) < 2:
        print(f"⚠️ Not enough valid data points for climate visualization (need at least 2, have {len(valid_data)})")
        return
    
    temps, completions = zip(*valid_data)
    
    fig, ax = plt.subplots(figsize=(10, 6))
    
    scatter = ax.scatter(temps, completions, 
                        alpha=0.6,
                        c='coral',
                        edgecolors='darkred',
                        linewidth=0.5,
                        s=60)
    
    z = np.polyfit(temps, completions, 1)
    p = np.poly1d(z)
    x_line = np.linspace(min(temps), max(temps), 100)
    ax.plot(x_line, p(x_line), "b--", alpha=0.8, linewidth=2, label='Trend Line')
    
    ax.set_xlabel('Average Max Temperature (°C)', fontsize=12)
    ax.set_ylabel('Completion Rate (%)', fontsize=12)
    ax.set_title('Climate vs. College Completion Rate', fontsize=14, fontweight='bold')
    ax.legend()
    
    corr = np.corrcoef(temps, completions)[0, 1]
    ax.text(0.05, 0.95, 
            f'Correlation: {corr:.3f}', 
            transform=ax.transAxes,
            fontsize=10, 
            verticalalignment='top', 
            bbox=dict(boxstyle='round', facecolor='lightcyan', alpha=0.8))
    
    if abs(corr) < 0.3:
        interpretation = "Weak correlation"
    elif abs(corr) < 0.7:
        interpretation = "Moderate correlation"
    else:
        interpretation = "Strong correlation"
    
    ax.text(0.05, 0.88, 
            interpretation, 
            transform=ax.transAxes,
            fontsize=9, 
            verticalalignment='top',
            style='italic')
    
    plt.tight_layout()
    plt.savefig(filename, dpi=150)
    plt.close()
    print(f"✓ Saved: {filename}")


def plot_universities_per_country(country_counts, filename='visualizations/universities_per_country.png'):
    """Plot universities by country"""
    
    if not country_counts:
        print("⚠️ No university data for visualization")
        return
    
    top_countries = country_counts[:10]
    
    countries = [c['country'] for c in top_countries]
    counts = [c['count'] for c in top_countries]
    
    fig, ax = plt.subplots(figsize=(12, 6))
    
    colors = plt.cm.plasma(np.linspace(0.2, 0.8, len(countries)))
    
    bars = ax.barh(countries, counts, color=colors, edgecolor='black', linewidth=0.5)
    
    ax.set_xlabel('Number of Universities', fontsize=12)
    ax.set_ylabel('Country', fontsize=12)
    ax.set_title('Universities per Country (Top 10)', fontsize=14, fontweight='bold')
    
    for bar, count in zip(bars, counts):
        width = bar.get_width()
        ax.annotate(f'{count}',
                    xy=(width, bar.get_y() + bar.get_height() / 2),
                    xytext=(3, 0),
                    textcoords="offset points",
                    ha='left', va='center', fontsize=10)
    
    plt.tight_layout()
    plt.savefig(filename, dpi=150)
    plt.close()
    print(f"✓ Saved: {filename}")


def write_results_to_file(state_stats, climate_stats, country_counts, correlations, 
                          filename='results_summary.txt'):
    """FIXED: Better formatting and NULL handling"""
    
    with open(filename, 'w') as f:
        f.write("=" * 60 + "\n")
        f.write("SI 201 FINAL PROJECT - PEA TEAM RESULTS SUMMARY\n")
        f.write("=" * 60 + "\n\n")
        
        f.write("SECTION 1: STATE-LEVEL COLLEGE STATISTICS\n")
        f.write("-" * 60 + "\n")
        f.write(f"{'State':<8} {'# Colleges':<12} {'Avg Tuition':<18} {'Completion %':<15}\n")
        f.write("-" * 60 + "\n")
        
        for state in state_stats[:20]:
            tuition = f"${int(state['avg_in_state_tuition']):,}" if state['avg_in_state_tuition'] else "N/A"
            completion = f"{(state['avg_completion_rate'] or 0) * 100:.1f}%" if state['avg_completion_rate'] else "N/A"
            f.write(f"{state['state']:<8} {state['num_colleges']:<12} {tuition:<18} {completion:<15}\n")
        
        f.write(f"\nTotal states analyzed: {len(state_stats)}\n")
        f.write("\n")
        
        f.write("SECTION 2: CORRELATION ANALYSIS\n")
        f.write("-" * 60 + "\n")
        if correlations:
            f.write(f"Tuition vs Completion Rate:  {correlations.get('tuition_vs_completion', 'N/A'):.4f}\n")
            f.write(f"Tuition vs 10-Year Earnings: {correlations.get('tuition_vs_earnings', 'N/A'):.4f}\n")
            f.write(f"Completion vs Earnings:      {correlations.get('completion_vs_earnings', 'N/A'):.4f}\n")
            f.write("\nInterpretation:\n")
            f.write("- Values close to 1 indicate strong positive correlation\n")
            f.write("- Values close to -1 indicate strong negative correlation\n")
            f.write("- Values close to 0 indicate weak or no correlation\n")
        else:
            f.write("Insufficient data for correlation analysis\n")
        
        f.write("\n")
        
        f.write("SECTION 3: CLIMATE AND COMPLETION ANALYSIS\n")
        f.write("-" * 60 + "\n")
        if climate_stats and len(climate_stats) > 0:
            f.write(f"Number of colleges with climate data: {len(climate_stats)}\n")
            avg_temp = sum(s['avg_temp_max'] or 0 for s in climate_stats) / len(climate_stats)
            avg_precip = sum(s['avg_precip'] or 0 for s in climate_stats) / len(climate_stats)
            avg_completion = sum((s['completion_rate'] or 0) for s in climate_stats) / len(climate_stats)
            f.write(f"Average max temperature across colleges: {avg_temp:.1f}°C\n")
            f.write(f"Average precipitation: {avg_precip:.2f} mm\n")
            f.write(f"Average completion rate: {avg_completion * 100:.1f}%\n")
            
            f.write("\nTop 5 Colleges by Completion Rate:\n")
            sorted_colleges = sorted(climate_stats, key=lambda x: x['completion_rate'] or 0, reverse=True)
            for i, college in enumerate(sorted_colleges[:5], 1):
                f.write(f"  {i}. {college['name']} ({college['state']})\n")
                f.write(f"     Completion: {(college['completion_rate'] or 0) * 100:.1f}%\n")
                f.write(f"     Avg Temp: {college['avg_temp_max']:.1f}°C\n")
        else:
            f.write("⚠️ NO CLIMATE DATA AVAILABLE\n")
            f.write("\nACTION REQUIRED:\n")
            f.write("1. Run gather_weather.py multiple times (at least 4-5 times)\n")
            f.write("2. Each run adds 25 weather records\n")
            f.write("3. Need 100+ weather records to meet project requirements\n")
            f.write("4. After collecting weather data, run this script again\n")
        
        f.write("\n")
        
        f.write("SECTION 4: UNIVERSITIES BY COUNTRY\n")
        f.write("-" * 60 + "\n")
        if country_counts:
            for country in country_counts[:15]:
                f.write(f"{country['country']:<30} {country['count']:>5} universities\n")
            f.write(f"\nTotal countries: {len(country_counts)}\n")
        else:
            f.write("No university data available\n")
        
        f.write("\n")
        f.write("=" * 60 + "\n")
        f.write("END OF REPORT\n")
        f.write("=" * 60 + "\n")
    
    print(f"✓ Results written to: {filename}")


def plot_earnings_by_tuition_category(conn, filename='visualizations/bonus_earnings_by_tuition.png'):
    """BONUS Visualization 1 - FIXED: Better data filtering"""
    cur = conn.cursor()
    
    cur.execute("""
        SELECT cf.in_state_tuition, cf.earnings_10yr
        FROM college_financials cf
        WHERE cf.in_state_tuition IS NOT NULL 
          AND cf.in_state_tuition > 0
          AND cf.earnings_10yr IS NOT NULL
          AND cf.earnings_10yr > 0
    """)
    
    data = cur.fetchall()
    
    if len(data) < 10:
        print("⚠️ Not enough data for earnings visualization")
        return
    
    tuitions = [row[0] for row in data]
    earnings = [row[1] for row in data]
    
    tuition_25th = np.percentile(tuitions, 25)
    tuition_75th = np.percentile(tuitions, 75)
    
    low_tuition_earnings = []
    med_tuition_earnings = []
    high_tuition_earnings = []
    
    for tuition, earning in zip(tuitions, earnings):
        if tuition < tuition_25th:
            low_tuition_earnings.append(earning)
        elif tuition < tuition_75th:
            med_tuition_earnings.append(earning)
        else:
            high_tuition_earnings.append(earning)
    
    fig, ax = plt.subplots(figsize=(10, 7))
    
    box_data = [low_tuition_earnings, med_tuition_earnings, high_tuition_earnings]
    labels = ['Low Tuition\n(< $' + f'{int(tuition_25th):,}' + ')',
              'Medium Tuition\n($' + f'{int(tuition_25th):,}' + ' - $' + f'{int(tuition_75th):,}' + ')',
              'High Tuition\n(> $' + f'{int(tuition_75th):,}' + ')']
    
    bp = ax.boxplot(box_data, tick_labels=labels, patch_artist=True,
                    notch=True, showmeans=True)
    
    colors = ['#90EE90', '#3CB371', '#006400']
    for patch, color in zip(bp['boxes'], colors):
        patch.set_facecolor(color)
        patch.set_alpha(0.7)
    
    for median in bp['medians']:
        median.set(color='red', linewidth=2)
    
    for mean in bp['means']:
        mean.set(marker='D', markerfacecolor='orange', markersize=8)
    
    ax.set_ylabel('10-Year Median Earnings ($)', fontsize=12)
    ax.set_xlabel('Tuition Category', fontsize=12)
    ax.set_title('Graduate Earnings by College Tuition Level', 
                 fontsize=14, fontweight='bold', pad=20)
    
    ax.yaxis.grid(True, linestyle='--', alpha=0.3)
    ax.yaxis.set_major_formatter(plt.FuncFormatter(lambda x, p: f'${int(x/1000)}K'))
    
    stats_text = f'Sample Size:\nLow: {len(low_tuition_earnings)}\nMed: {len(med_tuition_earnings)}\nHigh: {len(high_tuition_earnings)}'
    ax.text(0.02, 0.98, stats_text, transform=ax.transAxes,
            fontsize=9, verticalalignment='top',
            bbox=dict(boxstyle='round', facecolor='wheat', alpha=0.5))
    
    from matplotlib.lines import Line2D
    legend_elements = [
        Line2D([0], [0], color='red', linewidth=2, label='Median'),
        Line2D([0], [0], marker='D', color='w', markerfacecolor='orange', 
               markersize=8, label='Mean')
    ]
    ax.legend(handles=legend_elements, loc='upper right')
    
    plt.tight_layout()
    plt.savefig(filename, dpi=150, bbox_inches='tight')
    plt.close()
    
    print(f"✓ Saved BONUS: {filename}")


def plot_state_completion_heatmap(conn, filename='visualizations/bonus_state_heatmap.png'):
    """BONUS Visualization 2 - FIXED: Better data validation"""
    cur = conn.cursor()

    cur.execute("""
        SELECT 
            c.state,
            COUNT(*) as num_colleges,
            AVG(cf.completion_rate) as avg_completion,
            AVG(cf.in_state_tuition) as avg_tuition
        FROM us_colleges c
        JOIN college_financials cf ON c.id = cf.id
        WHERE c.state IS NOT NULL 
          AND cf.completion_rate IS NOT NULL
          AND cf.completion_rate > 0
        GROUP BY c.state
        HAVING num_colleges >= 1
        ORDER BY num_colleges DESC
    """)
    
    results = cur.fetchall()
    
    if len(results) < 3:
        print("⚠️ Not enough states for heatmap visualization")
        return
    
    states = [row[0] for row in results]
    college_counts = [row[1] for row in results]
    completion_rates = [row[2] * 100 for row in results]
    
    fig, ax = plt.subplots(figsize=(14, 8))
    
    sizes = [count * 100 for count in college_counts]
    
    scatter = ax.scatter(range(len(states)), completion_rates, 
                        s=sizes, 
                        c=completion_rates,
                        cmap='RdYlGn', 
                        alpha=0.6,
                        edgecolors='black',
                        linewidth=1.5)
    
    cbar = plt.colorbar(scatter, ax=ax)
    cbar.set_label('Completion Rate (%)', rotation=270, labelpad=20, fontsize=11)
    
    ax.set_xticks(range(len(states)))
    ax.set_xticklabels(states, rotation=45, ha='right')
    ax.set_xlabel('State', fontsize=12)
    
    ax.set_ylabel('Average Completion Rate (%)', fontsize=12)
    ax.set_title('College Completion Rates by State\n(Bubble size = Number of Colleges)', 
                 fontsize=14, fontweight='bold', pad=20)
    
    ax.grid(True, alpha=0.3, linestyle='--')
    
    for count in [5, 20, 50]:
        if max(college_counts) >= count:
            ax.scatter([], [], s=count*100, c='gray', alpha=0.6, 
                      edgecolors='black', linewidth=1.5,
                      label=f'{count} colleges')
    
    ax.legend(scatterpoints=1, frameon=True, labelspacing=2, 
              title='Number of Colleges', loc='upper left')
    
    for i in range(min(3, len(states))):
        ax.annotate(f'{states[i]}\n{college_counts[i]} colleges\n{completion_rates[i]:.1f}%',
                   xy=(i, completion_rates[i]),
                   xytext=(10, 10),
                   textcoords='offset points',
                   fontsize=8,
                   bbox=dict(boxstyle='round,pad=0.3', facecolor='yellow', alpha=0.7),
                   arrowprops=dict(arrowstyle='->', connectionstyle='arc3,rad=0'))
    
    plt.tight_layout()
    plt.savefig(filename, dpi=150, bbox_inches='tight')
    plt.close()
    
    print(f"✓ Saved BONUS: {filename}")


def main():
    """Main processing function with better error handling and diagnostics"""

    if not os.path.exists('visualizations'):
        os.makedirs('visualizations')
    
    conn = sqlite3.connect(DATABASE_NAME)
    
    print("=" * 60)
    print("SI 201 FINAL PROJECT - DATA PROCESSING & VISUALIZATION")
    print("=" * 60)
    
    print("\n=== CURRENT DATABASE STATUS ===")
    get_table_counts(conn)
    
    # ADDED: Diagnostic checks
    cur = conn.cursor()
    
    cur.execute("SELECT COUNT(*) FROM us_colleges")
    college_count = cur.fetchone()[0]
    
    cur.execute("SELECT COUNT(*) FROM college_financials")
    financial_count = cur.fetchone()[0]
    
    cur.execute("SELECT COUNT(*) FROM daily_weather")
    weather_count = cur.fetchone()[0]
    
    cur.execute("SELECT COUNT(*) FROM universities_world")
    uni_count = cur.fetchone()[0]
    
    print("\n=== DATA COLLECTION STATUS ===")
    print(f"✓ US Colleges: {college_count}/100 {'✓ COMPLETE' if college_count >= 100 else '⚠️ NEEDS MORE'}")
    print(f"✓ College Financials: {financial_count}/100 {'✓ COMPLETE' if financial_count >= 100 else '⚠️ NEEDS MORE'}")
    print(f"{'✓' if weather_count >= 100 else '⚠️'} Weather Records: {weather_count}/100 {'✓ COMPLETE' if weather_count >= 100 else '⚠️ NEEDS MORE - RUN gather_weather.py'}")
    print(f"✓ Universities World: {uni_count}/100 {'✓ COMPLETE' if uni_count >= 100 else '⚠️ NEEDS MORE'}")
    
    if college_count < 100 or financial_count < 100 or weather_count < 100 or uni_count < 100:
        print("\n" + "=" * 60)
        print("⚠️ WARNING: INCOMPLETE DATA COLLECTION")
        print("=" * 60)
        print("\nYou need to run the data gathering scripts more times:")
        if college_count < 100:
            print(f"  - gather_colleges.py: Need {100-college_count} more colleges")
        if weather_count < 100:
            print(f"  - gather_weather.py: Need {100-weather_count} more weather records")
        if uni_count < 100:
            print(f"  - gather_universities.py: Need {100-uni_count} more universities")
        print("\nContinuing with available data for now...")
        print("=" * 60)
    
    print("\n=== RUNNING CALCULATIONS ===")
    
    print("Calculating state-level statistics...")
    state_stats = calculate_state_level_stats(conn)
    print(f"  ✓ Found data for {len(state_stats)} states")
    
    print("Calculating climate and completion data...")
    climate_stats = calculate_climate_and_completion(conn)
    if climate_stats:
        print(f"  ✓ Found {len(climate_stats)} colleges with climate data")
    else:
        print(f"  ⚠️ NO CLIMATE DATA - Run gather_weather.py!")
    
    print("Calculating country university counts...")
    country_counts = calculate_country_uni_counts(conn)
    print(f"  ✓ Found universities in {len(country_counts)} countries")
    
    print("Calculating correlations...")
    correlations = calculate_correlations(conn)
    print(f"  ✓ Computed {len(correlations)} correlations")
    
    print("\n=== CREATING VISUALIZATIONS ===")
    
    if state_stats:
        print("Creating state tuition chart...")
        plot_state_tuition(state_stats)
    
    print("Creating tuition vs completion scatter plot...")
    plot_tuition_vs_completion(conn)
    
    if climate_stats:
        print("Creating climate vs completion plot...")
        plot_climate_vs_completion(climate_stats)
    else:
        print("⚠️ Skipping climate visualization - no data available")
    
    if country_counts:
        print("Creating universities per country chart...")
        plot_universities_per_country(country_counts)
    
    print("\n=== WRITING RESULTS FILE ===")
    write_results_to_file(state_stats, climate_stats, country_counts, correlations)
    
    print("\n=== CREATING BONUS VISUALIZATIONS (Extra Credit) ===")
    print("Creating earnings by tuition category box plot...")
    plot_earnings_by_tuition_category(conn)
    
    print("Creating state completion heat map...")
    plot_state_completion_heatmap(conn)
    
    print("\n" + "=" * 60)
    print("✓ PROCESSING COMPLETE!")
    print("=" * 60)
    
    print("\nCheck the 'visualizations' folder for your graphs:")
    print("\nRequired Visualizations:")
    print("    - state_tuition.png")
    print("    - tuition_vs_completion.png")
    if climate_stats:
        print("    - climate_vs_completion.png ✓")
    else:
        print("    - climate_vs_completion.png ⚠️ NOT CREATED (no weather data)")
    print("    - universities_per_country.png")
    
    print("\nBONUS Visualizations (+30 points):")
    print("    - bonus_earnings_by_tuition.png")
    print("    - bonus_state_heatmap.png")
    
    print("\nCheck 'results_summary.txt' for calculated data.")
    
    if weather_count < 100:
        print("\n" + "=" * 60)
        print("⚠️ CRITICAL: MISSING WEATHER DATA")
        print("=" * 60)
        print(f"\nYou have {weather_count} weather records but need 100+")
        print("\nTO FIX:")
        print("1. Run: python gather_weather.py")
        print("2. Run it multiple times (about 4-5 times)")
        print("3. Run: python process_and_visualize.py again")
        print("=" * 60)
    
    conn.close()

    
if __name__ == "__main__":
    main()