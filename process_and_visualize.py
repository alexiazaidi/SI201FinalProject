import sqlite3
import matplotlib.pyplot as plt
import numpy as np
import os
from database_setup import init_db, get_table_counts

DATABASE_NAME = "testing.db"

def calculate_state_level_stats(conn):
    cur = conn.cursor()
    
    cur.execute("""
        SELECT 
            s.name as state,
            COUNT(*) as num_colleges,
            AVG(CASE WHEN c.in_state_tuition > 0 THEN c.in_state_tuition END) as avg_in_state_tuition,
            AVG(CASE WHEN c.out_state_tuition > 0 THEN c.out_state_tuition END) as avg_out_state_tuition,
            AVG(CASE WHEN c.academic_year_cost > 0 THEN c.academic_year_cost END) as avg_total_cost,
            AVG(c.completion_rate) as avg_completion_rate,
            AVG(CASE WHEN c.earnings_10yr > 0 THEN c.earnings_10yr END) as avg_earnings,
            SUM(c.student_size) as total_students
        FROM colleges c
        JOIN state_provinces s ON c.state_id = s.id
        WHERE c.state_id IS NOT NULL
        GROUP BY c.state_id, s.name
        ORDER BY avg_in_state_tuition DESC
    """)
    
    results = cur.fetchall()
    columns = ['state', 'num_colleges', 'avg_in_state_tuition', 'avg_out_state_tuition',
               'avg_total_cost', 'avg_completion_rate', 'avg_earnings', 'total_students']
    
    return [dict(zip(columns, row)) for row in results]


def calculate_correlations(conn):
    cur = conn.cursor()
    
    cur.execute("""
        SELECT in_state_tuition, completion_rate, earnings_10yr
        FROM colleges
        WHERE in_state_tuition IS NOT NULL 
          AND in_state_tuition > 0
          AND completion_rate IS NOT NULL
          AND completion_rate > 0
          AND earnings_10yr IS NOT NULL
          AND earnings_10yr > 0
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
    cur = conn.cursor()
    
    cur.execute("SELECT COUNT(*) FROM daily_weather")
    weather_count = cur.fetchone()[0]
    
    if weather_count == 0:
        print("WARNING: No weather data found in database!")
        return []
    
    cur.execute("""
        SELECT 
            s.name as state,
            c.name,
            c.in_state_tuition,
            c.completion_rate,
            c.earnings_10yr,
            AVG(w.temp_max) as avg_temp_max,
            AVG(w.temp_min) as avg_temp_min,
            AVG(w.precip_sum) as avg_precip
        FROM colleges c
        JOIN state_provinces s ON c.state_id = s.id
        JOIN daily_weather w ON c.id = w.college_id
        WHERE c.completion_rate IS NOT NULL
          AND c.completion_rate > 0
        GROUP BY c.id
        HAVING avg_temp_max IS NOT NULL
    """)
    
    results = cur.fetchall()
    
    if len(results) == 0:
        print("WARNING: JOIN returned no results!")
        return []
    
    columns = ['state', 'name', 'in_state_tuition', 'completion_rate', 
               'earnings_10yr', 'avg_temp_max', 'avg_temp_min', 'avg_precip']
    
    return [dict(zip(columns, row)) for row in results]


def calculate_country_uni_counts(conn):
    cur = conn.cursor()
    
    cur.execute("""
        SELECT c.name as country, COUNT(*) as university_count
        FROM universities_world u
        JOIN countries c ON u.country_id = c.id
        GROUP BY u.country_id, c.name
        ORDER BY university_count DESC
    """)
    
    results = cur.fetchall()
    return [{'country': row[0], 'count': row[1]} for row in results]


def plot_state_tuition(state_stats, filename='visualizations/state_tuition.png'):
    valid_states = [s for s in state_stats if s['avg_in_state_tuition'] is not None]
    top_states = sorted(valid_states, key=lambda x: x['avg_in_state_tuition'], reverse=True)[:15]
    
    if len(top_states) == 0:
        print("No valid tuition data for visualization")
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
    print(f"Saved: {filename}")


def plot_tuition_vs_completion(conn, filename='visualizations/tuition_vs_completion.png'):
    cur = conn.cursor()
    cur.execute("""
        SELECT in_state_tuition, completion_rate
        FROM colleges
        WHERE in_state_tuition IS NOT NULL 
          AND in_state_tuition > 0
          AND completion_rate IS NOT NULL
          AND completion_rate > 0
    """)
    data = cur.fetchall()
    
    if len(data) < 2:
        print("Not enough data for tuition vs completion plot")
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
    print(f"Saved: {filename}")


def plot_climate_vs_completion(climate_stats, filename='visualizations/climate_vs_completion.png'):
    if not climate_stats or len(climate_stats) == 0:
        print("No climate data available for visualization")
        return
    
    valid_data = [(s['avg_temp_max'], (s['completion_rate'] or 0) * 100) 
                  for s in climate_stats 
                  if s['avg_temp_max'] is not None and s['completion_rate'] is not None]
    
    if len(valid_data) < 2:
        print(f"Not enough valid data points for climate visualization")
        return
    
    temps, completions = zip(*valid_data)
    
    fig, ax = plt.subplots(figsize=(10, 6))
    scatter = ax.scatter(temps, completions, alpha=0.6, c='coral',
                        edgecolors='darkred', linewidth=0.5, s=60)
    
    z = np.polyfit(temps, completions, 1)
    p = np.poly1d(z)
    x_line = np.linspace(min(temps), max(temps), 100)
    ax.plot(x_line, p(x_line), "b--", alpha=0.8, linewidth=2, label='Trend Line')
    
    ax.set_xlabel('Average Max Temperature (C)', fontsize=12)
    ax.set_ylabel('Completion Rate (%)', fontsize=12)
    ax.set_title('Climate vs. College Completion Rate', fontsize=14, fontweight='bold')
    ax.legend()
    
    corr = np.corrcoef(temps, completions)[0, 1]
    ax.text(0.05, 0.95, f'Correlation: {corr:.3f}', transform=ax.transAxes,
            fontsize=10, verticalalignment='top', 
            bbox=dict(boxstyle='round', facecolor='lightcyan', alpha=0.8))
    
    if abs(corr) < 0.3:
        interpretation = "Weak correlation"
    elif abs(corr) < 0.7:
        interpretation = "Moderate correlation"
    else:
        interpretation = "Strong correlation"
    
    ax.text(0.05, 0.88, interpretation, transform=ax.transAxes,
            fontsize=9, verticalalignment='top', style='italic')
    
    plt.tight_layout()
    plt.savefig(filename, dpi=150)
    plt.close()
    print(f"Saved: {filename}")


def plot_universities_per_country(country_counts, filename='visualizations/universities_per_country.png'):
    if not country_counts:
        print("No university data for visualization")
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
    print(f"Saved: {filename}")


def plot_earnings_by_tuition_category(conn, filename='visualizations/bonus_earnings_by_tuition.png'):
    cur = conn.cursor()
    
    cur.execute("""
        SELECT in_state_tuition, earnings_10yr
        FROM colleges
        WHERE in_state_tuition IS NOT NULL 
          AND in_state_tuition > 0
          AND earnings_10yr IS NOT NULL
          AND earnings_10yr > 0
    """)
    
    data = cur.fetchall()
    
    if len(data) < 10:
        print("Not enough data for earnings visualization")
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
    
    print(f"Saved BONUS: {filename}")


def plot_state_completion_heatmap(conn, filename='visualizations/bonus_state_heatmap.png'):
    cur = conn.cursor()

    cur.execute("""
        SELECT 
            s.name as state,
            COUNT(*) as num_colleges,
            AVG(c.completion_rate) as avg_completion,
            AVG(c.in_state_tuition) as avg_tuition
        FROM colleges c
        JOIN state_provinces s ON c.state_id = s.id
        WHERE c.state_id IS NOT NULL 
          AND c.completion_rate IS NOT NULL
          AND c.completion_rate > 0
        GROUP BY c.state_id, s.name
        HAVING num_colleges >= 1
        ORDER BY num_colleges DESC
    """)
    
    results = cur.fetchall()
    
    if len(results) < 3:
        print("Not enough states for heatmap visualization")
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
    
    print(f"Saved BONUS: {filename}")


def write_results_to_file(state_stats, climate_stats, country_counts, correlations, 
                          filename='results_summary.txt'):
    with open(filename, 'w') as f:
        f.write("=" * 60 + "\n")
        f.write("SI 201 FINAL PROJECT - RESULTS SUMMARY\n")
        f.write("=" * 60 + "\n\n")
        
        f.write("SECTION 1: STATE-LEVEL COLLEGE STATISTICS\n")
        f.write("-" * 60 + "\n")
        f.write(f"{'State':<8} {'# Colleges':<12} {'Avg Tuition':<18} {'Completion %':<15}\n")
        f.write("-" * 60 + "\n")
        
        for state in state_stats[:20]:
            tuition = f"${int(state['avg_in_state_tuition']):,}" if state['avg_in_state_tuition'] else "N/A"
            completion = f"{(state['avg_completion_rate'] or 0) * 100:.1f}%" if state['avg_completion_rate'] else "N/A"
            f.write(f"{state['state']:<8} {state['num_colleges']:<12} {tuition:<18} {completion:<15}\n")
        
        f.write(f"\nTotal states analyzed: {len(state_stats)}\n\n")
        
        f.write("SECTION 2: CORRELATION ANALYSIS\n")
        f.write("-" * 60 + "\n")
        if correlations:
            f.write(f"Tuition vs Completion Rate:  {correlations.get('tuition_vs_completion', 'N/A'):.4f}\n")
            f.write(f"Tuition vs 10-Year Earnings: {correlations.get('tuition_vs_earnings', 'N/A'):.4f}\n")
            f.write(f"Completion vs Earnings:      {correlations.get('completion_vs_earnings', 'N/A'):.4f}\n")
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
            f.write(f"Average max temperature: {avg_temp:.1f}C\n")
            f.write(f"Average precipitation: {avg_precip:.2f} mm\n")
            f.write(f"Average completion rate: {avg_completion * 100:.1f}%\n")
        else:
            f.write("NO CLIMATE DATA AVAILABLE\n")
        
        f.write("\n")
        
        f.write("SECTION 4: UNIVERSITIES BY COUNTRY\n")
        f.write("-" * 60 + "\n")
        if country_counts:
            for country in country_counts[:15]:
                f.write(f"{country['country']:<30} {country['count']:>5} universities\n")
            f.write(f"\nTotal countries: {len(country_counts)}\n")
        else:
            f.write("No university data available\n")
        
        f.write("\n" + "=" * 60 + "\n")
        f.write("END OF REPORT\n")
        f.write("=" * 60 + "\n")
    
    print(f"Results written to: {filename}")


def main():
    if not os.path.exists('visualizations'):
        os.makedirs('visualizations')
    
    conn = sqlite3.connect(DATABASE_NAME)
    
    print("=" * 60)
    print("SI 201 FINAL PROJECT - DATA PROCESSING")
    print("=" * 60)
    
    print("\n=== CURRENT DATABASE STATUS ===")
    get_table_counts(conn)
    
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM colleges")
    college_count = cur.fetchone()[0]
    
    cur.execute("SELECT COUNT(*) FROM daily_weather")
    weather_count = cur.fetchone()[0]
    
    cur.execute("SELECT COUNT(*) FROM universities_world")
    uni_count = cur.fetchone()[0]
    
    print("\n=== DATA COLLECTION STATUS ===")
    print(f"Colleges: {college_count}/100 {'COMPLETE' if college_count >= 100 else 'NEEDS MORE'}")
    print(f"Weather Records: {weather_count}/100 {'COMPLETE' if weather_count >= 100 else 'NEEDS MORE'}")
    print(f"Universities: {uni_count}/100 {'COMPLETE' if uni_count >= 100 else 'NEEDS MORE'}")
    
    print("\n=== RUNNING CALCULATIONS ===")
    
    print("Calculating state-level statistics...")
    state_stats = calculate_state_level_stats(conn)
    print(f"Found data for {len(state_stats)} states")
    
    print("Calculating climate and completion data...")
    climate_stats = calculate_climate_and_completion(conn)
    if climate_stats:
        print(f"Found {len(climate_stats)} colleges with climate data")
    else:
        print("NO CLIMATE DATA - Run gather_weather.py!")
    
    print("Calculating country university counts...")
    country_counts = calculate_country_uni_counts(conn)
    print(f"Found universities in {len(country_counts)} countries")
    
    print("Calculating correlations...")
    correlations = calculate_correlations(conn)
    print(f"Computed {len(correlations)} correlations")
    
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
        print("Skipping climate visualization - no data available")
    
    if country_counts:
        print("Creating universities per country chart...")
        plot_universities_per_country(country_counts)
    
    print("\n=== CREATING BONUS VISUALIZATIONS ===")
    print("Creating earnings by tuition category box plot...")
    plot_earnings_by_tuition_category(conn)
    
    print("Creating state completion heat map...")
    plot_state_completion_heatmap(conn)
    
    print("\n=== WRITING RESULTS FILE ===")
    write_results_to_file(state_stats, climate_stats, country_counts, correlations)
    
    print("\n" + "=" * 60)
    print("PROCESSING COMPLETE!")
    print("=" * 60)
    
    print("\nGenerated visualizations:")
    print("  Required:")
    print("    - state_tuition.png")
    print("    - tuition_vs_completion.png")
    print("    - climate_vs_completion.png")
    print("    - universities_per_country.png")
    print("  BONUS:")
    print("    - bonus_earnings_by_tuition.png")
    print("    - bonus_state_heatmap.png")
    
    conn.close()


if __name__ == "__main__":
    main()