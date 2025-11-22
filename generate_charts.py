#!/usr/bin/env python3
"""
ETender Data Analysis and Chart Generation
Analyzes e-tender data from Azerbaijan and generates insightful visualizations
"""

import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from datetime import datetime
import numpy as np
import warnings
warnings.filterwarnings('ignore')

# Set style for better-looking charts
plt.style.use('seaborn-v0_8-darkgrid')
plt.rcParams['figure.figsize'] = (12, 7)
plt.rcParams['font.size'] = 10
plt.rcParams['axes.titlesize'] = 14
plt.rcParams['axes.labelsize'] = 11

# Read the data
print("Loading data...")
df = pd.read_csv('etender_data_20250928_220005.csv')

# Data preprocessing
print(f"Total records: {len(df)}")
df['publishDate'] = pd.to_datetime(df['publishDate'], format='ISO8601', errors='coerce')
df['endDate'] = pd.to_datetime(df['endDate'], format='ISO8601', errors='coerce')
df['duration_days'] = (df['endDate'] - df['publishDate']).dt.days

# Event type mapping (based on common tender types)
event_type_map = {
    7: 'Open Tender',
    6: 'Limited Tender',
    5: 'Request for Quotation',
    4: 'Single Source',
    3: 'Framework Agreement',
    2: 'Two-Stage Tender',
    1: 'Other'
}
df['eventTypeName'] = df['eventType'].map(event_type_map).fillna('Unknown')

# Status mapping
status_map = {
    1: 'Active',
    2: 'Closed',
    3: 'Cancelled',
    4: 'Awarded',
    0: 'Draft'
}
df['eventStatusName'] = df['eventStatus'].map(status_map).fillna('Unknown')

# Check if awarded
df['isAwarded'] = df['awardedParticipantName'].notna()

print("\nGenerating charts...")

# ===== CHART 1: Tenders by Event Type =====
print("1. Generating Tenders by Event Type...")
plt.figure(figsize=(12, 7))
event_counts = df['eventTypeName'].value_counts().sort_values(ascending=True)
colors = plt.cm.Set3(np.linspace(0, 1, len(event_counts)))
bars = plt.barh(range(len(event_counts)), event_counts.values, color=colors)
plt.yticks(range(len(event_counts)), event_counts.index)
plt.xlabel('Number of Tenders', fontweight='bold')
plt.title('Distribution of Tenders by Event Type', fontweight='bold', pad=20, fontsize=16)
plt.grid(axis='x', alpha=0.3)

# Add value labels
for i, (value, bar) in enumerate(zip(event_counts.values, bars)):
    plt.text(value + 20, i, f'{value:,}', va='center', fontweight='bold')

plt.tight_layout()
plt.savefig('charts/01_tenders_by_event_type.png', dpi=300, bbox_inches='tight')
plt.close()

# ===== CHART 2: Tenders by Status =====
print("2. Generating Tenders by Status...")
plt.figure(figsize=(10, 10))
status_counts = df['eventStatusName'].value_counts()
colors = ['#2ecc71', '#e74c3c', '#f39c12', '#3498db', '#9b59b6'][:len(status_counts)]
wedges, texts, autotexts = plt.pie(status_counts.values,
                                     labels=status_counts.index,
                                     autopct='%1.1f%%',
                                     colors=colors,
                                     startangle=90,
                                     textprops={'fontsize': 12, 'fontweight': 'bold'})
for autotext in autotexts:
    autotext.set_color('white')
    autotext.set_fontsize(11)
plt.title('Tender Status Distribution', fontweight='bold', pad=20, fontsize=16)
plt.axis('equal')
plt.tight_layout()
plt.savefig('charts/02_tenders_by_status.png', dpi=300, bbox_inches='tight')
plt.close()

# ===== CHART 3: Top 15 Buyer Organizations =====
print("3. Generating Top Buyer Organizations...")
plt.figure(figsize=(14, 9))
top_buyers = df['buyerOrganizationName'].value_counts().head(15)
colors = plt.cm.viridis(np.linspace(0.2, 0.9, len(top_buyers)))
bars = plt.barh(range(len(top_buyers)), top_buyers.values, color=colors)
plt.yticks(range(len(top_buyers)), [name[:70] + '...' if len(name) > 70 else name for name in top_buyers.index], fontsize=9)
plt.xlabel('Number of Tenders', fontweight='bold')
plt.title('Top 15 Most Active Buyer Organizations', fontweight='bold', pad=20, fontsize=16)
plt.grid(axis='x', alpha=0.3)

# Add value labels
for i, (value, bar) in enumerate(zip(top_buyers.values, bars)):
    plt.text(value + 0.5, i, f'{value}', va='center', fontweight='bold', fontsize=9)

plt.tight_layout()
plt.savefig('charts/03_top_buyer_organizations.png', dpi=300, bbox_inches='tight')
plt.close()

# ===== CHART 4: Tenders Timeline (by month) =====
print("4. Generating Tenders Timeline...")
plt.figure(figsize=(14, 7))
df['publish_month'] = df['publishDate'].dt.to_period('M')
timeline = df.groupby('publish_month').size().reset_index(name='count')
timeline['publish_month'] = timeline['publish_month'].astype(str)

plt.plot(range(len(timeline)), timeline['count'], marker='o', linewidth=2.5,
         markersize=8, color='#3498db', markerfacecolor='#e74c3c', markeredgewidth=2)
plt.fill_between(range(len(timeline)), timeline['count'], alpha=0.3, color='#3498db')
plt.xticks(range(len(timeline)), timeline['publish_month'], rotation=45, ha='right')
plt.xlabel('Month', fontweight='bold')
plt.ylabel('Number of Tenders', fontweight='bold')
plt.title('Tender Publication Timeline', fontweight='bold', pad=20, fontsize=16)
plt.grid(True, alpha=0.3)
plt.tight_layout()
plt.savefig('charts/04_tenders_timeline.png', dpi=300, bbox_inches='tight')
plt.close()

# ===== CHART 5: Tender Duration Analysis =====
print("5. Generating Tender Duration Analysis...")
plt.figure(figsize=(12, 7))
duration_valid = df[df['duration_days'] > 0]['duration_days']
plt.hist(duration_valid, bins=30, color='#3498db', edgecolor='black', alpha=0.7)
plt.axvline(duration_valid.mean(), color='#e74c3c', linestyle='--', linewidth=2, label=f'Mean: {duration_valid.mean():.1f} days')
plt.axvline(duration_valid.median(), color='#2ecc71', linestyle='--', linewidth=2, label=f'Median: {duration_valid.median():.1f} days')
plt.xlabel('Duration (Days)', fontweight='bold')
plt.ylabel('Number of Tenders', fontweight='bold')
plt.title('Tender Duration Distribution (Publish to End Date)', fontweight='bold', pad=20, fontsize=16)
plt.legend(fontsize=11)
plt.grid(axis='y', alpha=0.3)
plt.tight_layout()
plt.savefig('charts/05_tender_duration_analysis.png', dpi=300, bbox_inches='tight')
plt.close()

# ===== CHART 6: Tender Status by Event Type =====
print("6. Generating Tender Status by Event Type...")
fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(16, 7))

# Status distribution pie chart
status_counts = df['eventStatusName'].value_counts()
colors_status = plt.cm.Set3(np.linspace(0, 1, len(status_counts)))
wedges, texts, autotexts = ax1.pie(status_counts.values,
                                     labels=status_counts.index,
                                     autopct='%1.1f%%',
                                     colors=colors_status,
                                     startangle=90,
                                     textprops={'fontsize': 11, 'fontweight': 'bold'})
for autotext in autotexts:
    autotext.set_color('black')
    autotext.set_fontsize(10)
ax1.set_title('Overall Tender Status Distribution', fontweight='bold', fontsize=14)
ax1.axis('equal')

# Tender count by event type and status (stacked bar)
status_by_type = df.groupby(['eventTypeName', 'eventStatusName']).size().unstack(fill_value=0)
status_by_type_sorted = status_by_type.loc[status_by_type.sum(axis=1).sort_values(ascending=True).index]

# Plot stacked horizontal bar chart
status_by_type_sorted.plot(kind='barh', stacked=True, ax=ax2,
                           color=plt.cm.Set3(np.linspace(0, 1, len(status_by_type_sorted.columns))),
                           edgecolor='black', linewidth=0.5)
ax2.set_xlabel('Number of Tenders', fontweight='bold')
ax2.set_ylabel('Event Type', fontweight='bold')
ax2.set_title('Tender Count by Event Type and Status', fontweight='bold', fontsize=14)
ax2.legend(title='Status', bbox_to_anchor=(1.05, 1), loc='upper left', fontsize=9)
ax2.grid(axis='x', alpha=0.3)

plt.tight_layout()
plt.savefig('charts/06_status_by_event_type.png', dpi=300, bbox_inches='tight')
plt.close()

# ===== CHART 7: Day of Week Analysis =====
print("7. Generating Day of Week Analysis...")
plt.figure(figsize=(12, 7))
df['day_of_week'] = df['publishDate'].dt.day_name()
day_order = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
day_counts = df['day_of_week'].value_counts().reindex(day_order, fill_value=0)
colors_day = plt.cm.Spectral(np.linspace(0, 1, len(day_counts)))
bars = plt.bar(range(len(day_counts)), day_counts.values, color=colors_day, edgecolor='black', linewidth=1.5)
plt.xticks(range(len(day_counts)), day_counts.index, rotation=45, ha='right')
plt.ylabel('Number of Tenders', fontweight='bold')
plt.title('Tender Publications by Day of Week', fontweight='bold', pad=20, fontsize=16)
plt.grid(axis='y', alpha=0.3)

# Add value labels
for i, (value, bar) in enumerate(zip(day_counts.values, bars)):
    plt.text(i, value + 10, f'{value}', ha='center', fontweight='bold')

plt.tight_layout()
plt.savefig('charts/07_day_of_week_analysis.png', dpi=300, bbox_inches='tight')
plt.close()

# ===== CHART 8: Top Categories Analysis =====
print("8. Generating Top Categories Analysis...")
# Extract main category from event name (first few words)
def extract_category(name):
    if pd.isna(name):
        return 'Unknown'
    words = str(name).split()[:3]
    return ' '.join(words)

df['category'] = df['eventName'].apply(extract_category)
top_categories = df['category'].value_counts().head(12)

plt.figure(figsize=(14, 8))
colors_cat = plt.cm.tab20(np.linspace(0, 1, len(top_categories)))
bars = plt.barh(range(len(top_categories)), top_categories.values, color=colors_cat)
plt.yticks(range(len(top_categories)), [cat[:60] + '...' if len(cat) > 60 else cat for cat in top_categories.index], fontsize=9)
plt.xlabel('Number of Tenders', fontweight='bold')
plt.title('Top 12 Tender Categories (by Event Name)', fontweight='bold', pad=20, fontsize=16)
plt.grid(axis='x', alpha=0.3)

# Add value labels
for i, (value, bar) in enumerate(zip(top_categories.values, bars)):
    plt.text(value + 1, i, f'{value}', va='center', fontweight='bold')

plt.tight_layout()
plt.savefig('charts/08_top_categories.png', dpi=300, bbox_inches='tight')
plt.close()

# ===== Generate Summary Statistics =====
print("\n" + "="*60)
print("SUMMARY STATISTICS")
print("="*60)

summary_stats = {
    'Total Tenders': len(df),
    'Unique Buyers': df['buyerOrganizationName'].nunique(),
    'Awarded Tenders': df['isAwarded'].sum(),
    'Award Rate': f"{(df['isAwarded'].sum() / len(df) * 100):.2f}%",
    'Average Duration (days)': f"{df[df['duration_days'] > 0]['duration_days'].mean():.1f}",
    'Median Duration (days)': f"{df[df['duration_days'] > 0]['duration_days'].median():.1f}",
    'Most Common Event Type': df['eventTypeName'].mode()[0],
    'Most Active Buyer': df['buyerOrganizationName'].mode()[0] if len(df) > 0 else 'N/A',
    'Date Range': f"{df['publishDate'].min().strftime('%Y-%m-%d')} to {df['publishDate'].max().strftime('%Y-%m-%d')}"
}

for key, value in summary_stats.items():
    print(f"{key:.<40} {value}")

print("\n✓ All charts generated successfully in /charts directory!")
print(f"✓ Total charts created: 8")
