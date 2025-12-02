"""
Export User Rating Profile
This script calculates user engagement metrics and ratings from Matomo analytics data,
then exports the results to a PostgreSQL table for use in Superset dashboards.
"""

import pandas as pd
from sqlalchemy import create_engine, text

# Database configuration
DB_URL = "postgresql://postgres:<yourpassword>@34.93.93.62:5432/matomo_analytics"
SOURCE_TABLE = "matomo_analytics_dashboard"
TARGET_TABLE = "user_rating_profile"

# Connect to PostgreSQL
en = create_engine(DB_URL)

print("Loading user session data...")

# Read all user session data
df = pd.read_sql(f"""
    SELECT 
        idvisitor,
        idvisit,
        full_url,
        feature,
        domain,
        server_time,
        visit_date,
        pageview_position,
        time_spent_ref_action,
        visit_total_actions,
        location_country,
        location_city,
        is_bounce,
        has_engagement
    FROM {SOURCE_TABLE}
    ORDER BY idvisitor, server_time
""", en)

print(f"Loaded {len(df)} rows")

# Calculate user-level metrics
print("Calculating user-level metrics...")

user_metrics = df.groupby('idvisitor').agg({
    'idvisit': 'nunique',  # Total number of visits
    'full_url': 'count',  # Total pageviews
    'time_spent_ref_action': 'sum',  # Total time spent
    'visit_total_actions': 'max',  # Max actions in a single visit
    'is_bounce': 'sum',  # Number of bounced sessions
    'has_engagement': 'sum',  # Number of engaged sessions
    'feature': lambda x: x.value_counts().index[0] if len(x) > 0 else None,  # Most visited feature
    'domain': lambda x: x.value_counts().index[0] if len(x) > 0 else None,  # Most visited domain
    'location_city': 'first',  # User's city
    'location_country': 'first',  # User's country
    'server_time': ['min', 'max']  # First and last visit
}).reset_index()

# Flatten column names
user_metrics.columns = [
    'idvisitor',
    'total_visits',
    'total_pageviews',
    'total_time_spent',
    'max_actions_per_visit',
    'bounced_sessions',
    'engaged_sessions',
    'favorite_feature',
    'favorite_domain',
    'location_city',
    'location_country',
    'first_visit',
    'last_visit'
]

# Calculate derived metrics
user_metrics['avg_pageviews_per_visit'] = (
    user_metrics['total_pageviews'] / user_metrics['total_visits']
).round(2)

user_metrics['avg_time_per_visit'] = (
    user_metrics['total_time_spent'] / user_metrics['total_visits']
).round(2)

user_metrics['bounce_rate'] = (
    (user_metrics['bounced_sessions'] / user_metrics['total_visits']) * 100
).round(2)

user_metrics['engagement_rate'] = (
    (user_metrics['engaged_sessions'] / user_metrics['total_visits']) * 100
).round(2)

# Calculate days active (between first and last visit)
user_metrics['days_active'] = (
    pd.to_datetime(user_metrics['last_visit']) - pd.to_datetime(user_metrics['first_visit'])
).dt.days

# Calculate user rating (0-100 scale)
print("Calculating user rating scores...")

# Normalize metrics for scoring (0-1 scale)
def normalize(series):
    min_val = series.min()
    max_val = series.max()
    if max_val == min_val:
        return pd.Series([0.5] * len(series))
    return (series - min_val) / (max_val - min_val)

# Positive factors (higher is better)
user_metrics['score_visits'] = normalize(user_metrics['total_visits']) * 20
user_metrics['score_pageviews'] = normalize(user_metrics['total_pageviews']) * 20
user_metrics['score_engagement'] = (user_metrics['engagement_rate'] / 100) * 30
user_metrics['score_time'] = normalize(user_metrics['avg_time_per_visit']) * 20

# Negative factor (lower bounce rate is better)
user_metrics['score_bounce'] = (1 - user_metrics['bounce_rate'] / 100) * 10

# Calculate final rating (0-100)
user_metrics['user_rating'] = (
    user_metrics['score_visits'] +
    user_metrics['score_pageviews'] +
    user_metrics['score_engagement'] +
    user_metrics['score_time'] +
    user_metrics['score_bounce']
).round(2)

# Classify users by rating
def classify_user(rating):
    if rating >= 80:
        return 'Champion'
    elif rating >= 60:
        return 'Loyal'
    elif rating >= 40:
        return 'Potential'
    elif rating >= 20:
        return 'At Risk'
    else:
        return 'Lost'

user_metrics['user_segment'] = user_metrics['user_rating'].apply(classify_user)

# Drop intermediate score columns
user_metrics = user_metrics.drop(columns=[
    'score_visits', 'score_pageviews', 'score_engagement', 'score_time', 'score_bounce'
])

print(f"Calculated metrics for {len(user_metrics)} users")

# Create table in PostgreSQL
print("Creating user rating table...")

with en.connect() as conn:
    conn.execute(text(f"DROP TABLE IF EXISTS {TARGET_TABLE}"))
    conn.execute(text(f"""
        CREATE TABLE {TARGET_TABLE} (
            id SERIAL PRIMARY KEY,
            idvisitor BYTEA NOT NULL,
            total_visits INTEGER,
            total_pageviews INTEGER,
            total_time_spent FLOAT,
            max_actions_per_visit INTEGER,
            bounced_sessions INTEGER,
            engaged_sessions INTEGER,
            favorite_feature VARCHAR(128),
            favorite_domain VARCHAR(128),
            location_city VARCHAR(64),
            location_country VARCHAR(8),
            first_visit TIMESTAMP,
            last_visit TIMESTAMP,
            avg_pageviews_per_visit FLOAT,
            avg_time_per_visit FLOAT,
            bounce_rate FLOAT,
            engagement_rate FLOAT,
            days_active INTEGER,
            user_rating FLOAT,
            user_segment VARCHAR(20)
        )
    """))
    conn.commit()
    print(f"Created table '{TARGET_TABLE}'")

# Insert data
user_metrics.to_sql(TARGET_TABLE, en, if_exists='append', index=False, method='multi')
print(f"Inserted {len(user_metrics)} rows into '{TARGET_TABLE}'")

# Show statistics
print("\nUser Rating Distribution:")
print(user_metrics['user_segment'].value_counts().sort_index())

print("\nSample data:")
print(user_metrics.head(10))

print("\nTop 10 Users by Rating:")
print(user_metrics.nlargest(10, 'user_rating')[['idvisitor', 'user_rating', 'user_segment', 'total_visits']])

en.dispose()
print("\nDone! User rating table ready for Superset")
print(f"\nNext steps:")
print(f"1. Go to Superset -> Data -> Datasets")
print(f"2. Add '{TARGET_TABLE}' as a new dataset")
print(f"3. Create charts:")
print(f"   - Pie Chart: User segment distribution")
print(f"   - Table: Top users by rating")
print(f"   - Scatter: Rating vs Total Visits")
print(f"   - Bar Chart: Avg rating by location/feature")