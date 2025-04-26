import pandas as pd
import os
from db_setup import get_connection, close_connection

# File path for the new USA cities with enriched descriptions
csv_file = 'datasets/uscities_with_descriptions.csv'

if not os.path.exists(csv_file):
    exit(f"🛑 File not found: {csv_file}")

# Load the CSV
df = pd.read_csv(csv_file)

# Prepare data
print("🧹 Preparing data...")
df["name"] = df["city"].str.strip()
df["city"] = df["city"].str.strip()
df["state"] = df["state_name"].str.strip()
df["country"] = "USA"
df["type"] = ""
df["tags"] = [[] for _ in range(len(df))]
df["rating"] = None
df["reviews"] = None

# Placeholder traits until enrichment
df["adventure"] = 2
df["relax"] = 3
df["nature"] = 3
df["culture"] = 4
df["luxury"] = 2

# Connect to DB
conn = get_connection()
if not conn:
    exit("🛑 Could not establish DB connection.")

cursor = conn.cursor()

inserted = 0
skipped = 0

for _, row in df.iterrows():
    # Check if destination already exists
    cursor.execute("""
        SELECT 1 FROM destinations
        WHERE LOWER(name) = %s AND LOWER(city) = %s AND LOWER(state) = %s AND LOWER(country) = %s
    """, (
        row["name"].lower(),
        row["city"].lower(),
        row["state"].lower(),
        row["country"].lower()
    ))
    
    if cursor.fetchone():
        skipped += 1
        continue

    try:
        cursor.execute("""
            INSERT INTO destinations (
                name, city, state, country, type,
                tags, rating, reviews, adventure, relax,
                nature, culture, luxury, description
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, (
            row["name"],
            row["city"],
            row["state"],
            row["country"],
            row["type"],
            row["tags"],
            row["rating"],
            row["reviews"],
            row["adventure"],
            row["relax"],
            row["nature"],
            row["culture"],
            row["luxury"],
            row["description"]
        ))
        inserted += 1
    except Exception as e:
        print(f"❌ Error inserting {row['name']}: {e}")

conn.commit()
close_connection(conn, cursor)

print(f"✅ Done: {inserted} new cities inserted, {skipped} skipped (already exists)")
