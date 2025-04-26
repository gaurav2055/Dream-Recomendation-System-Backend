import pandas as pd
from db_setup import get_connection, close_connection

# Load the expanded Indian travel dataset
csv_file = 'datasets/Expanded_Indian_Travel_Dataset.csv'
df = pd.read_csv(csv_file)

# Prepare data
df["name"] = df["Destination Name"].str.strip()
df["city"] = df["Popular Attraction"].str.strip()
df["state"] = df["State"].str.strip()
df["country"] = "India"
df["type"] = df["Category"].fillna("").str.strip()
df["tags"] = df.apply(
    lambda row: list({
        tag.strip().lower() for tag in [row.get("Category", ""), row.get("Region", "")]
        if tag and isinstance(tag, str)
    }), axis=1
)
df["rating"] = None
df["reviews"] = None

# Placeholder traits
df["adventure"] = 2
df["relax"] = 3
df["nature"] = 3
df["culture"] = 4
df["luxury"] = 2

# Connect to DB
conn = get_connection()
cursor = conn.cursor()

inserted = 0
skipped = 0

for _, row in df.iterrows():
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
            None  # description will be enriched later
        ))
        inserted += 1
    except Exception as e:
        print(f"❌ Error inserting {row['name']}: {e}")

conn.commit()
close_connection(conn, cursor)
print(f"✅ {inserted} new destinations inserted, {skipped} skipped (already exists)")
