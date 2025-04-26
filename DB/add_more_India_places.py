import pandas as pd
from db_setup import get_connection, close_connection

# Load the new places.csv file
csv_file = 'datasets/places.csv'
df = pd.read_csv(csv_file, encoding="ISO-8859-1")

# Prepare data
df["name"] = df["popular_destination"].str.strip()
df["city"] = df["city"].str.strip()
df["state"] = df["state"].str.strip()
df["country"] = "India"
df["type"] = df["interest"].fillna("").str.strip()
df["tags"] = df["interest"].fillna("").apply(lambda x: [tag.strip() for tag in x.split("&") if tag.strip()])
df["rating"] = df["google_rating"]
df["reviews"] = None

# Estimate luxury score from price
luxury_score = lambda p: 1 if p <= 10 else 2 if p <= 30 else 3 if p <= 70 else 4 if p <= 150 else 5
df["luxury"] = df["price_fare"].fillna(0).astype(int).apply(luxury_score)

# Placeholder traits (can be enriched later)
df["adventure"] = 2
df["relax"] = 3
df["nature"] = 3
df["culture"] = 4

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
print(f"✅ {inserted} new places inserted, {skipped} skipped (already exists)")
