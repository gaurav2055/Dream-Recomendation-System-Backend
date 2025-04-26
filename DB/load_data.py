from db_setup import get_connection, close_connection
import pandas as pd
import os

# List of CSV files and their associated country
datasets = [
    ('datasets/cleaned_data_India.csv', "India"),
    ('datasets/cleaned_data_USA.csv', "USA"),
    ('datasets/cleaned_data_Iran.csv', "Iran"),
]

conn = get_connection()
if not conn:
    exit("🛑 Could not establish DB connection.")

cursor = conn.cursor()

for csv_file, country in datasets:
    if not os.path.exists(csv_file):
        print(f"⚠️ File not found: {csv_file}")
        continue

    df = pd.read_csv(csv_file)

    # Clean data
    df["name"] = df["name"].str.strip()
    df["type"] = df["main_category"].fillna("").str.strip()
    df["tags"] = df["categories"].fillna("").apply(lambda x: [tag.strip() for tag in x.split(",") if tag.strip()])

    # Assign placeholder traits for now (can improve later)
    df["adventure"] = 2
    df["relax"] = 3
    df["nature"] = 3
    df["culture"] = 4
    df["luxury"] = 2

    for _, row in df.iterrows():
        try:
            cursor.execute("""
                INSERT INTO destinations (
                    name, city, state, country, type,
                    tags, rating, reviews, adventure, relax,
                    nature, culture, luxury, description
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                row.get("name"),
                row.get("city", None),
                row.get("state", None),
                country,
                row.get("type", None),
                row.get("tags", []),
                row.get("rating", None),
                row.get("reviews", None),
                row["adventure"],
                row["relax"],
                row["nature"],
                row["culture"],
                row["luxury"],
                row.get("description", None)
            ))
        except Exception as e:
            print(f"❌ Error inserting {row.get('name')}: {e}")

    print(f"✅ Finished inserting from {csv_file}\n")

conn.commit()
close_connection(conn, cursor)
print("✅ All data inserted cleanly.")
