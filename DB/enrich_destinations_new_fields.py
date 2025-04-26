import pandas as pd
import psycopg2
import re
from fuzzywuzzy import fuzz
from db_setup import get_connection, close_connection

# Load destinations.csv
destinations_df = pd.read_csv("datasets/destinations.csv", encoding="ISO-8859-1")

# Preprocess columns
destinations_df["Destination"] = destinations_df["Destination"].str.strip().str.lower()
destinations_df.rename(columns={
    "Description": "description",
    "Cost of Living": "cost",
    "Country": "country",
    "Region": "region",
    "Cultural Significance": "cultural"
}, inplace=True)

luxury_map = {"Low": 1, "Medium": 3, "Medium-high": 4, "High": 5}
destinations_df["luxury_score"] = destinations_df["cost"].map(lambda x: luxury_map.get(str(x).strip().title(), None) if pd.notna(x) else None)

# Define traits and tag keywords
trait_keywords = {
    "adventure": ["hike", "trek", "adventure", "zipline", "kayak", "climb", "safari", "rafting", "outdoor"],
    "relax": ["relax", "spa", "quiet", "peaceful", "calm", "serene", "retreat"],
    "nature": ["park", "mountain", "forest", "valley", "lake", "trail", "wildlife", "waterfall"],
    "culture": ["museum", "historic", "heritage", "culture", "tradition", "site"],
    "luxury": ["luxury", "fine dining", "resort", "exclusive", "high-end", "5-star"]
}

tag_keywords = {
    "beach": ["beach", "coast", "island", "seaside", "shore", "waves", "bay", "lagoon"],
    "mountain": ["mountain", "hill", "peak", "range", "ridge", "summit"],
    "desert": ["desert", "dune", "sands", "arid", "oasis"],
    "spiritual": ["temple", "ashram", "pilgrimage", "spiritual", "monastery", "holy", "divine"],
    "wildlife": ["safari", "national park", "jungle", "wildlife", "zoo", "reserve", "nature trail", "animal"],
    "historic": ["fort", "ruins", "monument", "castle", "citadel", "tomb"],
    "urban": ["shopping", "nightlife", "skyline", "market", "restaurant", "bar", "café", "street food", "bustling"]
}

def extract_traits_and_tags(text):
    text = str(text).lower() if pd.notna(text) else ""
    traits = {key: 0 for key in trait_keywords}
    tags = set()

    for trait, words in trait_keywords.items():
        for word in words:
            if re.search(rf"\b{re.escape(word)}\b", text):
                traits[trait] += 1

    for tag, words in tag_keywords.items():
        for word in words:
            if re.search(rf"\b{re.escape(word)}\b", text):
                tags.add(tag)

    return list(tags), traits

# Connect to DB
conn = get_connection()
cursor = conn.cursor()

cursor.execute("SELECT id, name, city, state, country FROM destinations")
rows = cursor.fetchall()

db_lookup = {
    (r[1].strip().lower(), str(r[4]).strip().lower()): r[0]
    for r in rows
}

updated = 0

# Process enrichment
for _, row in destinations_df.iterrows():
    name = row["Destination"]
    country = str(row["country"]).strip().lower()
    state = str(row.get("region", "")).strip()
    desc = str(row["description"]).strip()
    cultural = str(row.get("cultural", "")).strip()
    lux = int(row["luxury_score"]) if pd.notna(row["luxury_score"]) else None

    if pd.notna(cultural) and cultural:
        desc = f"{desc} {cultural}"

    new_tags, traits = extract_traits_and_tags(desc)
    traits["luxury"] = lux or traits["luxury"]

    key = (name, country)
    if key in db_lookup:
        cursor.execute("""
            UPDATE destinations SET
                description = %s,
                tags = %s,
                state = COALESCE(%s, state),
                adventure = %s,
                relax = %s,
                nature = %s,
                culture = %s,
                luxury = %s
            WHERE id = %s
        """, (
            desc,
            new_tags,
            state,
            traits["adventure"],
            traits["relax"],
            traits["nature"],
            traits["culture"],
            traits["luxury"],
            db_lookup[key]
        ))
        updated += 1

conn.commit()
close_connection(conn, cursor)
print(f"✅ Enrichment complete (without category in tags): {updated} destinations updated from destinations.csv")
