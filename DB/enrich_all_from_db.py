import pandas as pd
import psycopg2
import re
from db_setup import get_connection, close_connection

# Trait and tag keywords
trait_keywords = {
    "adventure": ["hike", "trek", "adventure", "zipline", "kayak", "climb", "safari", "rafting", "outdoor"],
    "relax": ["relax", "spa", "quiet", "peaceful", "calm", "serene", "retreat"],
    "nature": ["park", "mountain", "forest", "valley", "lake", "trail", "wildlife", "waterfall"],
    "culture": ["museum", "historic", "heritage", "culture", "tradition", "site", "temple", "art"],
    "luxury": ["luxury", "fine dining", "resort", "exclusive", "high-end", "5-star", "boutique", "premium"]
}

# Keywords to tag mappings
tag_keywords = {
    "beach": ["beach", "coast", "island", "seaside", "shore", "waves", "bay", "lagoon"],
    "mountain": ["mountain", "hill", "peak", "range", "ridge", "summit"],
    "desert": ["desert", "dune", "sands", "arid", "oasis"],
    "spiritual": ["temple", "ashram", "pilgrimage", "spiritual", "monastery", "holy", "divine"],
    "wildlife": ["safari", "national park", "jungle", "wildlife", "zoo", "reserve", "nature trail", "animal"],
    "historic": ["fort", "ruins", "monument", "castle", "citadel", "tomb"],
    "urban": ["shopping", "nightlife", "skyline", "market", "restaurant", "bar", "café", "bustling"]
}

def extract_traits_and_tags(text):
    text = text.lower() if isinstance(text, str) else ""
    traits = {key: 0 for key in trait_keywords}
    tags = set()

    for trait, keywords in trait_keywords.items():
        for word in keywords:
            if re.search(rf"\\b{re.escape(word)}\\b", text):
                traits[trait] += 1

    for tag, keywords in tag_keywords.items():
        for word in keywords:
            if re.search(rf"\\b{re.escape(word)}\\b", text):
                tags.add(tag)

    return list(tags), traits

# Connect to DB
conn = get_connection()
cursor = conn.cursor()

cursor.execute("SELECT id, description FROM destinations")
rows = cursor.fetchall()

updated = 0

for row in rows:
    dest_id, desc = row
    if not desc or not isinstance(desc, str) or desc.strip() == "":
        continue

    tags, traits = extract_traits_and_tags(desc)

    try:
        cursor.execute("""
            UPDATE destinations SET
                tags = %s,
                adventure = %s,
                relax = %s,
                nature = %s,
                culture = %s,
                luxury = %s
            WHERE id = %s
        """, (
            tags,
            traits["adventure"],
            traits["relax"],
            traits["nature"],
            traits["culture"],
            traits["luxury"],
            dest_id
        ))
        updated += 1
    except Exception as e:
        print(f"❌ Failed to update id {dest_id}: {e}")

conn.commit()
close_connection(conn, cursor)
print(f"✅ Enriched {updated} destinations from existing DB records.")
