import pandas as pd
import psycopg2
import re
from fuzzywuzzy import fuzz
from db_setup import get_connection, close_connection

# Load all datasets
usa_df = pd.read_csv("datasets/cleaned_data_USA_with_descriptions.csv")
iran_df = pd.read_csv("datasets/iran_tourist_pois_cleaned.csv")
holidify_df = pd.read_csv("datasets/holidify.csv")
destinations_df = pd.read_csv("datasets/destinations.csv", encoding="ISO-8859-1")

# Preprocess holidify
holidify_df["City"] = holidify_df["City"].str.strip().str.lower()
holidify_df.rename(columns={"About the city (long Description)": "description"}, inplace=True)

# Preprocess destinations.csv
destinations_df["Destination"] = destinations_df["Destination"].str.strip().str.lower()
destinations_df.rename(columns={"Description": "description", "Cost of Living": "cost", "Country": "country"}, inplace=True)
luxury_map = {"Low": 1, "Medium": 3, "Medium-high": 4, "High": 5}
destinations_df["luxury_score"] = destinations_df["cost"].map(lambda x: luxury_map.get(str(x).strip().title(), None) if pd.notna(x) else None)

# Trait and tag keywords
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

def extract_traits_and_tags(text, existing_tags=None):
    text = str(text).lower() if pd.notna(text) else ""
    traits = {key: 0 for key in trait_keywords}
    tags = set(existing_tags) if existing_tags else set()

    for trait, words in trait_keywords.items():
        for word in words:
            if re.search(rf"\b{re.escape(word)}\b", text):
                traits[trait] += 1

    for tag, words in tag_keywords.items():
        for word in words:
            if re.search(rf"\b{re.escape(word)}\b", text):
                tags.add(tag)

    return list(tags), traits

def match_destination(name, city, state, db_lookup, country):
    key = (name.strip().lower(), str(city).strip().lower(), str(state).strip().lower(), country.lower())
    if key in db_lookup:
        return db_lookup[key]["id"]
    for k in db_lookup:
        if k[3] == country.lower():
            score = fuzz.token_sort_ratio(" ".join(key[:3]), " ".join(k[:3]))
            if score > 90:
                return db_lookup[k]["id"]
    return None

# Connect to DB and fetch all destinations
conn = get_connection()
cursor = conn.cursor()
cursor.execute("SELECT id, name, city, state, country, description, tags FROM destinations")
rows = cursor.fetchall()
db_lookup = {
    (r[1].strip().lower(), str(r[2]).strip().lower(), str(r[3]).strip().lower(), r[4].strip().lower()): {
        "id": r[0],
        "description": r[5],
        "tags": r[6] if r[6] else []
    }
    for r in rows
}

updated, inserted = 0, 0

# Enrich USA
for _, row in usa_df.iterrows():
    name, city, state = row["name"], row["city"], row["state"]
    desc = row["description"]
    tag_text = str(row.get("categories", "")) + " " + str(row.get("main_category", ""))
    tag_list = list(set(tag_text.lower().split(",")))
    traits = extract_traits_and_tags(desc)[1]
    match_id = match_destination(name, city, state, db_lookup, "USA")
    if match_id:
        cursor.execute("""
            UPDATE destinations SET
                description = %s, tags = %s,
                adventure = %s, relax = %s, nature = %s, culture = %s, luxury = %s
            WHERE id = %s
        """, (
            desc, tag_list,
            traits["adventure"], traits["relax"], traits["nature"],
            traits["culture"], traits["luxury"], match_id
        ))
        updated += 1

# Enrich Iran
for _, row in iran_df.iterrows():
    name, city = row["name"], row.get("city", "")
    desc = f"{name} is a place of interest in Iran known for its tourism and cultural value."
    tags = []
    for field in ["tourism", "amenity", "man_made"]:
        val = row.get(field)
        if isinstance(val, str):
            tags.append(val.lower())
    traits = extract_traits_and_tags(" ".join(tags))[1]
    match_id = match_destination(name, city, "", db_lookup, "Iran")
    if match_id:
        cursor.execute("""
            UPDATE destinations SET
                description = %s, tags = %s,
                adventure = %s, relax = %s, nature = %s, culture = %s, luxury = %s
            WHERE id = %s
        """, (
            desc, tags,
            traits["adventure"], traits["relax"], traits["nature"],
            traits["culture"], traits["luxury"], match_id
        ))
        updated += 1

# Enrich Holidify (India)
for _, row in holidify_df.iterrows():
    key = (row["City"], "", "", "india")
    for k in db_lookup:
        if k[0] == row["City"] and k[3] == "india":
            new_tags, traits = extract_traits_and_tags(row["description"], db_lookup[k]["tags"])
            cursor.execute("""
                UPDATE destinations SET
                    description = %s, tags = %s,
                    adventure = %s, relax = %s, nature = %s, culture = %s, luxury = %s
                WHERE id = %s
            """, (
                row["description"], new_tags,
                traits["adventure"], traits["relax"], traits["nature"],
                traits["culture"], traits["luxury"], db_lookup[k]["id"]
            ))
            updated += 1

# Enrich/Add from destinations.csv
for _, row in destinations_df.iterrows():
    name = row["Destination"]
    country = str(row["country"]).strip().lower()
    desc = row["description"]
    print(desc)
    lux = int(row["luxury_score"]) if pd.notna(row["luxury_score"]) else None
    new_tags, traits = extract_traits_and_tags(desc)
    traits["luxury"] = lux or traits["luxury"]
    match = [k for k in db_lookup if k[0] == name and k[3] == country]
    if match:
        cursor.execute("""
            UPDATE destinations SET
                description = %s, tags = %s,
                adventure = %s, relax = %s, nature = %s, culture = %s, luxury = %s
            WHERE id = %s
        """, (
            desc, new_tags,
            traits["adventure"], traits["relax"], traits["nature"],
            traits["culture"], traits["luxury"], db_lookup[match[0]]["id"]
        ))
        updated += 1
    else:
        cursor.execute("""
            INSERT INTO destinations (
                name, country, description, tags,
                adventure, relax, nature, culture, luxury
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, (
            name.title(), country.title(), desc, new_tags,
            traits["adventure"], traits["relax"], traits["nature"],
            traits["culture"], traits["luxury"]
        ))
        inserted += 1

# Finalize
conn.commit()
close_connection(conn, cursor)
print(f"✅ Enrichment complete: {updated} updated, {inserted} inserted.")
