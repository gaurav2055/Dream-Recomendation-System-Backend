import pandas as pd
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.preprocessing import MinMaxScaler
from sklearn.metrics.pairwise import cosine_similarity
from DB.db_setup import get_connection

# -----------------------------
# Load data from PostgreSQL
# -----------------------------
conn = get_connection()
query = """
    SELECT id, name, city, state, country, description, tags,
           adventure, relax, nature, culture, luxury
    FROM destinations
"""
df = pd.read_sql(query, conn)
conn.close()

# -----------------------------
# Preprocess text
# -----------------------------
df["tags_str"] = df["tags"].apply(lambda x: " ".join(x) if isinstance(x, list) else str(x))
df["text"] = df["description"].fillna("") + " " + df["tags_str"].fillna("")
df["text"] = df["text"].str.lower()

# -----------------------------
# Build Vectorizers
# -----------------------------
tfidf = TfidfVectorizer(stop_words="english")
tfidf_matrix = tfidf.fit_transform(df["text"])

trait_cols = ["adventure", "relax", "nature", "culture", "luxury"]
scaler = MinMaxScaler()
traits_matrix = scaler.fit_transform(df[trait_cols].fillna(0))

# -----------------------------
# Helper: Find all matching destinations
# -----------------------------
def find_all_destination_matches(query):
    query = query.lower()
    matches = df[
        df["name"].str.lower().fillna("").str.contains(query) |
        df["city"].str.lower().fillna("").str.contains(query) |
        df["state"].str.lower().fillna("").str.contains(query) |
        df["country"].str.lower().fillna("").str.contains(query)
    ]
    return matches.index.tolist()

# -----------------------------
# Recommender Functions
# -----------------------------

def recommend_by_query(query_text, top_n=5):
    idxs = find_all_destination_matches(query_text)
    if not idxs:
        return df.sample(top_n)

    user_vector = tfidf.transform([" ".join(df.loc[idxs]["text"])])
    sim_scores = cosine_similarity(user_vector, tfidf_matrix)[0]

    top_indices = sim_scores.argsort()[::-1][:top_n]
    return df.iloc[top_indices]

def recommend_hybrid(query_text, top_n=5, alpha=0.7):
    idxs = find_all_destination_matches(query_text)
    if not idxs:
        return df.sample(top_n)

    user_vector_text = tfidf.transform([" ".join(df.loc[idxs]["text"])])
    user_vector_traits = scaler.transform(df.loc[idxs][trait_cols].fillna(0))

    text_scores = cosine_similarity(user_vector_text, tfidf_matrix)[0]
    trait_scores = cosine_similarity(user_vector_traits, traits_matrix).mean(axis=0)

    final_scores = alpha * text_scores + (1 - alpha) * trait_scores

    top_indices = final_scores.argsort()[::-1][:top_n]
    return df.iloc[top_indices]

def recommend_by_traits(query_text, top_n=5):
    idxs = find_all_destination_matches(query_text)
    if not idxs:
        return df.sample(top_n)

    user_vector_traits = scaler.transform(df.loc[idxs][trait_cols].fillna(0))
    sim_scores = cosine_similarity(user_vector_traits, traits_matrix).mean(axis=0)

    top_indices = sim_scores.argsort()[::-1][:top_n]
    return df.iloc[top_indices]

def recommend_by_vibe(user_traits, top_n=5):
    required = ["adventure", "relax", "nature", "culture", "luxury"]
    for trait in required:
        if trait not in user_traits:
            raise ValueError(f"Missing trait: {trait}")

    user_vector = pd.DataFrame([user_traits])[required].astype(float)
    user_scaled = scaler.transform(user_vector)

    sim_scores = cosine_similarity(user_scaled, traits_matrix)[0]

    top_indices = sim_scores.argsort()[::-1][:top_n]
    return df.iloc[top_indices]
