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
# Text similarity (TF-IDF)
# -----------------------------
tfidf = TfidfVectorizer(stop_words="english")
tfidf_matrix = tfidf.fit_transform(df["text"])
cosine_sim = cosine_similarity(tfidf_matrix)

# -----------------------------
# Trait similarity (cosine)
# -----------------------------
trait_cols = ["adventure", "relax", "nature", "culture", "luxury"]
scaler = MinMaxScaler()
traits_matrix = scaler.fit_transform(df[trait_cols].fillna(0))
trait_sim = cosine_similarity(traits_matrix)

# -----------------------------
# Helper function to find all matching destinations
# -----------------------------
def find_all_destination_matches(query):
    query = query.lower()
    matches = df[
        df["name"].str.lower().fillna("").str.contains(query) |
        df["city"].str.lower().fillna("").str.contains(query) |
        df["state"].str.lower().fillna("").str.contains(query) |
        df["country"].str.lower().fillna("").str.contains(query)
    ]
    if matches.empty:
        raise ValueError(f"No match found for '{query}'")
    return matches.index.tolist()

# -----------------------------
# Recommender functions
# -----------------------------
def recommend_by_query(query_text, top_n=5):
    indexes = find_all_destination_matches(query_text)
    combined_scores = pd.Series(0, index=df.index)
    for idx in indexes:
        combined_scores += cosine_sim[idx]
    combined_scores = combined_scores.sort_values(ascending=False)
    return df.iloc[combined_scores.index[1:top_n+1]]

def recommend_hybrid(query_text, top_n=5, alpha=0.7):
    try:
        indexes = find_all_destination_matches(query_text)
    except ValueError:
        indexes = []

    combined_scores = pd.Series(0.0, index=df.index)
    for idx in indexes:
        text_scores = cosine_sim[idx]
        trait_scores = trait_sim[idx]
        final_scores = alpha * text_scores + (1 - alpha) * trait_scores
        combined_scores += final_scores

    if indexes:
        combined_scores /= len(indexes)

    # Exclude exact matches from similar recommendations
    combined_scores = combined_scores.drop(indexes, errors='ignore')
    combined_scores = combined_scores.sort_values(ascending=False)
    similar_indices = combined_scores.index[:top_n]

    # Create the final combined DataFrame
    exact_matches_df = df.iloc[indexes] if indexes else pd.DataFrame()
    similar_df = df.loc[similar_indices]
    final_results = pd.concat([exact_matches_df, similar_df])

    return final_results.reset_index(drop=True)

def recommend_by_traits(query_text, top_n=5):
    indexes = find_all_destination_matches(query_text)
    combined_scores = pd.Series(0, index=df.index)
    for idx in indexes:
        combined_scores += trait_sim[idx]
    combined_scores = combined_scores.sort_values(ascending=False)
    return df.iloc[combined_scores.index[1:top_n+1]]

def recommend_by_vibe(user_traits, top_n=5):
    required = ["adventure", "relax", "nature", "culture", "luxury"]
    for trait in required:
        if trait not in user_traits:
            raise ValueError(f"Missing trait: {trait}")

    user_vector = pd.DataFrame([user_traits])[required].astype(float)
    user_scaled = scaler.transform(user_vector)
    sim_scores = cosine_similarity(user_scaled, traits_matrix)[0]
    sim_scores = list(enumerate(sim_scores))
    sim_scores = sorted(sim_scores, key=lambda x: x[1], reverse=True)[:top_n]
    return df.iloc[[i[0] for i in sim_scores]]