from flask import Flask, request, jsonify
from flask import Flask
from flask_cors import CORS
import os

from app.recommender import (
    recommend_by_query,
    recommend_by_traits,
    recommend_hybrid,
    recommend_by_vibe,
    df
)

app = Flask(__name__)
CORS(app, origins=["http://localhost:3000"])
# Health check
@app.route("/")
def home():
    return {"message": "🚀 Travel Recommender API is running!"}

# GET /recommend?query=Rome&top_n=5
@app.route("/recommend", methods=["GET"])
def recommend():
    query = request.args.get("query")
    top_n = int(request.args.get("top_n", 5))
    try:
        results = recommend_by_query(query, top_n)
        print(f"Query: {query}, Top N: {top_n}, Results: {results}")
        finalResult = results.to_dict(orient="records")
        return finalResult
    except Exception as e:
        return {"error": str(e)}, 400

# GET /recommend-hybrid?query=Rome&top_n=5&alpha=0.7
@app.route("/recommend-hybrid", methods=["GET"])
def hybrid():
    query = request.args.get("query")
    top_n = int(request.args.get("top_n", 5))
    alpha = float(request.args.get("alpha", 0.7))
    try:
        results = recommend_hybrid(query, top_n, alpha)
        return results.to_dict(orient="records")
    except Exception as e:
        return {"error": str(e)}, 400

# GET /recommend-traits?query=Rome&top_n=5
@app.route("/recommend-traits", methods=["GET"])
def traits():
    query = request.args.get("query")
    top_n = int(request.args.get("top_n", 5))
    try:
        results = recommend_by_traits(query, top_n)
        return results.to_dict(orient="records")
    except Exception as e:
        return {"error": str(e)}, 400

# POST /recommend-vibe
# Body: {"adventure": 5, "relax": 2, "nature": 4, "culture": 1, "luxury": 3}
@app.route("/recommend-vibe", methods=["POST"])
def vibe():
    try:
        user_traits = request.get_json()
        top_n = int(request.args.get("top_n", 5))
        results = recommend_by_vibe(user_traits, top_n)
        return results.to_dict(orient="records")
    except Exception as e:
        return {"error": str(e)}, 400
    
@app.route("/suggest", methods=["GET"])
def suggest():
    query = request.args.get("q", "").lower()
    if not query:
        return jsonify([])

    try:
        # Get up to 10 matching names or cities
        matches = df[
            df["name"].str.lower().fillna("").str.contains(query) |
            df["city"].str.lower().fillna("").str.contains(query)
        ][["name", "city", "state", "country"]].drop_duplicates().head(10)

        suggestions = []
        for _, row in matches.iterrows():
            parts = [row["name"], row["city"], row["state"], row["country"]]
            label = ", ".join([p for p in parts if p and isinstance(p, str)])
            suggestions.append(label)

        return jsonify(suggestions)
    except Exception as e:
        return {"error": str(e)}, 500

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))  # default for local dev
    app.run(debug=True, host="0.0.0.0", port=port)  # or change port if needed
