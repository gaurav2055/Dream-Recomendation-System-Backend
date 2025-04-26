import psycopg2
import os

def get_connection():
    """
    Establishes a connection to the PostgreSQL database.
    Returns:
        conn: psycopg2 connection object
    """
    try:
        conn = psycopg2.connect(
            dbname=os.environ.get("DB_NAME"),
            user=os.environ.get("DB_USER"),
            password=os.environ.get("DB_PASS"),
            host=os.environ.get("DB_HOST"),
            port=os.environ.get("DB_PORT", "5432")
        )
        print("✅ Connected to PostgreSQL!")
        return conn
    except psycopg2.Error as e:
        print("❌ Unable to connect to the database.")
        print(e)
        return None
    
def close_connection(conn, cursor=None):
    if cursor:
        cursor.close()
    if conn:
        conn.close()
        print("🔌 PostgreSQL connection closed.")
