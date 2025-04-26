import psycopg2

def get_connection():
    """
    Establishes a connection to the PostgreSQL database.
    Returns:
        conn: psycopg2 connection object
    """
    try:
        conn = psycopg2.connect(
            dbname="travel_db",
            user="postgres",
            password="post@Charade@01",
            host="localhost",
            port="5432"
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
