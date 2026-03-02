import sqlite3

def get_connection(db_path: str):
    """
    Create SQLite connection safely.
    """
    try:
        conn = sqlite3.connect(db_path)
        print("Connected to database")
        return conn
    except Exception as e:
        print(f"Database connection failed: {e}")
        