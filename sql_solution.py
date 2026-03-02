import sqlite3
import pandas as pd
import os

DB_PATH = "data/sales.db"
OUTPUT_PATH = "output/final_output.csv"


def create_directories():
    os.makedirs("output", exist_ok=True)


def get_connection():
    try:
        conn = sqlite3.connect(DB_PATH)
        print("Database connected successfully")
        return conn
    except Exception as e:
        print(f"Database connection failed: {e}")
        raise


def run_sql_query(conn):
    try:
        query = """
        SELECT 
            c.customer_id AS Customer,
            c.age AS Age,
            i.item_name AS Item,
            SUM(s.quantity) AS Quantity
        FROM Sales s
        JOIN Orders o ON s.order_id = o.order_id
        JOIN Customers c ON o.customer_id = c.customer_id
        JOIN Items i ON s.item_id = i.item_id
        WHERE c.age BETWEEN 18 AND 35
              AND s.quantity IS NOT NULL
        GROUP BY c.customer_id, c.age, i.item_name
        HAVING SUM(s.quantity) > 0
        ORDER BY c.customer_id, i.item_name
        """

        df = pd.read_sql_query(query, conn)
        print("SQL query executed successfully")
        return df

    except Exception as e:
        print(f"SQL execution failed: {e}")
        raise


def save_to_csv(df):
    df["Quantity"] = df["Quantity"].astype(int)
    df.to_csv(OUTPUT_PATH, sep=";", index=False)
    print("SQL output saved successfully")


def main():
    try:
        create_directories()
        conn = get_connection()
        result_df = run_sql_query(conn)
        save_to_csv(result_df)
        conn.close()
        print("SQL pipeline completed successfully")
    except Exception as e:
        print(f"SQL pipeline failed: {e}")


if __name__ == "__main__":
    main()