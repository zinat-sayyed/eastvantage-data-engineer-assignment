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


def run_pandas_logic(conn):
    try:
        sales = pd.read_sql("SELECT * FROM Sales", conn)
        orders = pd.read_sql("SELECT * FROM Orders", conn)
        customers = pd.read_sql("SELECT * FROM Customers", conn)
        items = pd.read_sql("SELECT * FROM Items", conn)

        df = (
            sales.merge(orders, on="order_id")
                 .merge(customers, on="customer_id")
                 .merge(items, on="item_id")
        )

        df = df[
            (df["age"].between(18, 35)) &
            (df["quantity"].notna())
        ]

        result = (
            df.groupby(["customer_id", "age", "item_name"])["quantity"]
              .sum()
              .reset_index()
        )

        result = result[result["quantity"] > 0]

        result.columns = ["Customer", "Age", "Item", "Quantity"]

        result = result.sort_values(["Customer", "Item"])

        print("Pandas processing completed successfully")
        return result

    except Exception as e:
        print(f"Pandas processing failed: {e}")
        raise


def save_to_csv(df):
    df["Quantity"] = df["Quantity"].astype(int)
    df.to_csv(OUTPUT_PATH, sep=";", index=False)
    print("Pandas output saved successfully")


def main():
    try:
        create_directories()
        conn = get_connection()
        result_df = run_pandas_logic(conn)
        save_to_csv(result_df)
        conn.close()
        print("Pandas pipeline completed successfully")
    except Exception as e:
        print(f"Pandas pipeline failed: {e}")


if __name__ == "__main__":
    main()
    """
    Create SQLite connection safely.
    """
    try:
        create_directories()
        conn = sqlite3.connect(DB_PATH)
        print("Database connected successfully")
        return conn
    except Exception as e:
        print(f"Database connection failed: {e}")
        raise


def run_pandas_solution(conn):
    """
    Pure Pandas implementation of the assignment.
    """
    try:
        # Load tables
        sales = pd.read_sql("SELECT * FROM Sales", conn)
        orders = pd.read_sql("SELECT * FROM Orders", conn)
        customers = pd.read_sql("SELECT * FROM Customers", conn)
        items = pd.read_sql("SELECT * FROM Items", conn)

        # Merge all tables
        df = (
            sales.merge(orders, on="order_id")
                 .merge(customers, on="customer_id")
                 .merge(items, on="item_id")
        )

        # Apply business rules
        df = df[
            (df["age"].between(18, 35)) &
            (df["quantity"].notna())
        ]

        # Group and aggregate
        result = (
            df.groupby(["customer_id", "age", "item_name"])["quantity"]
              .sum()
              .reset_index()
        )

        # Remove zero totals
        result = result[result["quantity"] > 0]

        # Rename columns as required
        result.columns = ["Customer", "Age", "Item", "Quantity"]

        # Sort for consistent output
        result = result.sort_values(["Customer", "Item"])

        print("Pandas processing completed successfully")
        return result

    except Exception as e:
        print(f"Pandas processing failed: {e}")
        raise


def save_to_csv(df):
    """
    Save result to CSV with required format.
    """
    try:
        df["Quantity"] = df["Quantity"].astype(int)
        df.to_csv(OUTPUT_PATH, sep=";", index=False)
        print("CSV file generated successfully")
    except Exception as e:
        print(f"CSV export failed: {e}")
        raise


def main():
    try:
        conn = get_connection()
        result_df = run_pandas_solution(conn)
        save_to_csv(result_df)
        conn.close()
        print("Pandas pipeline completed successfully")
    except Exception as e:
        print(f"Pipeline failed: {e}")


if __name__ == "__main__":
    main()