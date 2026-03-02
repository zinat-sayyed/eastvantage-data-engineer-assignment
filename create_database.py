import sqlite3
import pandas as pd

DB_PATH = "data/sales.db"
OUTPUT_PATH = "output/result.csv"

# =========================================================
# 1. CREATE DATABASE TABLES
# =========================================================
def create_tables(conn):
    cursor = conn.cursor()

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS Customers (
        customer_id INTEGER PRIMARY KEY,
        age INTEGER NOT NULL
    );
    """)

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS Orders (
        order_id INTEGER PRIMARY KEY,
        customer_id INTEGER,
        FOREIGN KEY (customer_id) REFERENCES Customers(customer_id)
    );
    """)

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS Items (
        item_id INTEGER PRIMARY KEY,
        item_name TEXT NOT NULL
    );
    """)

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS Sales (
        sales_id INTEGER PRIMARY KEY AUTOINCREMENT,
        order_id INTEGER,
        item_id INTEGER,
        quantity INTEGER,
        FOREIGN KEY (order_id) REFERENCES Orders(order_id),
        FOREIGN KEY (item_id) REFERENCES Items(item_id)
    );
    """)

    conn.commit()
    print("Tables created")


# =========================================================
# 2. INSERT SAMPLE DATA
# =========================================================
def insert_sample_data(conn):
    cursor = conn.cursor()

    cursor.execute("DELETE FROM Customers")
    cursor.execute("DELETE FROM Orders")
    cursor.execute("DELETE FROM Items")
    cursor.execute("DELETE FROM Sales")

    customers = [(1,21),(2,23),(3,35),(4,45)]
    cursor.executemany("INSERT INTO Customers VALUES (?,?)", customers)

    # ✅ Updated item names
    items = [
        (1,'Laptop'),
        (2,'Phone'),
        (3,'Tablet')
    ]
    cursor.executemany("INSERT INTO Items VALUES (?,?)", items)

    orders = [(1,1),(2,1),(3,2),(4,3)]
    cursor.executemany("INSERT INTO Orders VALUES (?,?)", orders)

    sales = [
        (1,1,5),(2,1,5),        # Customer 1 -> Laptop total 10
        (3,1,1),(3,2,1),(3,3,1),# Customer 2 -> all items 1
        (4,3,2),               # Customer 3 -> Tablet total 2
        (1,2,None),(2,3,None)  # NULL quantities
    ]

    cursor.executemany(
        "INSERT INTO Sales(order_id,item_id,quantity) VALUES (?,?,?)", sales
    )

    conn.commit()
    print("Sample data inserted")
python

