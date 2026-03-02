from db_connection import get_connection
from sql_solution import run_sql_solution
from pandas_solution import run_pandas_solution

OUTPUT_PATH = "output/result.csv"
DB_PATH = "data/sales.db"

def save_to_csv(df):
    try:
        df["Quantity"] = df["Quantity"].astype(int)
        df.to_csv(OUTPUT_PATH, sep=";", index=False)
        print(" CSV generated successfully")
    except Exception as e:
        print(f" Failed writing CSV: {e}")
        raise


def validate_outputs(sql_df, pandas_df):
    if sql_df.equals(pandas_df):
        print("SQL and Pandas outputs MATCH")
    else:
        raise Exception(" Outputs do NOT match")


def main():
    try:
        conn = get_connection(DB_PATH)

        sql_df = run_sql_solution(conn)
        pandas_df = run_pandas_solution(conn)

        validate_outputs(sql_df, pandas_df)
        save_to_csv(sql_df)

        conn.close()

    except Exception as e:
        print(f"Pipeline failed: {e}")


if __name__ == "__main__":
    main()