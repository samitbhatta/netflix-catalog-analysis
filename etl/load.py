import sqlite3
from pyspark.sql.functions import col
import os

def load_data(df_shows, df_genres, df_countries):
    """
    Load DataFrames into SQLite database.
    """
    print("✅ Loading data into SQLite...")

    os.makedirs("db", exist_ok=True)
    conn = sqlite3.connect("db/netflix.db")

    try:
        print("Rows: shows =", df_shows.count(),
              ", genres =", df_genres.count(),
              ", countries =", df_countries.count())

        # Rename problematic columns
        df_shows = df_shows.withColumn("cast", col("cast").cast("string"))
        df_shows = df_shows.withColumn("director", col("director").cast("string"))

        df_shows_pd = df_shows.toPandas()
        df_shows_pd.rename(columns={"cast": "cast_name"}, inplace=True)

        df_shows_pd.to_sql('shows', conn, if_exists='replace', index=False)
        df_genres.toPandas().to_sql('genres', conn, if_exists='replace', index=False)
        df_countries.toPandas().to_sql('countries', conn, if_exists='replace', index=False)

        conn.commit()
        print("✅ Data loaded into db/netflix.db")

    except Exception as e:
        print("❌ Error loading data:", e)

    finally:
        conn.close()
