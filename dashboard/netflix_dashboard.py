import streamlit as st
import sqlite3
import pandas as pd

# --- 1️⃣ Connect to your SQLite DB ---
conn = sqlite3.connect("db/netflix.db")

# --- 2️⃣ Load tables into Pandas ---
shows_df = pd.read_sql_query("SELECT * FROM shows", conn)
genres_df = pd.read_sql_query("SELECT * FROM genres", conn)
countries_df = pd.read_sql_query("SELECT * FROM countries", conn)

# --- 3️⃣ Streamlit layout ---
st.title("🎬 Netflix Movies & Shows Dashboard")

st.write("Explore Netflix data by year, genre, and country.")

# --- 4️⃣ KPI ---
col1, col2 = st.columns(2)
col1.metric("Total Titles", shows_df.shape[0])
col2.metric("Distinct Genres", genres_df['genre'].nunique())

# --- 5️⃣ Shows per year ---
st.subheader("Shows Released per Year")
shows_per_year = shows_df.groupby("release_year").size().reset_index(name="count").sort_values("release_year")
st.bar_chart(shows_per_year.set_index("release_year"))

# --- 6️⃣ Top genres ---
st.subheader("Top Genres")
top_genres = genres_df.groupby("genre").size().reset_index(name="count").sort_values("count", ascending=False).head(10)
st.bar_chart(top_genres.set_index("genre"))

# --- 7️⃣ Top countries ---
st.subheader("Top Countries by Number of Titles")
top_countries = countries_df.groupby("country").size().reset_index(name="count").sort_values("count", ascending=False).head(10)
st.bar_chart(top_countries.set_index("country"))

# --- 8️⃣ Search box ---
st.subheader("Search for a Title")
search = st.text_input("Enter title keyword:")
if search:
    results = shows_df[shows_df['title'].str.contains(search, case=False, na=False)]
    st.write(results)

# --- 9️⃣ Close DB connection ---
conn.close()
