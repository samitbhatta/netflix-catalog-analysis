from pyspark.sql.functions import split, explode, col, lit, when, trim
from pyspark.sql.types import StringType

def transform_data(df_csv, df_json):
    """
    Clean, combine, normalize Netflix data, and remove unwanted records.
    """
    print("✅ Transform: Combining CSV and JSON")

    # Clean all relevant string columns
    def clean_strings(df):
        string_columns = ['show_id', 'title', 'type', 'date_added', 'director', 'cast', 'listed_in', 'country', 'release_year']
        for col_name in string_columns:
            if col_name in df.columns:
                df = df.withColumn(col_name, trim(col(col_name).cast(StringType())))
        return df

    df_csv = clean_strings(df_csv)
    df_json = clean_strings(df_json)

    # Combine datasets
    df_combined = df_csv.unionByName(df_json, allowMissingColumns=True)

    # 🔥 Remove rows where 'cast' contains 'Paul Sambo'
    if "cast" in df_combined.columns:
        df_combined = df_combined.filter(~col("cast").contains("Paul Sambo"))

    # Debug: confirm removal
    print("⚠️ Verifying 'Paul Sambo' is removed:")
    df_combined.filter(col("cast").contains("Paul Sambo")).show()

    # Drop duplicates
    df_combined = df_combined.dropDuplicates(['show_id'])

    # Safely cast release_year to int, set to 0 if invalid
    df_combined = df_combined.withColumn(
        "release_year",
        when(col("release_year").rlike("^[0-9]{4}$"), col("release_year").cast("int"))
        .otherwise(lit(0))
    )

    # Fill missing or empty country values
    df_combined = df_combined.withColumn(
        "country",
        when(col("country").isNull() | (col("country") == ""), lit("Unknown"))
        .otherwise(col("country"))
    )

    # Create genre_list from listed_in
    df_combined = df_combined.withColumn(
        "genre_list",
        split(col("listed_in"), ",\\s*")
    )

    # Normalize genres table
    df_genres = df_combined.select(
        "show_id",
        explode(col("genre_list")).alias("genre")
    ).dropDuplicates()

    # Normalize countries table
    df_countries = df_combined.select(
        "show_id", "country"
    ).dropDuplicates()

    # Shows table
    select_cols = ["show_id", "title", "type", "release_year", "date_added"]
    for optional_col in ["director", "cast"]:
        if optional_col in df_combined.columns:
            select_cols.append(optional_col)

    df_shows = df_combined.select(*select_cols).dropDuplicates()

    print("✅ Transform done.")
    return df_shows, df_genres, df_countries
