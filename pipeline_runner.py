from etl.extract import create_spark_session, extract_data
from etl.transform import transform_data
from etl.load import load_data
import schedule
import time
import traceback

def run_pipeline():
    print("\n🚀 Running Netflix ETL Pipeline...")

    try:
        spark = create_spark_session()

        print("📥 Extracting data...")
        df_csv, df_json = extract_data(spark)

        print("🔄 Transforming data...")
        df_shows, df_genres, df_countries = transform_data(df_csv, df_json)

        print("📦 Loading data into destination...")
        load_data(df_shows, df_genres, df_countries)

        print("✅ Pipeline completed successfully.\n")

    except Exception as e:
        print("❌ Pipeline failed with error:")
        traceback.print_exc()

if __name__ == "__main__":
    run_pipeline()

    # Optional: schedule to run daily at 10:00 AM
    schedule.every().day.at("10:00").do(run_pipeline)

    print("⏰ Waiting for next scheduled run (every day at 10:00 AM)...")
    while True:
        schedule.run_pending()
        time.sleep(60)
