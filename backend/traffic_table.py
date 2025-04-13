import psycopg2
import os
from urllib.parse import urlparse
from dotenv import load_dotenv
load_dotenv()


# Updated connection using DATABASE_URL
def get_connection():
    db_url = os.getenv("DATABASE_URL")
    if not db_url:
        raise Exception("DATABASE_URL not set")

    result = urlparse(db_url)

    return psycopg2.connect(
        dbname=result.path[1:],
        user=result.username,
        password=result.password,
        host=result.hostname,
        port=result.port
    )

def create_table():
    conn = get_connection()
    cur = conn.cursor()

    cur.execute("""
        CREATE TABLE IF NOT EXISTS traffic_data (
            id SERIAL PRIMARY KEY,
            area TEXT,
            date DATE,
            day INT,
            month INT,
            year INT,
            is_weekend BOOLEAN,
            trafficvolume FLOAT,
            average_speed FLOAT,
            congestion_level FLOAT
        );
    """)

    conn.commit()
    cur.close()
    conn.close()
    print("✅ traffic_data table created successfully.")

if __name__ == "__main__":
    create_table()
