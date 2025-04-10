import psycopg2

def create_table():
    conn = psycopg2.connect(
        dbname="DB_Name",
        user="DBUSER",
        password="DBPASSWORD",
        host="DBHOST",
        port="5432"
    )
    cur = conn.cursor()

    cur.execute("""
        CREATE TABLE IF NOT EXISTS traffic_data (
            id SERIAL PRIMARY KEY,
            area TEXT,
            timestamp TIMESTAMP,
            hour INT,
            day INT,
            month INT,
            year INT,
            is_weekend BOOLEAN,
            traffic_level FLOAT
        );
    """)

    conn.commit()
    cur.close()
    conn.close()
    print("✅ traffic_data table created successfully.")

if __name__ == "__main__":
    create_table()
