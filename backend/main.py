from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
import psycopg2
import os

app = FastAPI()

# Enable CORS so frontend can talk to backend
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # or ["http://localhost:8000"] if you prefer
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Database connection
def get_connection():
    return psycopg2.connect(
        dbname="trafficdb",
        user="postgres",
        password="chetan",
        host="localhost",
        port="5432"
    )

@app.get("/traffic/{area}")
def get_traffic_data(area: str):
    try:
        conn = get_connection()
        cur = conn.cursor()

        query = """
            SELECT 
                AVG("TrafficVolume") AS avg_volume,
                AVG("Congestion Level") AS avg_congestion,
                COUNT(DISTINCT "Date") AS days_recorded
            FROM traffic_data
            WHERE LOWER("Area") = LOWER(%s);
        """
        cur.execute(query, (area,))
        result = cur.fetchone()
        conn.close()

        if not result or result[0] is None:
            raise HTTPException(status_code=404, detail="No data found for this area.")

        return {
            "area": area,
            "avg_traffic_volume": round(result[0], 2),
            "avg_congestion": round(result[1], 2),
            "days_recorded": result[2]
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
