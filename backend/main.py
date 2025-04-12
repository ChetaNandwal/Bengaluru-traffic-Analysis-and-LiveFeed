from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
import psycopg2
import os
from fastapi.staticfiles import StaticFiles
from urllib.parse import urlparse
from fastapi.templating import Jinja2Templates
from fastapi.requests import Request

templates = Jinja2Templates(directory="frontend")

app = FastAPI()

# Enable CORS so frontend can talk to backend
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # or ["http://localhost:8000"] if you prefer
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.get("/")
def serve_index(request: Request):
    api_key = os.getenv("MAPS_API_KEY")
    return templates.TemplateResponse("index.html", {"request": request, "api_key": api_key})

def get_connection():
    db_url = os.getenv("DATABASE_URL")
    if not db_url:
        raise Exception("DATABASE_URL is not set")

    result = urlparse(db_url)
    return psycopg2.connect(
        dbname=result.path[1:],
        user=result.username,
        password=result.password,
        host=result.hostname,
        port=result.port
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
