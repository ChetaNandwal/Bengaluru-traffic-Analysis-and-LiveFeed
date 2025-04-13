from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
import psycopg2
import os
from fastapi.staticfiles import StaticFiles
from urllib.parse import urlparse
from fastapi.templating import Jinja2Templates
from fastapi.requests import Request
import traceback
from dotenv import load_dotenv
load_dotenv()

db_url = os.getenv("DATABASE_URL")





templates = Jinja2Templates(directory="frontend")

app = FastAPI()

# Enable CORS so frontend can talk to backend
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"], 
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.get("/")
def serve_index(request: Request):
    api_key = os.getenv("MAPS_API_KEY")
    return templates.TemplateResponse("index.html", {"request": request, "api_key": api_key})

# Serve static files like styles.css
app.mount("/static", StaticFiles(directory="frontend"), name="static")

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
                area,
                AVG(congestion_level) as avg_congestion,
                COUNT(DISTINCT date) as days_recorded
            FROM traffic_data
            WHERE LOWER(area) LIKE %s
            GROUP BY area;
        """

        cur.execute(query, (f"%{area.lower()}%",))  # ✅ Fixed: matches %s placeholder
        result = cur.fetchone()

        cur.close()
        conn.close()

        if result:
            return {
                "area": result[0],
                "avg_congestion": round(result[1], 2),
                "days_recorded": result[2]
            }
        else:
            raise HTTPException(status_code=404, detail="No data found")

    except Exception as e:
        print("❌ Error:", e)
        raise HTTPException(status_code=500, detail="Internal server error")
