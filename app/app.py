from fastapi import FastAPI, UploadFile, File, Form, HTTPException, Depends
from fastapi.responses import JSONResponse
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from pathlib import Path
import tempfile
from datetime import datetime, timezone
import os
from app import raster_stats
from typing import List, Dict, Any
from pymongo import MongoClient
from pymongo.errors import ConnectionFailure
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()
security = HTTPBearer()

def verify_token(credentials: HTTPAuthorizationCredentials = Depends(security)):
    token = os.getenv("API_KEY", "secret-key")  # Default for development
    if credentials.credentials != token:
        raise HTTPException(status_code=401, detail="Invalid token")

def get_database():
    mongodb_url = os.getenv("MONGODB_URL", "mongodb://localhost:27017")
    client = MongoClient(mongodb_url)
    db = client["fotogrametria"]  # Assuming database name
    return db

app = FastAPI(title="Polygon Stats API", version="1.0.0")

@app.get("/health")
async def health_check():
    return {"status": "healthy"}

@app.post("/calculate_stats", dependencies=[Depends(verify_token)])
async def calculate_stats(
    tif_file: UploadFile = File(...),
    date: str = Form(...),
    index_type: str = Form(...),
    crop: str = Form(""),
    farm: str = Form(""),
    gpkg_file: UploadFile = File(...)
):
    try:
        parsed_date = datetime.fromisoformat(date)
        if parsed_date.tzinfo is None:
            parsed_date = parsed_date.replace(tzinfo=timezone.utc)
    except ValueError:
        raise HTTPException(
            status_code=400,
            detail="Invalid date format. Use ISO format like 2026-01-01 or 2026-01-01T00:00:00"
        )

    with tempfile.TemporaryDirectory() as temp_dir:
        tif_path = Path(temp_dir) / tif_file.filename
        gpkg_path = Path(temp_dir) / gpkg_file.filename

        with open(tif_path, "wb") as f:
            f.write(await tif_file.read())

        with open(gpkg_path, "wb") as f:
            f.write(await gpkg_file.read())

        flight_id = f"{Path(tif_file.filename).stem}_{parsed_date.date().isoformat()}"

        try:
            results = raster_stats.calculate_polygon_stats(
                raster_path=str(tif_path),
                polygons_path=str(gpkg_path),
                plot_id_field="lote_id",
                flight_id=flight_id,
                index_type=index_type,
                include_no_coverage=False
            )

            inserted_count = 0
            skipped_count = 0
            inserted_docs = []
            skipped_docs = []

            for row in results:
                if row.get("status") != "ok":
                    continue

                doc = {
                    "date": parsed_date,
                    "metadata": {
                        "crop": crop,
                        "farm": farm,
                        "plot": str(row["plot_id"]),
                        "metric": index_type.lower(),
                    },
                    "avg": row["mean"],
                    "max": row["max"],
                    "min": row["min"],
                }

                existing = get_database().metric.find_one({
                    "date": parsed_date,
                    "metadata.crop": crop,
                    "metadata.farm": farm,
                    "metadata.plot": str(row["plot_id"]),
                    "metadata.metric": index_type.lower(),
                })

                if existing:
                    skipped_count += 1
                    skipped_docs.append({
                        "plot": str(row["plot_id"]),
                        "reason": "document already exists for same date/crop/farm/plot/metric"
                    })
                    continue

                get_database().metric.insert_one(doc)
                inserted_count += 1
                inserted_docs.append({
                    "plot": str(row["plot_id"]),
                    "avg": row["mean"],
                    "max": row["max"],
                    "min": row["min"],
                })

            return {
                "message": "Statistics processed successfully.",
                "flight_id": flight_id,
                "inserted_count": inserted_count,
                "skipped_count": skipped_count,
                "inserted_docs": inserted_docs,
                "skipped_docs": skipped_docs,
            }

        except Exception as e:
            raise HTTPException(status_code=500, detail=str(e))

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
