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
import geopandas as gpd

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

def parse_tif_filename(filename: str) -> dict:
    # EP_V1_291025_NDVI.tif
    stem = Path(filename).stem
    parts = stem.rsplit("_", 3)

    if len(parts) != 4:
        raise ValueError(
            "Invalid tif filename format. Expected EP_V1_291025_NDVI.tif"
        )

    local_id, flight_code, raw_date, metric = parts
    parsed_date = datetime.strptime(raw_date, "%d%m%y").replace(tzinfo=timezone.utc)

    return {
        "local_id": local_id,
        "flight_code": flight_code,
        "date": parsed_date,
        "metric": metric.lower(),
    }

@app.post("/calculate_stats", dependencies=[Depends(verify_token)])
async def calculate_stats(
    tif_file: UploadFile = File(...),
    gpkg_file: UploadFile = File(...),
    plot_id_field: str = Form("uid"),
):
    try:
        tif_info = parse_tif_filename(tif_file.filename)
        parsed_date = tif_info["date"]
        metric = tif_info["metric"]
        local_id = tif_info["local_id"]
        flight_code = tif_info["flight_code"]
        flight_id = f"{local_id}_{flight_code}_{parsed_date.strftime('%Y-%m-%d')}"
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))

    with tempfile.TemporaryDirectory() as temp_dir:
        tif_path = Path(temp_dir) / tif_file.filename
        gpkg_path = Path(temp_dir) / gpkg_file.filename

        with open(tif_path, "wb") as f:
            f.write(await tif_file.read())

        with open(gpkg_path, "wb") as f:
            f.write(await gpkg_file.read())

        try:
            layers = gpd.list_layers(str(gpkg_path))
            if layers.empty:
                raise ValueError("The GPKG file does not contain any layers.")

            layer_name = layers.iloc[0]["name"]

            base_metadata = {
                "local_id": local_id,
                "flight_code": flight_code,
                "flight_id": flight_id,
                "metric": metric,
                "plot_id_field": plot_id_field,
                "source_tif": tif_file.filename,
                "source_gpkg": gpkg_file.filename,
                "layer_name": layer_name,
            }

            documents = raster_stats.calculate_polygon_stats(
                raster_path=str(tif_path),
                polygons_path=str(gpkg_path),
                date=parsed_date,
                base_metadata=base_metadata,
                plot_id_field=plot_id_field,
                include_no_coverage=False,
                layer_name=layer_name,
                extra_metrics=True,
            )

            inserted_count = 0
            skipped_count = 0

            for doc in documents:
                plot_value = str(doc["metadata"].get(plot_id_field))

                existing = get_database().metric.find_one({
                    "date": doc["date"],
                    f"metadata.{plot_id_field}": plot_value,
                    "metadata.metric": doc["metadata"]["metric"],
                    "metadata.flight_code": doc["metadata"]["flight_code"],
                    "metadata.local_id": doc["metadata"]["local_id"],
                })

                if existing:
                    skipped_count += 1
                    continue

                get_database().metric.insert_one(doc)
                inserted_count += 1

            return {
                "message": "Statistics processed successfully.",
                "flight_id": flight_id,
                "plot_id_field": plot_id_field,
                "generated_count": len(documents),
                "inserted_count": inserted_count,
                "skipped_count": skipped_count,
            }

        except Exception as e:
            raise HTTPException(status_code=500, detail=str(e))

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
