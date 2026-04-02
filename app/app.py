from fastapi import FastAPI, UploadFile, File, Form, HTTPException, Depends
from fastapi.responses import JSONResponse
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from pathlib import Path
import tempfile
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
    qpkg_file: UploadFile = File(...)
):
    # Save files temporarily
    with tempfile.TemporaryDirectory() as temp_dir:
        tif_path = Path(temp_dir) / tif_file.filename
        qpkg_path = Path(temp_dir) / qpkg_file.filename

        with open(tif_path, "wb") as f:
            content = await tif_file.read()
            f.write(content)

        with open(qpkg_path, "wb") as f:
            content = await qpkg_file.read()
            f.write(content)

        # Compute flight_id
        flight_id = f"{Path(tif_file.filename).stem}_{date}"

        try:
            # Run the calculation
            results = raster_stats.calculate_polygon_stats(
                raster_path=str(tif_path),
                polygons_path=str(qpkg_path),
                plot_id_field="lote_id",  # Assuming default, can be made configurable
                flight_id=flight_id,
                index_type=index_type,
                include_no_coverage=False
            )

            # Store results in MongoDB
            try:
                db = get_database()
                collection = db["polygon_stats"]
                for result in results:
                    # Use upsert to avoid duplicates based on flight_id and plot_id
                    filter_doc = {"flight_id": result["flight_id"], "plot_id": result["plot_id"]}
                    collection.update_one(filter_doc, {"$set": result}, upsert=True)
            except Exception as db_error:
                # Log error but don't fail the response
                print(f"Error storing in MongoDB: {db_error}")

            return JSONResponse(content={"results": results})

        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Error processing files: {str(e)}")

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
