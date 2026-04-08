from fastapi import FastAPI, UploadFile, File, Form, HTTPException, Depends, Query, Response
from fastapi.responses import JSONResponse
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from fastapi.middleware.cors import CORSMiddleware
from pathlib import Path
import tempfile
from datetime import datetime, timezone
import os
import shutil
import json
from app import raster_stats
from typing import List, Dict, Any, Optional
from dotenv import load_dotenv
import geopandas as gpd
from pyproj import Transformer
from motor.motor_asyncio import AsyncIOMotorClient
# Load environment variables from .env file
load_dotenv()
security = HTTPBearer()

# Likely UTM Zone 13N for your data. Change if needed.
SOURCE_CRS = "EPSG:32613"
TARGET_CRS = "EPSG:4326"

transformer = Transformer.from_crs(SOURCE_CRS, TARGET_CRS, always_xy=True)

def transform_ring(ring: List[List[float]]) -> List[List[float]]:
    transformed = []
    for x, y in ring:
        lon, lat = transformer.transform(x, y)
        transformed.append([lon, lat])
    return transformed

def transform_polygon(polygon_coords: List[List[List[float]]]) -> List[List[List[float]]]:
    # polygon_coords = [outer_ring, hole1, hole2, ...]
    return [transform_ring(ring) for ring in polygon_coords]

def transform_geometry_to_wgs84(geometry: Dict[str, Any]) -> Dict[str, Any]:
    geom_type = geometry.get("type")
    coords = geometry.get("coordinates")

    if not geom_type or coords is None:
        raise ValueError("Invalid geometry")

    if geom_type == "Polygon":
        return {
            "type": "Polygon",
            "coordinates": transform_polygon(coords),
        }

    if geom_type == "MultiPolygon":
        return {
            "type": "MultiPolygon",
            "coordinates": [transform_polygon(polygon) for polygon in coords],
        }

    raise ValueError(f"Unsupported geometry type: {geom_type}")

def verify_token(credentials: HTTPAuthorizationCredentials = Depends(security)):
    token = os.getenv("API_KEY", "secret-key")  # Default for development
    if credentials.credentials != token:
        raise HTTPException(status_code=401, detail="Invalid token")

def get_database():
    mongodb_url = os.getenv("MONGODB_URL", "mongodb://localhost:27017")
    mongo = AsyncIOMotorClient(mongodb_url)
    db = mongo[os.getenv("MONGODB_DB", "fotogrametria")]
    return db

app = FastAPI(title="Polygon Stats API", version="1.0.0")

origins = [
    "https://salaisesmap.prox.one",
    "https://8000--main--fotogrametria--agusrumayor.coder.rumayor.org"
    "https://maps3569.prox.one",
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)
def parse_tif_filename(filename: str) -> dict:
    # EP_V1_291025_NDVI.tif
    stem = Path(filename).stem
    parts = stem.rsplit("_", 3)

    if len(parts) != 4:
        raise ValueError(
            "Invalid tif filename format. Expected UP_V#_291025_NDVI.tif"
        )

    local_id, flight_code, raw_date, metric = parts
    parsed_date = datetime.strptime(raw_date, "%d%m%y").replace(tzinfo=timezone.utc)

    return {
        "local_id": local_id,
        "flight_code": flight_code,
        "date": parsed_date,
        "metric": metric.lower(),
    }

@app.get("/api/geometry")
async def get_geometry(
    uid: str = Query(..., description="Plot uid, e.g. UP-L23-AJO-PRE-M05-A"),
    source_tif: Optional[str] = Query(None, description="Optional source tif filter"),
) -> Dict[str, Any]:
    query: Dict[str, Any] = {"metadata.uid": uid}

    if source_tif:
        query["metadata.source_tif"] = source_tif

    doc = await get_database().metric.find_one(
        query,
        {
            "_id": 0,
            "metadata.uid": 1,
            "metadata.Mudada": 1,
            "metadata.C_Mudada": 1,
            "metadata.up": 1,
            "metadata.metric": 1,
            "metadata.source_tif": 1,
            "metadata.area_ha": 1,
            "metadata.geometry": 1,
            "avg": 1,
            "min": 1,
            "max": 1,
            "p10": 1,
            "p50": 1,
            "p90": 1,
            "stddev": 1,
            "count": 1,
            "date": 1,
        },
    )

    if not doc:
        raise HTTPException(status_code=404, detail="Geometry not found")

    geometry = doc.get("metadata", {}).get("geometry")
    if not geometry:
        raise HTTPException(status_code=404, detail="Document has no geometry")

    try:
        geometry_wgs84 = transform_geometry_to_wgs84(geometry)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))

    metadata = doc.get("metadata", {})

    return {
        "type": "Feature",
        "geometry": geometry_wgs84,
        "properties": {
            "uid": metadata.get("uid"),
            "Mudada": metadata.get("Mudada"),
            "C_Mudada": metadata.get("C_Mudada"),
            "up": metadata.get("up"),
            "metric": metadata.get("metric"),
            "source_tif": metadata.get("source_tif"),
            "area_ha": metadata.get("area_ha"),
            "date": doc.get("date"),
            "avg": doc.get("avg"),
            "min": doc.get("min"),
            "max": doc.get("max"),
            "p10": doc.get("p10"),
            "p50": doc.get("p50"),
            "p90": doc.get("p90"),
            "stddev": doc.get("stddev"),
            "count": doc.get("count"),
        },
    }

@app.post("/calculate_stats", dependencies=[Depends(verify_token)])
async def calculate_stats(
    tif_file: UploadFile = File(...),
    gpkg_file: UploadFile = File(...),
    plot_id_field: str = Form("uid"),
    force_store: bool = Form(False),
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

            # Store tif in cogs path if we have data
            tif_stored = False
            if len(documents) > 0:
                cogs_path = os.getenv("COGS_PATH", "./cogs")
                cogs_dir = Path(cogs_path)
                cogs_dir.mkdir(parents=True, exist_ok=True)
                target = cogs_dir / tif_file.filename
                if not target.exists() or force_store:
                    shutil.copy(tif_path, target)
                    tif_stored = True

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
                "tif_stored": tif_stored,
            }

        except Exception as e:
            raise HTTPException(status_code=500, detail=str(e))
from shapely.geometry import shape, mapping


def build_lot_label(metadata: Dict[str, Any]) -> str:
    parts = [metadata.get("um_1"), metadata.get("um_2"), metadata.get("um_3")]
    label = " ".join([p for p in parts if p])
    return label or metadata.get("Mudada") or metadata.get("uid") or "Lote"


def build_label_point_from_geometry(geometry: Dict[str, Any]) -> Dict[str, Any]:
    geometry_wgs84 = transform_geometry_to_wgs84(geometry)
    shp = shape(geometry_wgs84)
    point = shp.point_on_surface()
    return mapping(point)


@app.get("/api/lots_labels")
async def get_lots_labels(
    source_tif: Optional[str] = Query(default=None)
):
    mongo_filter: Dict[str, Any] = {
        "metadata.geometry": {"$exists": True},
        "metadata.uid": {"$exists": True, "$ne": None},
    }

    if source_tif:
        mongo_filter["metadata.source_tif"] = source_tif

    pipeline: List[Dict[str, Any]] = [
        {"$match": mongo_filter},
        {"$sort": {"date": -1}},
        {
            "$group": {
                "_id": "$metadata.uid",
                "doc": {"$first": "$$ROOT"}
            }
        },
        {"$replaceRoot": {"newRoot": "$doc"}}
    ]

    docs = await get_database().metric.aggregate(pipeline).to_list(length=None)

    features = []

    for doc in docs:
        metadata = doc.get("metadata", {})
        raw_geometry = metadata.get("geometry")

        if not raw_geometry:
            continue

        try:
            label_point = build_label_point_from_geometry(raw_geometry)
        except Exception as e:
            print(f"Error building label point for uid {metadata.get('uid')}: {e}")
            continue

        features.append({
            "type": "Feature",
            "geometry": label_point,
            "properties": {
                "uid": metadata.get("uid"),
                "name": metadata.get("Mudada") or metadata.get("uid"),
                "label": build_lot_label(metadata),
                "Mudada": metadata.get("Mudada"),
                "C_Mudada": metadata.get("C_Mudada"),
                "um_1": metadata.get("um_1"),
                "um_2": metadata.get("um_2"),
                "um_3": metadata.get("um_3"),
                "up": metadata.get("up"),
                "variedad": metadata.get("variedad"),
                "especie": metadata.get("especie"),
                "area_ha": metadata.get("area_ha"),
                "source_tif": metadata.get("source_tif"),
                "flight_id": metadata.get("flight_id"),
                "flight_code": metadata.get("flight_code"),
                "metric": metadata.get("metric"),
                "avg": doc.get("avg"),
                "p10": doc.get("p10"),
                "p50": doc.get("p50"),
                "p90": doc.get("p90"),
                "min": doc.get("min"),
                "max": doc.get("max"),
                "date": doc.get("date").isoformat() if doc.get("date") else None,
            }
        })

    return Response(
        content=json.dumps({
            "type": "FeatureCollection",
            "features": features
        }),
        media_type="application/json",
        headers={
            "Cache-Control": "public, max-age=3600"
        }
    )
if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
