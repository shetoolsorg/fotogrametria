from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

import geopandas as gpd
import numpy as np
import rasterio
from rasterio.features import geometry_mask
from shapely.geometry import box

def build_histogram(values, bins=10, hist_range=(0, 1)):
    hist, bin_edges = np.histogram(values, bins=bins, range=hist_range)

    return [
        {
            "bin_start": float(bin_edges[i]),
            "bin_end": float(bin_edges[i + 1]),
            "count": int(hist[i]),
        }
        for i in range(len(hist))
    ]

def make_json_safe(value: Any) -> Any:
    if value is None:
        return None

    if isinstance(value, (str, int, float, bool, datetime)):
        return value

    if isinstance(value, np.integer):
        return int(value)

    if isinstance(value, np.floating):
        return float(value)

    if isinstance(value, np.bool_):
        return bool(value)

    try:
        if np.isnan(value):
            return None
    except Exception:
        pass

    if hasattr(value, "__geo_interface__"):
        return value.__geo_interface__

    return str(value)


def calculate_polygon_stats(
    raster_path: str | Path,
    polygons_path: str | Path,
    date: datetime,
    base_metadata: Dict[str, Any],
    plot_id_field: str = "uid",
    include_no_coverage: bool = True,
    layer_name: Optional[str] = None,
    extra_metrics: bool = True,
) -> List[Dict[str, Any]]:
    """
    Regresa documentos con estructura tipo MongoDB time series:

    {
        "date": datetime,
        "metadata": { ... },
        "avg": 0.5,
        "max": 0.7,
        "min": 0.4,
        ...
    }
    """
    raster_path = Path(raster_path)
    polygons_path = Path(polygons_path)

    if not raster_path.exists():
        raise FileNotFoundError(f"Raster file not found: {raster_path}")

    if not polygons_path.exists():
        raise FileNotFoundError(f"Polygon file not found: {polygons_path}")

    if not isinstance(base_metadata, dict):
        raise ValueError("base_metadata must be a dict")

    documents: List[Dict[str, Any]] = []

    with rasterio.open(raster_path) as src:
        raster_crs = src.crs
        raster_bounds = src.bounds
        raster_transform = src.transform
        raster_nodata = src.nodata

        if raster_crs is None:
            raise ValueError(f"The raster has no CRS defined: {raster_path}")

        raster_data = src.read(1)

        if raster_nodata is not None:
            nodata_mask = raster_data == raster_nodata
        else:
            nodata_mask = np.zeros(raster_data.shape, dtype=bool)

        if polygons_path.suffix.lower() == ".gpkg":
            available_layers = gpd.list_layers(str(polygons_path))
            if available_layers.empty:
                raise ValueError(f"The GPKG file has no layers: {polygons_path}")

            if layer_name is None:
                layer_name = available_layers.iloc[0]["name"]

            gdf = gpd.read_file(str(polygons_path), layer=layer_name)
        else:
            gdf = gpd.read_file(str(polygons_path))

        if gdf.empty:
            raise ValueError(f"The polygon layer/file is empty: {polygons_path}")

        if gdf.crs is None:
            raise ValueError(f"The polygon file has no CRS defined: {polygons_path}")

        if plot_id_field not in gdf.columns:
            raise ValueError(
                f"The field '{plot_id_field}' does not exist in the polygon file. "
                f"Available fields: {list(gdf.columns)}"
            )

        gdf = gdf[gdf.geometry.notnull()].copy()
        gdf = gdf[gdf.is_valid].copy()

        if gdf.empty:
            raise ValueError("No valid geometries were found in the polygon file.")

        if gdf.crs != raster_crs:
            gdf = gdf.to_crs(raster_crs)

        raster_bbox_geom = box(*raster_bounds)
        gdf["intersects_raster"] = gdf.geometry.intersects(raster_bbox_geom)

        intersecting = gdf[gdf["intersects_raster"]].copy()
        non_intersecting = gdf[~gdf["intersects_raster"]].copy()

        for _, row in intersecting.iterrows():
            plot_id = str(row[plot_id_field])
            geom = row.geometry

            metadata = dict(base_metadata)
            for col in gdf.columns:
                if col == "intersects_raster":
                    continue
                metadata[col] = make_json_safe(row[col])

            try:
                mask = geometry_mask(
                    [geom],
                    transform=raster_transform,
                    invert=True,
                    out_shape=raster_data.shape,
                )

                valid_mask = mask & (~nodata_mask)
                values = raster_data[valid_mask]
                values = values[~np.isnan(values)]
                

                if values.size == 0:
                    if include_no_coverage:
                        documents.append({
                            "date": date,
                            "metadata": metadata,
                            "avg": None,
                            "max": None,
                            "min": None,
                            "stddev": None,
                            "count": 0,
                            "p10": None,
                            "p50": None,
                            "p90": None,
                            "status": "empty_intersection",
                        })
                    continue
                
                histogram = build_histogram(values, bins=10, hist_range=(0, 1))
                doc: Dict[str, Any] = {
                    "date": date,
                    "metadata": metadata,
                    "avg": float(np.mean(values)),
                    "max": float(np.max(values)),
                    "min": float(np.min(values)),
                }

                if extra_metrics:
                    doc["stddev"] = float(np.std(values))
                    doc["count"] = int(values.size)
                    doc["p10"] = float(np.percentile(values, 10))
                    doc["p50"] = float(np.percentile(values, 50))
                    doc["p90"] = float(np.percentile(values, 90))
                    doc["histogram"] = histogram
                documents.append(doc)

            except Exception as e:
                if include_no_coverage:
                    documents.append({
                        "date": date,
                        "metadata": metadata,
                        "avg": None,
                        "max": None,
                        "min": None,
                        "stddev": None,
                        "count": 0,
                        "p10": None,
                        "p50": None,
                        "p90": None,
                        "status": "error",
                        "error_message": str(e),
                    })

        if include_no_coverage:
            for _, row in non_intersecting.iterrows():
                metadata = dict(base_metadata)
                for col in gdf.columns:
                    if col == "intersects_raster":
                        continue
                    metadata[col] = make_json_safe(row[col])

                documents.append({
                    "date": date,
                    "metadata": metadata,
                    "avg": None,
                    "max": None,
                    "min": None,
                    "stddev": None,
                    "count": 0,
                    "p10": None,
                    "p50": None,
                    "p90": None,
                    "status": "no_coverage",
                })

    return documents