from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, List, Optional

import geopandas as gpd
import rasterio
from rasterio.features import geometry_mask
from shapely.geometry import box
import numpy as np


def calculate_polygon_stats(
    raster_path: str | Path,
    polygons_path: str | Path,
    plot_id_field: str = "lote_id",
    flight_id: Optional[str] = None,
    index_type: str = "NDVI",
    include_no_coverage: bool = True,
) -> List[Dict[str, Any]]:
    """
    Calcula estadísticas zonales simples por polígono sobre un raster.

    Parámetros:
        raster_path: ruta al raster .tif
        polygons_path: ruta al archivo vectorial (gpkg, geojson, shp, etc.)
        plot_id_field: nombre del campo identificador del polígono
        flight_id: identificador del vuelo
        index_type: tipo de índice, por ejemplo NDVI
        include_no_coverage: si True, agrega registros para polígonos sin cobertura

    Regresa:
        Lista de dicts, un registro por polígono.
    """
    raster_path = Path(raster_path)
    polygons_path = Path(polygons_path)

    results: List[Dict[str, Any]] = []

    with rasterio.open(raster_path) as src:
        raster_crs = src.crs
        raster_bounds = src.bounds
        raster_transform = src.transform
        raster_nodata = src.nodata

        # Leer primera banda
        raster_data = src.read(1)

        # Máscara global de nodata
        if raster_nodata is not None:
            nodata_mask = raster_data == raster_nodata
        else:
            nodata_mask = np.zeros(raster_data.shape, dtype=bool)

        # Leer polígonos
        gdf = gpd.read_file(polygons_path)

        if plot_id_field not in gdf.columns:
            raise ValueError(f"El campo '{plot_id_field}' no existe en el archivo de polígonos.")

        # Reproyectar al CRS del raster si hace falta
        if gdf.crs != raster_crs:
            gdf = gdf.to_crs(raster_crs)

        # Filtrar geometrías válidas
        gdf = gdf[gdf.geometry.notnull()].copy()
        gdf = gdf[gdf.is_valid].copy()

        # Crear bbox del raster como polígono
        raster_bbox_geom = box(*raster_bounds)

        # Detectar intersección con el raster
        gdf["intersects_raster"] = gdf.geometry.intersects(raster_bbox_geom)

        intersecting = gdf[gdf["intersects_raster"]].copy()
        non_intersecting = gdf[~gdf["intersects_raster"]].copy()

        # Procesar solo los que intersectan
        for _, row in intersecting.iterrows():
            plot_id = row[plot_id_field]
            geom = row.geometry

            record: Dict[str, Any] = {
                "flight_id": flight_id,
                "raster_path": str(raster_path),
                "index_type": index_type,
                "plot_id": plot_id,
                "status": None,
                "mean": None,
                "min": None,
                "max": None,
                "stddev": None,
                "count": None,
            }

            try:
                # Máscara del polígono sobre el raster
                mask = geometry_mask(
                    [geom],
                    transform=raster_transform,
                    invert=True,   # True dentro del polígono
                    out_shape=raster_data.shape,
                )

                # Combinar máscara del polígono con nodata
                valid_mask = mask & (~nodata_mask)
                values = raster_data[valid_mask]

                # Quitar NaN si existen
                values = values[~np.isnan(values)]

                if values.size == 0:
                    record["status"] = "empty_intersection"
                else:
                    record["status"] = "ok"
                    record["mean"] = float(np.mean(values))
                    record["min"] = float(np.min(values))
                    record["max"] = float(np.max(values))
                    record["stddev"] = float(np.std(values))
                    record["count"] = int(values.size)

            except Exception as e:
                record["status"] = "error"
                record["error_message"] = str(e)

            results.append(record)

        # Agregar polígonos sin cobertura si así lo quieres
        if include_no_coverage:
            for _, row in non_intersecting.iterrows():
                results.append(
                    {
                        "flight_id": flight_id,
                        "raster_path": str(raster_path),
                        "index_type": index_type,
                        "plot_id": row[plot_id_field],
                        "status": "no_coverage",
                        "mean": None,
                        "min": None,
                        "max": None,
                        "stddev": None,
                        "count": None,
                    }
                )

    return results


if __name__ == "__main__":
    raster = "2026-01-28.tif"
    polygons = "lotes.gpkg"

    records = calculate_polygon_stats(
        raster_path=raster,
        polygons_path=polygons,
        plot_id_field="lote_id",
        flight_id="vuelo_2026_04_01",
        index_type="NDVI",
        include_no_coverage=True,
    )

    for r in records:
        print(r)