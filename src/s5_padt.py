# -*- coding: utf-8 -*-
import os
import numpy as np
import pandas as pd
import geopandas as gpd
from src.utils import get_project_root
import inflection
import re
from sklearn.preprocessing import minmax_scale
from Config import DataConfig, DevConfig


# if __name__ == "__main__":
def run_padt_processing():
    path_to_prj_dir = get_project_root()
    path_to_raw = os.path.join(path_to_prj_dir, DevConfig.DIR_NAME_DATA, DevConfig.DIR_NAME_RAW)
    path_interim_data = os.path.join(path_to_prj_dir, DevConfig.DIR_NAME_DATA, DevConfig.DIR_NAME_INTERIM)
    path_to_padt = os.path.join(path_to_raw, DataConfig.DIR_SEG_T3)
    path_to_padt_shapefile = os.path.join(path_to_padt, DataConfig.SHAPEFILE_SEG_T3)
    path_processed_data = os.path.join(path_to_prj_dir, DevConfig.DIR_NAME_DATA, DevConfig.DIR_NAME_PROCESSED)
    if not os.path.isdir(path_processed_data):  # Check if interim data directory exists
        os.mkdir(path_processed_data)  # Create interim data directory if it doesn't exist already
    path_aadt_crash_si = os.path.join(path_interim_data, DevConfig.INTERIM_GPKG_AADT_SAFETY_MERGE)
    crash_aadt_fil_si_geom_gdf = gpd.read_file(path_aadt_crash_si, driver="gpkg")
    route_id_lrs_gdf = crash_aadt_fil_si_geom_gdf.filter(
        items=[
            "route_id",
            "aadt_interval_left",
            "aadt_interval_right",
            "route_class",
            "route_qual",
            "route_inventory",
            "route_county",
            "route_no",
            "geometry",
        ]
    ).assign(
        business_route=lambda df: df.route_qual.apply(
            lambda series: "business" if series == 9 else np.nan
        )
    )

    padt_gpd = gpd.read_file(path_to_padt_shapefile, driver="shp")
    padt_gpd = padt_gpd.to_crs(epsg=4326)
    padt_gpd.columns = [inflection.underscore(col) for col in padt_gpd.columns]
    padt_gpd = padt_gpd[["rte_1_nbr", "rte_1_clss", "street_nam", "padt_rec", "geometry"]]
    pat_bus = re.compile(r"\S+\s+(\S.*)$", flags=re.IGNORECASE)
    padt_gpd["route_qual_padt"] = padt_gpd.street_nam.str.extract(pat_bus)
    padt_gpd["route_qual_padt"] = padt_gpd.route_qual_padt.str.strip().str.lower()
    padt_gpd.rte_1_clss = padt_gpd.rte_1_clss.str.strip().str.upper()
    padt_gpd["route_class"] = np.select(
        [
            padt_gpd.rte_1_clss == "I",
            padt_gpd.rte_1_clss == "US",
            padt_gpd.rte_1_clss == "NC",
        ],
        [1, 2, 3],
        np.nan,
    )
    padt_gpd["route_qual_padt"] = np.select(
        [
            padt_gpd["route_qual_padt"] == np.nan,
            padt_gpd["route_qual_padt"] == "alternate",
            padt_gpd["route_qual_padt"] == "bypass",
            padt_gpd["route_qual_padt"] == "east",
            (padt_gpd["route_qual_padt"] == "connector")
            | (padt_gpd["route_qual_padt"] == "spur"),
            padt_gpd["route_qual_padt"] == "truck route",
            (padt_gpd["route_qual_padt"] == "business")
            | (padt_gpd["route_qual_padt"] == "bus"),
        ],
        [0, 1, 2, 5, 7, 8, 9],
    )
    padt_gpd = padt_gpd.query("~ route_class.isna()", engine='python')
    padt_gpd.rte_1_nbr = padt_gpd.rte_1_nbr.astype(int)

    route_id_lrs_grp = route_id_lrs_gdf.groupby(["route_class", "route_no"])
    padt_gpd_grp = padt_gpd.groupby(["route_class", "rte_1_nbr"])
    route_cls_no_not_found = []
    inc_fac_padt_gpd = gpd.GeoDataFrame()
    inc_fac_padt_gpd_list = []
    for name, route_id_lrs_sub_grp in route_id_lrs_grp:
        if name not in padt_gpd_grp.groups.keys():
            route_cls_no_not_found.append(name)
            continue
        padt_sub_gpd = padt_gpd_grp.get_group(name)
        inc_fac_padt_gpd_list.append(
            gpd.sjoin(left_df=route_id_lrs_sub_grp, right_df=padt_sub_gpd, how="left",)
        )

    inc_fac_padt_gpd = pd.concat(inc_fac_padt_gpd_list, ignore_index=True)
    inc_fac_padt_gpd = (
        inc_fac_padt_gpd.groupby(
            ["route_id", "aadt_interval_left", "aadt_interval_right"]
        )
        .agg(padt_rec=("padt_rec", "max"), geometry=("geometry", "first"),)
        .reset_index()
    )
    inc_fac_padt_gpd = gpd.GeoDataFrame(inc_fac_padt_gpd, crs=route_id_lrs_gdf.crs)
    inc_fac_padt_gpd["seasonal_fac"] = minmax_scale(inc_fac_padt_gpd.padt_rec, (0, 1))
    inc_fac_padt_gpd.to_file(
        os.path.join(path_processed_data, DevConfig.PROCESSED_PADT_ON_INCIDENT_FACTOR), driver="GPKG"
    )
