"""
Handle missing crash data for incidence factor and severity index calculations.
Created by: Apoorba Bibeka
Modified by: Lake Trask (2022/01/22)
"""
import geopandas as gpd
import os
from src.utils import get_project_root
import numpy as np
from sklearn.preprocessing import minmax_scale
from Config import DevConfig


# if __name__ == "__main__":
def run_process_incident_factor():
    # Set the paths to relevant files and folders.
    # Load crash and aadt data.
    # ************************************************************************************
    path_to_prj_dir = get_project_root()
    path_interim_data = os.path.join(path_to_prj_dir, DevConfig.DIR_NAME_DATA, DevConfig.DIR_NAME_INTERIM)
    path_processed_data = os.path.join(path_to_prj_dir, DevConfig.DIR_NAME_DATA, DevConfig.DIR_NAME_PROCESSED)
    path_aadt_crash_si = os.path.join(path_interim_data, DevConfig.INTERIM_GPKG_AADT_SAFETY_MERGE)
    path_inc_fac_si = os.path.join(path_processed_data, DevConfig.PROCESSED_INCIDENT_FACTOR_SCALED)

    path_aadt_but_no_crash_route_set = os.path.join(path_interim_data, DevConfig.INTERIM_CSV_AADT_BUT_NO_CRASH)
    crash_aadt_fil_si_geom_gdf = gpd.read_file(path_aadt_crash_si, driver="gpkg")
    crash_aadt_fil_si_geom_gdf = (
        crash_aadt_fil_si_geom_gdf
        .sort_values(by=["route_id", "aadt_interval_left"])
        .assign(route_class=lambda df: df.route_class.replace(
            {1: "Interstate", 2: "US Route", 3: "NC Route", 4: "Secondary Routes"})
            )
        .query("route_class in ['Interstate', 'US Route', 'NC Route']")
    )
    crash_df_fil_si_geom_gdf_nan = crash_aadt_fil_si_geom_gdf.query(
        " severity_index.isna()"
    )
    crash_df_fil_si_geom_gdf_no_nan = crash_aadt_fil_si_geom_gdf.query(
        "~ severity_index.isna()"
    )
    crash_df_fil_si_geom_gdf_nan.to_csv(path_aadt_but_no_crash_route_set)

    crash_aadt_fil_si_geom_gdf.groupby("route_class").severity_index.quantile(.95)
    crash_df_fil_si_geom_gdf_no_nan.severity_index.quantile(.90)
    crash_df_fil_si_geom_gdf_no_nan.inc_fac.describe()
    quantile_90th = crash_aadt_fil_si_geom_gdf.severity_index.quantile(.90)

    crash_aadt_fil_si_geom_gdf_scaled_si = crash_aadt_fil_si_geom_gdf.assign(
        severity_index=lambda df: df.severity_index.fillna(1),
        severity_index_q90=quantile_90th,
        severity_index_need_scaling=lambda df: np.select(
            [
                df.severity_index.isna(),
                df.severity_index <= quantile_90th,
                df.severity_index > quantile_90th],
            [np.nan, True, False]
        ),
        severity_index_scaled=lambda df: (
            df.groupby("severity_index_need_scaling")
            .severity_index
            .transform(lambda x: minmax_scale(x, (0, 1)))),
    )

    crash_aadt_fil_si_geom_gdf_scaled_si.loc[
        lambda x: ~ x.severity_index_need_scaling.astype(bool),
        "severity_index_scaled"
        ] = 1
    crash_aadt_fil_si_geom_gdf_scaled_si.to_file(path_inc_fac_si, driver="GPKG")

    path_missing_crash = os.path.join(path_processed_data, DevConfig.PROCESSED_DIR_MISSING_CRASHES)
    if not os.path.isdir(path_missing_crash):
        os.mkdir(path_missing_crash)
    path_missing_crash_shp = os.path.join(path_missing_crash, DevConfig.PROCESSED_SHAPEFILE_MISSING_CRASHES)
    crash_df_fil_si_geom_gdf_nan.to_file(path_missing_crash_shp)
