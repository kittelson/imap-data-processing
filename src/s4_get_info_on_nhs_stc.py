"""
Explore NHS, STC routes. Create shapefiles for visualizing NHS and STC routes.
"""
from io import StringIO
import pandas as pd
import os
from src.utils import get_project_root
import geopandas as gpd
import numpy as np
from Config import DataConfig, DevConfig


def get_strategic_trans_cor():
    """
    Create a dataframe for North Carolina strategic transportation routes.
    """
    stc_string_io = StringIO(
        """route_class,route_no
        US,74
        US,441
        I,26 
        US,23 
        US,321
        US,421
        I,73
        I,77
        I,74
        I,85
        I,285
        US,29
        NC,87
        US,1
        I,495
        US,64
        US,13
        US,17 
        US,70
        I,40
        NC,49
        I,795
        US,117
        I,95
        US,264
        US,401
        NC,24
        US,258
        NC,11
        US,158
    """
    )
    stc_df_ = pd.read_csv(stc_string_io)
    stc_df_ = stc_df_.assign(
        route_class=lambda df: df.route_class.astype(str).str.strip(),
        route_no=lambda df: df.route_no.astype(int),
    )
    stc_df_.loc[:, "stc"] = True
    return stc_df_


def test_all_stc_in_study_gdf(
    study_gdf, stc_df_, merge_cols=("route_class", "route_no")
):
    """
    Test all NC strategic transportation routes are in the study df: aadt or crash data.
    Parameters
    ----------
    study_gdf: gpd.GeoDataFrame()
        aadt or crash data.
    stc_df_: pd.DataFrame()
        NC strategic transportation routes.
    merge_cols
        Columns to the merge study_gdf and stc_df_.
    Returns
    -------
    stc_study_routes[~stc_study_routes.route_in_study_gdf]: pd.DataFrame()
        stc DataFrame with routes in stc but not in aadt or crash data.
    """
    study_routes = pd.DataFrame(
        study_gdf.loc[:, merge_cols]
        .drop_duplicates(subset=merge_cols)
        .reset_index(drop=True)
        .assign(
            route_class=lambda df: df.route_class.replace(
                {1: "I", 2: "US", 3: "NC", 4: "Secondary Routes"}
            ),
            route_in_study_gdf=True,
        )
    )
    stc_study_routes = stc_df_.merge(study_routes, on=merge_cols, how="left").assign(
        route_in_study_gdf=lambda df: df.route_in_study_gdf.fillna(False)
    )
    return stc_study_routes[~stc_study_routes.route_in_study_gdf]


def routes_in_hpms_nhs(hpms_nc_, stc_df_):
    """
    Find routes that both NHS and NC STC routes.
    Parameters
    ----------
    hpms_nc_: pd.DataFrame()
        HPMS 20XX NC data.
    stc_df_: pd.DataFrame()
        North Carolina strategic transportation routes.
    Returns
    -------
    hpms_nc_fil_stc: pd.DataFrame()
        Dataframe with a columns "stc" that is true if an NHS route is also a STC route.
    """
    hpms_nc_fil_ = (
        hpms_nc_.replace({"route_sign": {2: "I", 3: "US", 4: "NC"}})
        .query("route_sign in ['I', 'US', 'NC']")
        .assign(
            nhs_net=lambda df: np.select(
                [df.nhs == 0, df.nhs != 0], [False, True], np.nan
            ).astype(bool)
        )
        .filter(
            items=[
                "route_id",
                "route_sign",
                "route_numb",
                "route_qual",
                "nhs",
                "nhs_net",
                "strahnet_t",
            ]
        )
        .drop_duplicates(["route_id"])
        .rename(columns={"route_sign": "route_class", "route_numb": "route_no"})
    )
    hpms_nc_fil_stc = (
        hpms_nc_fil_.merge(stc_df_, on=["route_class", "route_no"], how="outer",)
        .assign(stc=lambda df: df.stc.fillna(False))
        .sort_values(["route_class", "route_no"])
    )
    return hpms_nc_fil_stc


# if __name__ == "__main__":
def run_get_info_on_nhs_stc():
    # Set the paths to relevant files and folders.
    # Load cleaned AADT, crash, AADT+Crash data; output form crash.py, aadt.py, and
    # aadt_crash_merge.py.
    # Load HPMS NC 2018 raw data to get NHS information for the routes.
    # ************************************************************************************
    path_to_prj_dir = get_project_root()
    path_to_prj_data = os.path.join(path_to_prj_dir, DevConfig.DIR_NAME_DATA, DevConfig.DIR_NAME_RAW)
    path_interim_data = os.path.join(path_to_prj_dir, DevConfig.DIR_NAME_DATA, DevConfig.DIR_NAME_INTERIM)
    path_aadt_nc = os.path.join(path_interim_data, DevConfig.INTERIM_GPKG_AADT)
    path_hpms = os.path.join(path_to_prj_data, DataConfig.DIR_HPMS, DataConfig.SHAPEFILE_HPMS)
    hpms_nc = gpd.read_file(path_hpms)
    aadt_gdf = gpd.read_file(path_aadt_nc, driver="gpkg")
    aadt_gdf_fil = aadt_gdf.loc[lambda df: df.route_class.isin([1, 2, 3])]
    stc_df = get_strategic_trans_cor().assign(stc=True)

    # Test for missing stc routes in aadt or crash data.
    # ************************************************************************************
    test_all_stc_in_study_gdf(aadt_gdf_fil, stc_df)
    # Get I, US, or NC routes in aadt_gdf_fil, but not in hpms_2018_nc
    # ************************************************************************************
    routes_in_aadt_not_hpms = set(aadt_gdf_fil.route_id) - set(hpms_nc.route_id)
    aadt_gdf_fil_test_missing_routes = aadt_gdf_fil.loc[
        lambda df: df.route_id.isin(routes_in_aadt_not_hpms)
    ]
    print(
        f"HPMS has info on all routes in the AADT layer expect for the following"
        f" :{aadt_gdf_fil_test_missing_routes.route_id.values}"
    )
    # Find routes in hpms nhs and not in stc
    # ************************************************************************************
    hpms_nc_fil = routes_in_hpms_nhs(hpms_nc_=hpms_nc, stc_df_=stc_df)
    # Get route IDs with NHS and STC info
    # ************************************************************************************
    hpms_nc_fil_1 = hpms_nc_fil.filter(items=["route_id", "stc", "nhs_net"])
    aadt_gdf_fil_route = aadt_gdf_fil.filter(items=["route_id"]).drop_duplicates()
    aadt_nhs_stc_df = aadt_gdf_fil_route.merge(hpms_nc_fil_1, on="route_id", how="left")
    aadt_nhs_stc_df[["stc", "nhs_net"]] = aadt_nhs_stc_df[["stc", "nhs_net"]].fillna(False)
    # aadt_nhs_stc_df.columns
    aadt_nhs_stc_df = aadt_nhs_stc_df.assign(
        nat_imp_fac=lambda df: np.select(
            [
                df.nhs_net == True,
                (df.stc == True) & (df.nhs_net == False),
                (df.stc == False) & (df.nhs_net == False),
            ],
            [1, 0.5, 0],
            "error",
        ),
        nat_imp_cat=lambda df: np.select(
            [
                df.nhs_net == True,
                (df.stc == True) & (df.nhs_net == False),
                (df.stc == False) & (df.nhs_net == False),
            ],
            ["nhs", "stc_but_not_nhs", "other"],
            "error",
        ),
    )
    # Output routes in hpms nhs and not in stc
    # ************************************************************************************
    out_path_hpms_nc_fil = os.path.join(path_interim_data, DevConfig.INTERIM_CSV_NHS_STC_ROUTES)
    aadt_nhs_stc_df.to_csv(out_path_hpms_nc_fil, index=False)
