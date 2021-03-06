# This file configures the locations and names of key files and folders

class Config(object):
    DEBUG = False
    TESTING = False


class DataConfig(Config):
    pass
    # ------- Files and Directories ---------
    # Name of folder/directory containing AADT Traffic Segment Data
    # DIR_AADT_SEGMENTS = "NCDOT_2020_Traffic_Segment_Shapefile_Description"
    DIR_AADT_SEGMENTS = "NCDOT 2018 Traffic Segments Shapefile Description"
    # Name of AADT Segments shapefile (.shp) in the DIR_AADT_SEGMENTS folder/directory
    # SHAPEFILE_AADT = "NCDOT_AADT_Segments.shp"
    SHAPEFILE_AADT = "NCDOT_AADT_Traffic_Segments.shp"
    # Field/column name identifier for AADT in the SHAPEFILE_AADT shapefile (e.g. AADT_2020)
    # FIELD_AADT = "aadt_2020"
    FIELD_AADT = "aadt_2018"
    # Field/column name identifier for AADTT (Truck AADT) in the SHAPEFILE_AADT shapefile (e.g. AADTT_2020)
    # FIELD_AADTT = "aadtt_2020"
    FIELD_AADTT = "aadtt2018"
    # Name of Section Safety Scores directory (e.g. SectionScores_2015_2019 or SectionSafetyScores_2016_2020)
    # DIR_SAFETY_SCORES = "SectionSafetyScores_2016_2020"
    DIR_SAFETY_SCORES = "SectionScores_2015_2019"
    # Name of Section Safety Scores shapefile in DIR_SAFETY_SCORES (e.g. SectionScores_2015_2019.shp)
    # SHAPEFILE_SAFETY = "SectionSafetyScores_2016_2020.shp"
    SHAPEFILE_SAFETY = "SectionScores_2015_2019.shp"
    # Field representing the GIS Route identifier in the SHAPEFILE_SAFETY shapefile
    # FIELD_GIS_ROUTE = "gis_route"
    FIELD_GIS_ROUTE = "route_gis"
    # Field representing "total crashes" in the SHAPEFILE_SAFETY shapefile
    # FIELD_TOTAL_CNT = "crash_cnt"
    FIELD_TOTAL_CNT = "total_cnt"
    # Name of the folder/directory containing the HPMS shapefile
    DIR_HPMS = "hpms_northcarolina2018"
    # Name of the HPMS shapefile (.shp) in the DIR_HPMS directory
    SHAPEFILE_HPMS = "NorthCarolina_PR_2018.shp"
    # Name of folder/directory containing the SEG_T3_All Routes_Revised data
    DIR_SEG_T3 = "SEG_T3_All Routes_Revised"
    # Name of the SEG_T3 shapefile in the DIR_SEG_T3 directory
    SHAPEFILE_SEG_T3 = "SEG_T3_PADT_All_Routes_Revised.shp"
    # Name of the folder/directory containing the CensusTract2010.shp shapefile
    DIR_CENSUS_TRACT = "CensusTract2010"
    # Name of the Census Tract shapefile in the DIR_CENSUS_TRACT folder/directory
    SHAPEFILE_CENSUS_TRACT = "CensusTract2010.shp"
    # Name of the CSV file containing the combined flow by census tract data
    CSV_CENSUS_COMBINED_FLOW = "Combined_FlowByCensusTract.csv"
    # Name of the directory/folder containing the IMAP routes shapefile
    DIR_NAME_IMAP_ROUTES = "_IMAP_Routes"
    # Name of the shapefile containing the IMAP routes in the directory specified in DIR_NAME_IMAP_ROUTES
    SHAPEFILE_IMAP_ROUTES = "_IMAP_Routes.shp"
    # Name of the directory/folder containing the NCDOT Division Boundaries
    DIR_NAME_NCDOT_DIVISIONS = "NCDOT_Division_Boundaries-shp"
    # Name of the shapefile containing the IMAP routes in the directory specified in DIR_NAME_NCDOT_DIVISIONS
    SHAPEFILE_NCDOT_DIVISIONS = "NCDOT_Division_Boundaries.shp"


class DevConfig(Config):
    # ------- Files and Directories ---------
    DIR_NAME_DATA = "data"
    DIR_NAME_RAW = "0_raw"
    DIR_NAME_INTERIM = "1_interim"
    DIR_NAME_PROCESSED = "2_processed"
    INPUT_DIR_DETOUR_TESTING = "detour_testing"
    INPUT_SHAPEFILE_DETOUR = "detour_work_ASG.shp"
    INTERIM_GPKG_AADT = "ncdot_aadt_processed.gpkg"
    INTERIM_GPKG_SAFETY = "nc_crash_si_processed.gpkg"
    INTERIM_GPKG_AADT_SAFETY_MERGE = "aadt_crash_merge.gpkg"
    INTERIM_CSV_NHS_STC_ROUTES = "nhs_hpms_stc_routes.csv"  # "nhs_hpms_stc_routes.csv"
    INTERIM_CSV_AADT_BUT_NO_CRASH = "aadt_but_no_crash_route_set.csv"
    PROCESSED_PADT_ON_INCIDENT_FACTOR = "padt_on_inc_fac_gis.gpkg"   # "padt_on_inc_fac_gis.gpkg"
    PROCESSED_CENSUS_GPD_GROWTH = "census_gpd_growth.gpkg"  # "census_gpd_growth.gpkg"
    PROCESSED_INCIDENT_FACTOR_SCALED = "inc_fac_si_scaled.gpkg"
    PROCESSED_DIR_MISSING_CRASHES = "missing_crashes"
    PROCESSED_SHAPEFILE_MISSING_CRASHES = "missing_crash.shp"
    PROCESSED_GPKG_ALL_DATA_MERGE = "ncdot_processed_roadways.gpkg"  # "if_si_detour_nat_imp_census_padt.gpkg"
    FINAL_DIR_NAME = "output"
    FINAL_MERGE_SHAPEFILE = "ncdot_processed_roadways.shp"  # "if_si_detour_nat_imp_census_padt.shp"




