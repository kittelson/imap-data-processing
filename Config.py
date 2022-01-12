# This file configures the locations and names of key files and folders

class Config(object):
    DEBUG = False
    TESTING = False


class DataConfig(Config):
    pass
    # ------- Files and Directories ---------
    # Name of folder/directory containing AADT Traffic Segment Data
    DIR_AADT_SEGMENTS = "NCDOT_2020_Traffic_Segment_Shapefile_Description"
    # Name of AADT Segments shapefile (.shp) in the DIR_AADT_SEGMENTS folder/directory
    SHAPEFILE_AADT = "NCDOT_AADT_Segments.shp"
    # Field/column name identifier for AADT in the SHAPEFILE_AADT shapefile (e.g. AADT_2020)
    FIELD_AADT = "aadt_2020"
    # Field/column name identifier for AADTT (Truck AADT) in the SHAPEFILE_AADT shapefile (e.g. AADTT_2020)
    FIELD_AADTT = "aadtt_2020"
    # Name of Section Safety Scores directory (e.g. SectionScores_2015_2019 or SectionSafetyScores_2016_2020)
    DIR_SAFETY_SCORES = "SectionSafetyScores_2016_2020"
    # Name of Section Safety Scores shapefile in DIR_SAFETY_SCORES (e.g. SectionScores_2015_2019.shp)
    SHAPEFILE_SAFETY = "SectionSafetyScores_2016_2020.shp"
    # Field representing the GIS Route identifier in the SHAPEFILE_SAFETY shapefile
    FIELD_GIS_ROUTE = "gis_route"
    # Field representing "total crashes" in the SHAPEFILE_SAFETY shapefile
    FIELD_TOTAL_CNT = "crash_cnt"


class DevConfig(Config):
    # ------- Files and Directories ---------
    DIR_NAME_DATA = "data"
    DIR_NAME_RAW = "0_raw"
    DIR_NAME_INTERIM = "1_interim"
    DIR_NAME_PROCESSED = "2_processed"
    INTERIM_GPKG_AADT = "ncdot_aadt_processed.gpkg"
    INTERIM_GPKG_SAFEY = "ncdot_section_safety_processed.gpkg"




