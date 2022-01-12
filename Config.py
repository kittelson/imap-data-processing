# This file configures the locations and names of key files and folders

class Config(object):
    DEBUG = False
    TESTING = False


class DataConfig(Config):
    pass
    # ------- Files and Directories ---------
    # Path to "raw" data folder/directory
    # PATH_DATA_RAW = "data/0_raw"
    # Path to "interim" data folder/directory
    # PATH_DATA_INTERIM = "data/1_interim"
    # Path to "processed" data folder/directory
    # PATH_DATA_PROCESSED = "data/2_processed"
    # Name of folder/directory containing AADT Traffic Segment Data
    DIR_AADT_SEGMENTS = "NCDOT_2020_Traffic_Segment_Shapefile_Description"
    # Name of AADT Segments shapefile (.shp) in the DIR_AADT_SEGMENTS folder/directory
    SHAPEFILE_AADT = "NCDOT_AADT_Segments.shp"
    # Field/column name identifier for AADT in the SHAPEFILE_AADT shapefile (e.g. AADT_2020)
    FIELD_AADT = "aadt_2020"
    # Field/column name identifier for AADTT (Truck AADT) in the SHAPEFILE_AADT shapefile (e.g. AADTT_2020)
    FIELD_AADTT = "aadtt_2020"


class DevConfig(Config):
    # ------- Files and Directories ---------
    DIR_NAME_DATA = "data"
    DIR_NAME_RAW = "0_raw"
    DIR_NAME_INTERIM = "1_interim"
    DIR_NAME_PROCESSED = "2_processed"
    INTERIM_GPKG_AADT = "ncdot_aadt_processed.gpkg"




