## Instructions for 0_raw directory 

This directory should contain the following directories:
 - NCDOT 20XX AADT Traffic Segments
   - This file is updated and maintained by NCDOT, and the most recent version is *NCDOT_2020_Traffic_Segment_Shapefile_Description*. This file can be downloaded from [Traffic Survey GIS Data Products & Documents - Connect NDOT](https://connect.ncdot.gov/resources/State-Mapping/Pages/Traffic-Survey-GIS-Data.aspx), and must be the folder extracted from the .zip file available at the link.
   - The name of the  folder within **0_raw** and the name of the shapefile should be specified by the user in the *Config.py* file as the variables ```DIR_AADT_SEGMENTS``` and ```SHAPEFILE_AADT```, respectively.
 - Planning Level Section Safety Scores
   - This file is updated and maintained by NCDOT, and the most recent version can be requested from NCDOT or is viewable in ArcGIS Online: [Planning Level Safety Scoring Data](https://ncdot.maps.arcgis.com/home/webmap/viewer.html?webmap=7415a4df4df1468585225bc74a77369b)
   - The data placed in **0_raw** folder should be a folder/directory containing the uncompressed shapefile.
   - The name of the folder should be specified in the *Config.py* file as the variables ```DIR_SAFETY_SCORES```and ```SHAPEFILE_SAFETY```, respectively.
 - File 3
   - *File 3 description...*