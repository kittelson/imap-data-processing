from src.s1_aadt import run_aadt_init_process
from src.s2_crash import run_safety_init_process
from src.s3_aadt_crash_merge import run_aadt_crash_merge
from src.s4_get_info_on_nhs_stc import run_get_info_on_nhs_stc
from src.s5_padt import run_padt_processing
from src.s6_census_growth_rate import run_process_census_data
from src.s7_if_si_calc import run_process_incident_factor
if __name__ == "__main__":
    # ----------- Execute the code
    # - Step 1: Process NCDOT AADT Data
    run_aadt_init_process()
    # - Step 2: Process the Safety data
    run_safety_init_process()
    # - Step 3: Merge the AADT and Crash Data
    run_aadt_crash_merge()
    # - Step 4: Get info on NHS and Strategic corridors from HPMS, etc.
    run_get_info_on_nhs_stc()
    # - Step 5: Process the PADT data
    run_padt_processing()
    # - Step 6: Process the Census Tract and Growth Data
    run_process_census_data()
    # - Step 7: Incident Factor Scaling
    run_process_incident_factor()

