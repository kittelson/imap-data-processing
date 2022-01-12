from src.s1_aadt import run_aadt_init_process
from src.s2_crash import run_safety_init_process
from src.s3_aadt_crash_merge import run_aadt_crash_merge
if __name__ == "__main__":
    # ----------- Execute the code
    # - Step 1: Process NCDOT AADT Data
    run_aadt_init_process()
    # - Step 2: Process the Safety data
    run_safety_init_process()
    # - Step 3: Merge the AADT and Crash Data
    run_aadt_crash_merge()

