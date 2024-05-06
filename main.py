from utils import *

# Province name
PROV = 'Sichuan_Sichuan_msb_20220814'

get_files(PROV)

# List of file names to be read in
FILE_NAMES = []

# Set current file number
NUM = 1

# Set column headers
col_headers = []

# Initialize the parsed and raw dataframes
parsed_df, raw_df = init(PROV, FILE_NAMES, NUM, col_headers)

# If raw_df exists
if is_df(raw_df):
    # Drop common columns from the dataframe
    parsed_df, raw_df = drop_common(parsed_df, raw_df)

    # Print the results
    print_results(parsed_df, raw_df)

else:
    # Print the results
    print_results(parsed_df, None)
