from utils import *

PROV = 'Sichuan_Sichuan_msb_20220814'

get_files(PROV)

FILE_NAMES = []

FILE_NUM = 1

col_headers = []

parsed_df, raw_df = init(PROV, FILE_NAMES, FILE_NUM, col_headers)

# If raw_df exists
if is_df(raw_df):
    parsed_df, raw_df = drop_common_cols(parsed_df, raw_df)
    print_results(parsed_df, raw_df)

else:
    print_results(parsed_df, None)
