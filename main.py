from utils import *

PROV = 'Chaozhou_Guangdong_msb_20230823'

get_files(PROV)

FILE_NAMES = []

FILE_NUM = 1

col_headers = []

parsed_df, raw_df = init(PROV, FILE_NAMES, FILE_NUM, col_headers)

if df_exists(raw_df):
    parsed_df, raw_df = remove_common_cols(parsed_df, raw_df)

results(parsed_df, raw_df)
