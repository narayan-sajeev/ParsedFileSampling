from filter import filter_df
from utils import *

PROV = 'Dongguan_Guangdong_msb_20230331'

FILE_NAMES = ['112417附件2_食品监督抽检合格产品信息.pdf']

FILE_NUM = 1

col_headers = []

parsed_df, raw_df = init(PROV, FILE_NAMES, FILE_NUM, col_headers)

if df_exists(raw_df):
    parsed_df, raw_df = remove_common_cols(parsed_df, raw_df)

results(parsed_df, raw_df)

filtered = filter_df(parsed_df)
print(filtered)
