from utils import *

# List of file names to be read in
FILE_NAMES = ['111.docx.pkl.gz', '123347附件.docx.pkl.gz', '123609111.docx.pkl.gz',
              '2018年第四季度各类食品监督抽检结果汇总表.docx.pkl.gz',
              '内蒙古自治区2018年第12批食品添加剂生产许可证获证生产企业名单.docx.pkl.gz',
              '内蒙古自治区2019年第4批食品添加剂生产许可证获证生产企业名单.docx.pkl.gz',
              '内蒙古自治区2019年第6批食品添加剂生产许可证获证生产企业名单.doc.pkl.gz',
              '附件1_2021年内蒙古自治区第2批保健食品备案产品信息表.doc.pkl.gz',
              '附件1_2021年内蒙古自治区第3批保健食品备案产品信息表.doc.pkl.gz',
              '附件_2020年内蒙古自治区知识产权人才智库专家名单.doc.pkl.gz']

# Province name
PROV = 'Inner Mongolia_Inner Mongolia_msb_20220814'

# Set current file number
NUM = 7

# Set column headers
col_headers = ['序号', '企业名称', '产品名称', '明细', '住所', '生产地址', '证书编号', '有效期至', '发证日期', '批次',
               '说明', '发证机关', '备注']

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
