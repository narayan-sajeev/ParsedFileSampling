# Import necessary libraries
import json
import os
import random

import pandas as pd

# Define the root directory where the parsed files are located
ROOT = '/Users/narayansajeev/Desktop/MIT/parsed_files/'


# Function to get file names from a directory
def get_fnames(DIR):
    current = []
    files = []
    # Loop through each file in the directory
    for file in os.listdir(DIR):
        # Check if the file is an Excel file and if it's not already in the list of files
        is_xl = file.endswith(('.xlsx', '.xls'))
        pkl = file + '.pkl.gz'
        is_food = not any([i in file for i in ['业', '商']])
        if is_xl and file not in current and pkl in os.listdir(DIR) and is_food:
            files.append(file)

    # Shuffle the list of files
    random.shuffle(files)

    # Print the first 25 files in the list
    print('\n'.join(sorted(files[:25])))

    quit()


# Function to print a specific file name from a list of file names
def print_fname(i):
    # list of file names to be read in
    fnames = ['041821不合格产品信息.xls.pkl.gz', '041838合格产品信息.xls.pkl.gz',
              '041849附件3_合格产品信息.xlsx.pkl.gz', '041930不合格产品信息.xls.pkl.gz',
              '042127不合格产品信息.xls.pkl.gz', '042146附件3_合格产品信息.xlsx.pkl.gz',
              '042331不合格产品信息.xls.pkl.gz', '042425合格产品信息.xls.pkl.gz', '042448不合格产品信息.xls.pkl.gz',
              '042526不合格产品信息.xls.pkl.gz', '042536不合格产品信息.xls.pkl.gz', '042602不合格产品信息.xls.pkl.gz',
              '042654合格产品信息.xls.pkl.gz', '0427503、合格产品信息.xls.pkl.gz', '0428072、不合格产品信息.xlsx.pkl.gz',
              '042858不合格产品信息.xls.pkl.gz', '0429152、不合格产品信息.xlsx.pkl.gz', '042937合格产品信息.xls.pkl.gz',
              '0430442、不合格产品信息.xlsx.pkl.gz', '043101不合格产品信息.xls.pkl.gz',
              '0432462、不合格产品信息.xls.pkl.gz', '0433012、不合格产品信息.xls.pkl.gz',
              '0433202、不合格产品信息.xls.pkl.gz', '0433352、不合格产品信息.xls.pkl.gz',
              '2、月饼专项监督抽检不合格产品信息.xls.pkl.gz']

    # select the filename from the list
    fname = fnames[i]
    return fname


# Function to read a dataframe from a pickle file
def get_df(DIR, fname):
    # read in the first file from the list using pandas
    return pd.read_pickle('%s/%s' % (DIR, fname))


# Function to get known column classifiers from a JSON file
def get_known_cols():
    # checking column classifier
    known_cols_fn = '/Users/narayansajeev/Desktop/MIT/known_columns.json'
    with open(known_cols_fn) as f:
        return json.load(f)


# Function to clean column headers by removing newline, carriage return, non-breaking space and space characters
def clean(col_headers):
    return [n.replace('\n', '').replace('\r', '').replace('\xa0', '').replace(' ', '') for n in col_headers]


# Function to check substrings in column headers
def substr_check(substr_sets, k):
    substr_dict = {}
    for s in substr_sets:
        try:
            substr_dict[s] = any([i in k for i in substr_sets[s]])
        except TypeError:
            print(k)
            raise
    return substr_dict


# Function to check substrings for unmatched columns
def substring(df, known_cols, col_headers):
    # Clean up the column headers
    col_headers = clean(col_headers)

    # Try to remove '判定依据' from the column headers
    try:
        col_headers.remove('判定依据')
    except:
        pass

    # dictionary of substrings to check for in column headers
    substr_sets = {
        'announcement_date': ['检', '抽', '报'],
        'address': ['地址', '所在地'],
        'region': ['省', '县', '市', '区'],
        'been_sampled': ['受', '被'],
        'name': ['名称', '单位', '机构', '人'],
        'testing_agency': ['采样', '检', '抽', '委托'],
        'value_or_result': ['值', '结果', '要求'],
        'limit': ['标', '限'],
        'actual': ['测', '检', '实', '不合格'],
        'result': ['结论', '结果', '判定'],
        'not_qualified': ['不合格', '不符合'],
        'item_or_reason': ['项', '原因'],
        'produce': ['生产'],
        'illegal': ['违法']
    }

    # list of possible inspection result values
    insp_res_vals = [
        '合格',
        '合  格',
        '合格（阴性）',
        '符合',
        '不合格',
        '不符合',
        '所检项目符合标准',
        '铝不符合国家标准',
        '所检项目符合标准要求。',
        '所检项目符合国家标准或地方标准']

    # list to store unmatched column headers
    unmatched_cols = []
    # review these cols
    review_cols = []

    # check for unmatched column headers
    for term in col_headers:
        if term not in known_cols:
            unmatched_cols.append(term)

    # check substrings for unmatched columns
    for k in unmatched_cols:
        substr_dict = substr_check(substr_sets, k)
        if '日期' in k and '生产' in k:
            continue
        if '日期' in k and substr_dict['announcement_date']:
            continue
        if substr_dict['address'] and '生产' in k and not substr_dict['region']:
            continue
        if substr_dict['address'] and substr_dict['been_sampled'] and not substr_dict['region']:
            continue
        if substr_dict['name'] and substr_dict['been_sampled'] and not substr_dict['address']:
            cup = k
            for i in substr_sets['name']:
                cup = cup.replace(i, '')
            for i in substr_sets['been_sampled']:
                cup = cup.replace(i, '')
            for i in substr_sets['testing_agency']:
                cup = cup.replace(i, '')
            if len(cup) <= 1:
                continue
        if substr_dict['name'] and '生产' in k and not substr_dict['address'] and not '违法' in k:
            continue
        if substr_dict['name'] and substr_dict['testing_agency'] and not substr_dict['been_sampled']:
            cup = k
            for i in substr_sets['name']:
                cup = cup.replace(i, '')
            for i in substr_sets['testing_agency']:
                cup = cup.replace(i, '')
            if len(cup) <= 1:
                continue
        if substr_dict['value_or_result'] and substr_dict['limit']:
            if substr_dict['actual']:
                continue
            else:
                continue
        if substr_dict['value_or_result'] and substr_dict['actual']:
            continue
        if substr_dict['not_qualified'] and substr_dict['item_or_reason'] and '数' not in k:
            continue
        if '规格' in k:
            continue
        if substr_dict['result']:
            if len(df.index) >= 5:
                first_obs = df.iloc[0:4][k]
            else:
                first_obs = df.loc[:][k]
            if any([n in insp_res_vals for n in first_obs]):
                continue
        else:
            review_cols.append(k)

    return review_cols


# Function to drop columns from the dataframe
def drop_columns(df, col_headers):
    cols = ['manufacturer_name', 'manufacturer_address', 'sampled_location_name', 'sampled_location_address',
            'food_name', 'specifications_model', 'announcement_date', 'production_date',
            'product_classification', 'task_source_or_project_name', 'testing_agency', 'adulterant',
            'inspection_results', 'failing_results', 'test_outcome', 'legal_limit']

    # select only the existing columns from the dataframe
    df = df[[col for col in cols if col in df.columns]]

    # Drop columns that have all NaN values
    drop_df = df.dropna(axis=1, how='all')

    # List to store dropped column headers
    dropped = []

    # Check for dropped column headers
    for col in df.columns:
        if col not in drop_df.columns:
            dropped.append(col)

    # Sort column headers
    col_headers = sorted(col_headers)

    # Remove unnecessary column headers
    for _ in ['商标', '备注', '序号', '抽样编号', '购进日期', '被抽样单位省', '被抽样单位盟市', '被抽样单位所在盟市',
              '公告网址链接', '产品具体名称', '销售单位/电商']:
        try:
            col_headers.remove(_)
        except:
            pass

    # Print unmatched and dropped column headers
    if len(dropped) > 0 and len(col_headers) > 0:
        print(col_headers)
        print(dropped)

    return drop_df


# Function to print a horizontal line
def hr():
    print('*' * 300)


# Function to print a newline
def newline():
    print()


# Function to print the dataframe
def print_df(DIR, df, fname):
    # Replace spaces in the directory path with '\ '
    DIR = DIR.replace(' ', '\ ')

    # Construct the full directory path
    full_dir = '%s/%s' % (DIR, fname)
    full_dir = full_dir.split('.pkl')[0]

    # Open the file in the default application
    s = 'open %s' % full_dir
    os.system(s)

    # Print the file path and number of rows in the dataframe
    print(s.split(ROOT)[-1])
    print('Rows:', len(df))

    # Print first few rows of the dataframe
    newline()
    print(df.head())
    hr()

    if len(df) > 10:
        print(df.tail())

        # Check for duplicate rows
        dup_df = df[df.duplicated(keep=False)]

        # If there are duplicate rows, print them
        if len(dup_df) > 0:
            hr()
            print('Duplicates:', len(dup_df))
            print(dup_df)

        # Check for empty columns
        empty_cols = df.columns[df.isnull().any()]

        # If there are empty columns, print them
        if len(empty_cols) > 0:
            hr()
            print('Empty:', len(empty_cols))
            print(df[empty_cols])

        # If 'adulterant' is a column in the dataframe, check for null values
        if 'adulterant' in df.columns:

            df_adulterant_none = df[df['adulterant'].isnull()]

            # If there are rows where 'adulterant' is null, print them
            if len(df_adulterant_none) > 0:
                hr()
                print('Adulterant None:', len(df_adulterant_none))
                print(df_adulterant_none)

    else:
        print(df.tail(len(df) - 5))


# Define the directory where the parsed files are located
dir = ROOT + 'Jiangsu_Jiangsu_msb_20220815'

# Set pandas option to display all columns
pd.set_option('display.max_columns', None)

# Get the file name from the list of file names
fname = print_fname(0)

# Read the dataframe from the pickle file
df = get_df(dir, fname)

# Get the column headers from the dataframe
col_headers = ['序号', '标称生产企业名称', '标称生产企业地址', '被抽样单位名称', '被抽样单位地址', '食品名称',
               '规格型号', '商标', '生产日期/批号', '不合格项目║检验结果║标准值', '分类', '任务来源', '检验机构',
               '备注']

# Check substrings for unmatched columns
review_cols = substring(df, get_known_cols(), col_headers)

# Drop unnecessary columns from the dataframe
df = drop_columns(df, review_cols)

# Print the dataframe
print_df(dir, df, fname)
