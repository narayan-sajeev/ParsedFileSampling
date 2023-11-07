# Import necessary libraries
import json
import os
import random
from datetime import datetime

import pandas as pd

# Define the root directory where the parsed files are located
ROOT = '/Users/narayansajeev/Desktop/MIT/parsed_files/'


# Function to get all file names from a directory
def get_all_fnames(DIR):
    current = []
    files = []
    # Loop through each file in the directory
    for file in os.listdir(DIR):
        # Check if the file is an Excel file and if it's not already in the list of files
        is_xl = file.endswith(('.xlsx', '.xls'))
        pkl = file + '.pkl.gz'
        is_food = not any([i in file for i in ['业', '商', '饮', '酒']])
        if is_xl and file not in current and pkl in os.listdir(DIR) and is_food:
            files.append(file)

    # Shuffle the list of files
    random.shuffle(files)

    # Print the first 25 files in the list
    print('\n'.join(sorted(files[:25])))

    quit()


# Function to retrieve a specific file name from a list of file names
def retrieve_fname_at(i):
    # list of file names to be read in
    fnames = ['162942胡麻油专项监督抽检结果信息表.xlsx.pkl.gz', '固原市2020年监督抽检信息.xlsx.pkl.gz',
              '固原市市场监督管理局2020年食品_安全监督抽检信息（市抽）.xls.pkl.gz',
              '固原市市场监督管理局2020年食品安全监督抽检信息（市抽）.xls.pkl.gz',
              '固原市市场监督管理局2021年食品安全_监督抽检结果信息表.xlsx.pkl.gz',
              '固原市市场监督管理局经济开发区分局2021年食品安全监督抽检样品信息表（国测食用农产品）.xlsx.pkl.gz',
              '固原市市场监督管理局经济开发区分局2021年食品安全监督抽检样品信息表（菲杰特_市级抽检）.xlsx.pkl.gz',
              '固原市经济开发区2020年食品安全监督抽检信息（县抽）.xls.pkl.gz',
              '固原市经济开发区2020年食品安全监督抽检信息（食用农产品——县抽）.xls.pkl.gz',
              '固原市经济开发区2020年食品安全监督抽检（县抽）.xls.pkl.gz',
              '固原市经济开发区2020年食品安全监督抽检（县抽）信息.xls.pkl.gz', '胡麻油专项监督抽检结果信息表.xlsx.pkl.gz']

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
              '公告网址链接', '产品具体名称', '销售单位/电商', '通告号', '通告日期', '号', '地址', '序']:
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


# Function to get the file path
def get_path(dir, fname):
    return '%s/%s' % (dir, fname.split('.pkl')[0])


# Function to print the dataframe
def print_df(dir, df, fname):
    # Print the file path
    s = 'open %s' % get_path(dir, fname)
    print(s)

    # If the dataframe has no columns or only has the inspection results column, quit
    if len(df.columns) < 1 or list(df.columns) == ['inspection_results']:
        quit()

    # Open the file
    os.system(s)

    # Print first few rows of the dataframe
    newline()
    print(df.head())
    hr()

    # Print last few rows of the dataframe
    if len(df) > 10:
        print(df.tail())

    elif len(df) > 5:
        print(df.tail(len(df) - 5))


# Function to read the raw Excel file
def read_excel(dir, fname, df):
    # Read the raw Excel file
    raw_df = pd.read_excel(get_path(dir, fname))
    # If the number of rows in the parsed dataframe is different from the number of rows in the raw dataframe
    diff = len(raw_df) - len(df)
    # Skip the first few rows of the raw dataframe
    raw_df = pd.read_excel(get_path(dir, fname), skiprows=diff)
    return raw_df


# Function to process the date column
def process_date(c):
    # Convert the date column to a string
    c = str(c)[:10].replace('//', '').replace('/', '-').replace('.', '-')
    # Format the date column
    try:
        return datetime.strptime(c.split()[0], '%Y-%m-%d').strftime('%Y-%m-%d')
    # If the date column is empty, return the original value
    except ValueError:
        return c


# Function to drop common columns
def drop_common(df, raw_df):
    # Production date columns
    dates = ['生产日期/批号', '生产日期']
    # Process the production date columns
    for col in dates:
        # Try to process the date column
        try:
            # Apply the process_date function to the date column
            raw_df[col] = raw_df[col].apply(process_date)
        except:
            pass
    drop_cols = []

    # Drop columns that are common to both the parsed and raw dataframes
    for col1 in df.columns:
        for col2 in raw_df.columns:
            # If the cells are the same, drop the column
            if (df[col1] == raw_df[col2]).all():
                # Add the column to the list of columns to be dropped
                drop_cols.append(col1)
                break

    # Drop the columns from the dataframe
    df = df.drop(drop_cols, axis=1)

    # If the number of rows in the parsed dataframe is the same as the number of rows in the raw dataframe
    if len(df) == len(raw_df):
        print('0/%s' % len(df))

    # If the number of rows in the parsed dataframe is different from the number of rows in the raw dataframe
    else:
        # Print the number of rows in the parsed and raw dataframes
        print('Rows in Parsed:', len(df))
        print('Rows in Raw:', len(raw_df))

    return df


# Define the directory where the parsed files are located
dir = ROOT + 'Ningxia_Ningxia_msb_20220814'

# Set pandas option to display all columns
pd.set_option('display.max_columns', None)

# Get the file name from the list of file names
fname = retrieve_fname_at(0)

# Read the dataframe from the pickle file
df = get_df(dir, fname)

# Get the column headers from the dataframe
col_headers = ['序', '号', '抽样编号', '样品名称', '生产企业', '名称', '生产企业地址', '被抽样单位', '名称',
               '被抽样单位', '地址', '规格', '生产日期', '抽样单位', '检测机构', '检测', '结果', '不合格项目', '备 注']

# Check substrings for unmatched columns
review_cols = substring(df, get_known_cols(), col_headers)

# Drop unnecessary columns from the dataframe
df = drop_columns(df, review_cols)

# Read the raw Excel file
raw_df = read_excel(dir, fname, df)

# Drop common columns from the dataframe
df = drop_common(df, raw_df)

# Print the dataframe
print_df(dir, df, fname)
