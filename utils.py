# Function to get all file names from a directory
import json
import os
import random
import warnings

import pandas as pd

# Define the root directory where the parsed files are located
ROOT_DIR = '/Users/narayansajeev/Desktop/MIT/parsed_files/'

# Define the directory where the parsed files are located
DIR = ''

# Define the file name
FILE_NAME = ''

FILES_PER_PROV = 25


def get_all_fnames(PROV):
    global DIR
    DIR = ROOT_DIR + PROV

    current = []
    files = []
    # Loop through each file in the directory
    for file in os.listdir(DIR):
        # Check if the file isn't already in the list of files
        pkl = file + '.pkl.gz'
        # Check if the file is PDF or Excel
        is_xl_pdf = any([i in file for i in ['xls', 'xlsx', 'pdf']])
        # Check if the file is a food file
        is_food = not any([i in file for i in ['商', '饮', '酒']])
        if file not in current and pkl in os.listdir(DIR) and is_food and is_xl_pdf:
            files.append(file)

    # Shuffle the list of files
    random.shuffle(files)

    # Retrieve the list of PDF files
    pdf = sorted([file for file in files if 'pdf' in file])

    # Retrieve the list of Excel files
    xl = sorted([file for file in files if 'xls' in file])

    # Calculate the proportional number of PDF files
    num_pdf = int(FILES_PER_PROV * len(pdf) / len(files))

    # Calculate the proportional number of Excel files
    num_xl = FILES_PER_PROV - num_pdf

    # Combine the lists of PDF and Excel files
    files = pdf[:num_pdf] + xl[:num_xl]

    # Print the first 25 files in the list
    print('\n'.join(files))

    quit()


# Function to read a dataframe from a pickle file
def get_df():
    # read in the first file from the list using pandas
    return pd.read_pickle('%s/%s' % (DIR, FILE_NAME))


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
def substring(df, col_headers):
    known_cols = get_known_cols()

    # Clean up the column headers
    col_headers = clean(col_headers)

    # Remove unicode characters
    col_headers = [s.strip('\u2003') for s in col_headers]

    # Drop unnecessary columns from column headers
    col_headers = drop_useless_columns(col_headers)

    # dictionary of substrings to check for in column headers
    substr_sets = {
        'announcement_date': ['检', '抽', '报', '公布'],
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

    # Print unmatched and dropped column headers
    if len(dropped) > 0 and len(col_headers) > 0:
        print(col_headers)
        print(dropped)

    return drop_df


# Drop unnecessary columns from column headers
def drop_useless_columns(col_headers):
    # Convert to list
    col_headers = list(col_headers)

    # Remove unnecessary column headers
    for _ in ['商标', '备注', '序号', '抽样编号', '购进日期', '被抽样单位省', '被抽样单位盟市', '被抽样单位所在盟市',
              '公告网址链接', '产品具体名称', '销售单位/电商', '通告号', '通告日期', '号', '地址', '序', '抽查领域',
              '统一社会信用代码', '产品细类', '企业所在市', '抽样单编号', '属地', '任务类别', '地市', '检验报告编号',
              '抽查结果', '户外低帮休闲鞋', '采样时间', '计量单位', '样品数量', '样品编号', '检验标准']:
        try:
            while _ in col_headers:
                col_headers.remove(_)
        except:
            pass

    return col_headers


# Function to print a horizontal line
def hr():
    print('*' * 300)


# Function to get the file path
def get_path():
    return '%s/%s' % (DIR, FILE_NAME.split('.pkl')[0])


def debug(l1, l2):
    for i, _ in enumerate(l1):
        print(_, l2[i], _ == l2[i])
        print(type(_), type(l2[i]))


def read_excel():
    '''
    Function to read the raw Excel file.
    '''
    # Read the raw Excel file
    try:
        raw_df = pd.read_excel(get_path())
    except:
        return False
    # Get the index of the first occurrence
    try:
        idx = raw_df[(raw_df == '序号').any(axis=1)].index[0]
        # Set the column headers to be the entries in the row that contains '序号'
        raw_df.columns = raw_df.iloc[idx]
        # Remove the row that contains '序号'
        raw_df = raw_df.loc[idx + 1:]
    except:
        pass
    # Fill all NaN values with '/'
    raw_df = raw_df.fillna('/')
    return raw_df


def init(PROV, FILE_NAMES, NUM, col_headers=False):
    # Ignore all warnings
    warnings.filterwarnings('ignore')

    # Set pandas option to display all columns
    pd.set_option('display.max_columns', None)

    global DIR
    DIR = ROOT_DIR + PROV

    NUM -= 1

    global FILE_NAME
    FILE_NAME = FILE_NAMES[NUM]

    # Read the raw Excel file
    raw_df = read_excel()

    # Read the dataframe from the pickle file
    parsed_df = get_df()

    if not col_headers:
        # Get the column headers
        col_headers = raw_df.columns

    # Check substrings for unmatched columns
    review_cols = substring(parsed_df, col_headers)

    # Drop unnecessary columns from the dataframe
    parsed_df = drop_columns(parsed_df, review_cols)

    # Remove whitespace from the column headers
    parsed_df = remove_whitespace(parsed_df)

    # If the raw dataframe exists (if it's an Excel file)
    if isinstance(raw_df, pd.DataFrame):
        # Drop unnecessary columns from the dataframe
        raw_columns = drop_useless_columns(raw_df.columns)

        # Select only the columns that are in the raw dataframe
        raw_df = raw_df.loc[:, raw_columns]

        # Remove whitespace from the column headers
        raw_df = remove_whitespace(raw_df)

    return parsed_df, raw_df


def remove_whitespace(df):
    '''
    Function to remove whitespace from the column headers.
    '''
    df.columns = df.columns.str.strip()
    return df


def process_df(df):
    '''
    Helper function to process a dataframe.
    '''
    # Define the replacements
    replacements = {
        '，': '_',
        ',': '_',
        '\t': ''
    }

    # Apply the replacements
    for old, new in replacements.items():
        df = df.applymap(lambda x: x.replace(old, new) if isinstance(x, str) else x)

    # Fill all NaN values with '/'
    df = df.fillna('/')
    # Replace all '/' with None
    df = df.applymap(lambda x: None if x == '/' else x)

    return df


def print_file_path():
    path = 'open %s' % get_path()
    fname = path.split('/')[-1]
    print(fname)
    return path


def print_head(df):
    '''
    Function to print the first few rows of the dataframe.
    '''
    # Print first few rows of the dataframe
    print()
    print(df.head())
    hr()


def print_tail(df):
    '''
    Function to print the last few rows of the dataframe.
    '''

    # Print last few rows of the dataframe
    if len(df) > 10:
        print(df.tail())

    elif len(df) > 5:
        print(df.tail(len(df) - 5))

    hr()


def print_df(parsed_df, file_path):
    '''
    Function to print the dataframe.
    '''

    # Drop columns that have all duplicate cells
    no_dup_df = parsed_df.loc[:, parsed_df.nunique() != 1]

    # If the dataframe has no columns or only has the inspection results column or has duplicate values, quit
    if not list(no_dup_df.columns) or list(parsed_df.columns) == ['inspection_results']:
        quit()

    # Set the dataframe to be the dataframe with no duplicate columns
    parsed_df = no_dup_df

    # Open the file
    os.system(file_path)

    return parsed_df


def print_results(parsed_df, raw_df, raw=True):
    '''
    Function to print the results.
    '''

    # Print the file path
    file_path = print_file_path()

    # Print the dataframe
    parsed_df = print_df(parsed_df, file_path)

    if raw:
        # Print first few rows of the dataframes
        print_head(parsed_df)
        print_head(raw_df)

        # Print last few rows of the dataframes
        print_tail(parsed_df)
        print_tail(raw_df)

    else:
        # Print first & last few rows of the dataframe
        print_head(parsed_df)
        print_tail(parsed_df)
