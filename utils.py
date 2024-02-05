# Function to get all file names from a directory
import json
import os
import re
import warnings
from datetime import datetime
from random import shuffle

import pandas as pd
from tqdm import tqdm

# Define the root directory where the parsed files are located
ROOT_DIR = '/Users/narayansajeev/Desktop/MIT/parsed_files/'

# Define the directory where the parsed files are located
DIR = ''

# Define the file name
FILE_NAME = ''

FILES_PER_PROV = 10


def get_files(PROV):
    global DIR
    DIR = ROOT_DIR + PROV

    current = []
    files = []
    shuffled = os.listdir(DIR)
    shuffle(shuffled)
    # Loop through each file in the directory
    for file in tqdm(shuffled):
        # Check if the file isn't already in the list of files
        pkl = file + '.pkl.gz'
        # Check if the file is valid
        # is_valid = any([i in file for i in ['xls', 'xlsx', 'doc', 'docx', 'html', 'pdf']])
        is_valid = any([i in file for i in ['doc', 'docx', 'html', 'pdf']])
        # Check if the file is a food file
        is_food = not any([i in file for i in ['商', '饮', '酒']])
        if file not in current and pkl in os.listdir(DIR) and is_food and is_valid and 'http' not in file:
            files.append(file)

        # Quit if the number of files is greater than or equal to the number of files per province
        if len(files) >= FILES_PER_PROV:
            break

    # Retrieve first few files
    files = sorted(files[:FILES_PER_PROV])

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
        substr_dict[s] = any([i in k for i in substr_sets[s]])
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
        print('\'%s\'' % ('\', \''.join(col_headers)))
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
              '抽查结果', '户外低帮休闲鞋', '采样时间', '计量单位', '样品数量', '样品编号', '检验标准', '检验判定依据',
              '注册商标', '检验报告单编号', '型号', '等级', '不合格样品数量/批次', '合格样品数量/批次', '样品合格率',
              '样品抽检数量/批次', '不合格样品数量（批次）', '监督抽检样品总量（批次）', '住所', '有效期至', '检验方式',
              '注销原因', '证书编号', '公告文号', '食品亚类（二级）', '食品品种（三级）', '食品细类（四级）']:
        try:
            while _ in col_headers:
                col_headers.remove(_)
        except:
            pass

    return col_headers


# Function to print a horizontal line
def hr():
    print('*' * 100)


# Function to get the file path
def get_path():
    return '%s/%s' % (DIR, FILE_NAME.split('.pkl')[0])


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
    if is_df(raw_df):
        # Drop unnecessary columns from the dataframe
        raw_columns = drop_useless_columns(raw_df.columns)

        # Select only the columns that are in the raw dataframe
        raw_df = raw_df.loc[:, raw_columns]

        # Remove whitespace from the column headers
        raw_df = remove_whitespace(raw_df)

    return parsed_df, raw_df


def process_date(date):
    '''
    Function to process the date column.
    '''
    # If the date column is empty, return None
    if date in ['/', '-', '不详'] or date is None:
        return None

    # Convert the date column to a string and remove unnecessary parts
    date = str(date).split('：')[-1].strip()
    date = date.lstrip('/')

    date = date.split(' ')[-1]

    rem = ['D', 'J', 'T', '产', '加', '号', '品', '工', '批', '日', '期', '样', '检', '生', '疫', '购', '进']

    # Remove unnecessary parts from the date column
    for r in rem:
        date = date.replace(r, '')

    date = date.replace('年', '-').replace('月', '-')
    date = re.sub(r'[ /.-]', '', date[:10])
    date = date.rstrip('-')
    date = date.split('～')[0]
    date = date.split()[0]

    # If the date column has only one '-', return None
    if date.count('-') == 1:
        return None

    # If the date is an integer, try to convert it to a datetime object
    if date.isdigit():
        try:
            return datetime.strptime(date, '%Y%m%d').strftime('%Y-%m-%d')
        except:
            return None

    # Format the date column
    date = '-'.join(date.split('-')[:3])

    # Try to convert the date to a datetime object in different formats
    date_formats = ['%Y-%m%d', '%m-%d-%Y', '%Y-%m-%d']
    for fmt in date_formats:
        try:
            return datetime.strptime(date, fmt).strftime('%Y-%m-%d')
        except ValueError:
            pass

    # Return the original value
    return date


def drop_common(parsed_df, raw_df):
    '''
    Function to drop common columns.
    '''
    # Process the production date columns
    date_cols = ['生产日期/批号', '生产日期', '生产日期或批号', '生产(购进）日期/批号', '标称生产日期/批号',
                 '生产日期（批号）', '生产日期/批号/购进日期', '生产/购进日期', '生产日期(批号)', '生产日期\n（购进日期）']

    for col in date_cols:
        # Try to process the date column
        try:
            # Apply the process_date function to the date column
            raw_df[col] = raw_df[col].apply(process_date)
        except:
            pass

    prod_date = 'production_date'

    if prod_date in parsed_df.columns:
        # Process the production date columns
        parsed_df[prod_date] = parsed_df[prod_date].apply(process_date)

    # Process the dataframes
    parsed_df = process_df(parsed_df)
    raw_df = process_df(raw_df)

    # Replace all pd.NaT with None
    raw_df = raw_df.replace({pd.NaT: None})

    # Try to split the '不合格项目‖检验结果‖标准值' column into three columns
    try:
        raw_df[['adulterant', 'test_outcome', 'legal_limit']] = raw_df['不合格项目‖检验结果‖标准值'].str.split('‖',
                                                                                                               expand=True)
    except:
        pass

    # Find the number of unique rows to be the number of rows in the parsed dataframe
    unique_rows = len(parsed_df)

    # Drop all rows that have all NaN values
    drop_empty = raw_df.dropna(how='all')

    # Find the number of unique rows to be the number of rows in the parsed dataframe
    try:
        unique_rows = parsed_df['failing_results'].nunique()
    except:
        pass

    # If the number of unique rows in the parsed dataframe is the same as the number of rows in the raw dataframe
    if unique_rows == len(raw_df):
        print('1\t0/%s\t1' % len(parsed_df))

    # If the number of rows in the raw dataframe is the same as the number of rows in the dataframe with all empty
    # rows dropped
    elif len(drop_empty) == len(parsed_df):
        print('1\t0/%s\t1' % len(parsed_df))
        raw_df = drop_empty

    else:
        # Print the number of rows in the parsed and raw dataframes
        print('Raw:', len(raw_df))
        print('Parsed:', unique_rows)

    drop_cols = []

    # Drop columns that are common to both the parsed and raw dataframes
    for col1 in parsed_df.columns:
        # Iterate through the columns in the raw dataframe
        for col2 in raw_df.columns:
            # Get the lists of cells in the columns
            l1 = list(parsed_df[col1])
            l2 = list(raw_df[col2])

            # Remove the last row if it contains '注：排名不分先后'
            while '注：排名不分先后' in l1:
                parsed_df = parsed_df.iloc[:-1]
                l1 = list(parsed_df[col1])

            # Remove the last row if it contains '注：排名不分先后'
            while '注：排名不分先后' in l2:
                raw_df = raw_df.iloc[:-1]
                l2 = list(raw_df[col2])

            # If the cells are the same, drop the column
            if l1[:5] == l2[:5] and l1[-5:] == l2[-5:]:
                # Add the column to the list of columns to be dropped
                drop_cols.append(col1)
                break

    # Drop the columns from the dataframe
    parsed_df = parsed_df.drop(drop_cols, axis=1)

    # Replace all 'Non' with None
    parsed_df = parsed_df.applymap(lambda x: None if x == 'Non' else x)

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
    path = 'open \'%s\'' % get_path()
    fname = path.split('/')[-1][:-1]
    print(fname)
    return path


def print_head(df, ten=False):
    '''
    Function to print the first few rows of the dataframe.
    '''
    num = 10 if ten else 5
    # Print first few rows of the dataframe
    print()
    print(df.head(num))
    hr()


def print_tail(df, ten=False):
    '''
    Function to print the last few rows of the dataframe.
    '''

    # Set the number of rows to be printed
    num = 10 if ten else 5

    # Print last few rows of the dataframe
    if len(df) > num * 2:
        print(df.tail(num))
        hr()

    elif len(df) > num:
        print(df.tail(len(df) - num))
        hr()


def edit_df(parsed_df, file_path):
    '''
    Function to edit the dataframe.
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


def is_df(df):
    '''
    Function to check if the input is a dataframe.
    '''
    return isinstance(df, pd.DataFrame)


def print_results(parsed_df, raw_df):
    '''
    Function to print the results.
    '''

    # Print the file path
    file_path = print_file_path()

    # Edit the dataframe
    parsed_df = edit_df(parsed_df, file_path)

    # If the raw dataframe exists (if it's an Excel file)
    if is_df(raw_df):
        # Print first few rows of the dataframes
        print_head(parsed_df)
        print_head(raw_df)

        # Print last few rows of the dataframes
        print_tail(parsed_df)
        print_tail(raw_df)

    # If the raw dataframe doesn't exist (if it's a PDF file)
    else:
        # Print first & last few rows of the dataframe
        print('Parsed:', len(parsed_df))
        print_head(parsed_df, True)
        print_tail(parsed_df, True)
