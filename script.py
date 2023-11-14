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

    try:
        col_headers.remove('抽查结果')
    except:
        pass

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

    # Remove unnecessary column headers
    for _ in ['商标', '备注', '序号', '抽样编号', '购进日期', '被抽样单位省', '被抽样单位盟市', '被抽样单位所在盟市',
              '公告网址链接', '产品具体名称', '销售单位/电商', '通告号', '通告日期', '号', '地址', '序', '抽查领域',
              '统一社会信用代码', '产品细类', '企业所在市']:
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


# Function to get the file path
def get_path(dir, fname):
    return '%s/%s' % (dir, fname.split('.pkl')[0])


# Function to print the dataframe
def print_df(df, raw_df):
    # Print the file path
    s = 'open %s' % get_path(dir, fname)
    txt = s.split('/')[-1]
    print(txt)

    # Drop columns that have all duplicate rows
    no_dup_df = df.loc[:, df.nunique() != 1]

    cols = list(df.columns)

    # If the dataframe has no columns or only has the inspection results column or has duplicate values, quit
    if len(cols) < 1 or cols == ['inspection_results'] or len(no_dup_df.columns) < 1:
        quit()

    df = no_dup_df

    # Open the file
    os.system(s)

    # Print first few rows of the dataframe
    print()
    print(df.head())
    hr()
    print(raw_df.head())

    # Print last few rows of the dataframe
    if len(df) > 10:
        hr()
        print(df.tail())
        hr()
        print(raw_df.tail())

    elif len(df) > 5:
        hr()
        print(df.tail(len(df) - 5))
        hr()
        print(raw_df.tail(len(df) - 5))


# Function to read the raw Excel file
def read_excel(dir, fname):
    # Read the raw Excel file
    raw_df = pd.read_excel(get_path(dir, fname))
    # Get the index of the first occurrence
    idx = raw_df[(raw_df == '序号').any(axis=1)].index[0]
    # Set the column headers to be the entries in the row that contains '序号'
    raw_df.columns = raw_df.iloc[idx]
    raw_df = raw_df.loc[idx + 1:]
    raw_df = raw_df.fillna('/')
    return raw_df


# Function to process the date column
def process_date(date):
    # If the date column is empty, return None
    if date in ['/', '-']:
        return None
    # Convert the date column to a string
    date = str(date).split('：')[-1]
    date = date.replace('加工日期', '').replace('检疫日期', '').replace('购进日期', '')
    date = date[:10].replace('//', '').replace('/', '-').replace('.', '-')
    # If the last character is a '-', remove it
    if date[-1] == '-':
        date = date[:-1]
    # If the last character is a '-', remove it
    if '～' in date:
        date = date.split('～')[0]
    # Replace '年' and '月' with '-'
    date = date.replace('年', '-').replace('月', '-')
    # Remove the last character if it's not a digit
    if not date[-1].isdigit():
        date = date[:-1]
    date = date.split()[0]
    # If the date is an integer
    if date.isdigit():
        try:
            # Convert the date to a datetime object
            return datetime.strptime(date, '%Y%m%d').strftime('%Y-%m-%d')
        except:
            return None
    # Format the date column
    date = '-'.join(date.split('-')[:3])
    try:
        return datetime.strptime(date, '%Y-%m-%d').strftime('%Y-%m-%d')
    # Return the original value
    except ValueError:
        return date


def debug(l1, l2):
    for i, _ in enumerate(l1):
        print(_, l2[i], _ == l2[i])


# Function to drop common columns
def drop_common(df, raw_df):
    # Process the production date columns
    for col in ['生产日期/批号', '生产日期', '生产日期或批号', '生产(购进）日期/批号', '标称生产日期/批号']:
        # Try to process the date column
        try:
            # Apply the process_date function to the date column
            raw_df[col] = raw_df[col].apply(process_date)
        except:
            pass

    df = df.applymap(lambda x: x.strip() if isinstance(x, str) else x)
    df.replace('\t', '', regex=True, inplace=True)
    df = df.fillna('/')
    df = df.applymap(lambda x: None if x == '/' else x)
    df['production_date'] = df['production_date'].apply(process_date)

    raw_df = raw_df.applymap(lambda x: x.strip() if isinstance(x, str) else x)
    raw_df.replace('\t', '', regex=True, inplace=True)
    raw_df = raw_df.fillna('/')
    raw_df = raw_df.applymap(lambda x: None if x == '/' else x)
    raw_df = raw_df.replace({pd.NaT: None})

    unique_rows = len(df)

    drop_empty = raw_df.dropna(how='all')

    try:
        unique_rows = df['failing_results'].nunique()
    except:
        pass

    # If the number of unique rows in the parsed dataframe is the same as the number of rows in the raw dataframe
    if unique_rows == len(raw_df):
        print('1\t0/%s\t1' % len(df))

    elif len(drop_empty) == len(df):
        print('1\t0/%s\t1' % len(df))
        raw_df = drop_empty

    else:
        # Print the number of rows in the parsed and raw dataframes
        print('Raw:', len(raw_df))
        print('Parsed:', unique_rows)

    drop_cols = []

    # Drop columns that are common to both the parsed and raw dataframes
    for col1 in df.columns:
        for col2 in raw_df.columns:
            l1 = list(df[col1])
            l2 = list(raw_df[col2])

            while '注：排名不分先后' in l1:
                df = df.iloc[:-1]
                l1 = list(df[col1])

            while '注：排名不分先后' in l2:
                raw_df = raw_df.iloc[:-1]
                l2 = list(raw_df[col2])

            # If the cells are the same, drop the column
            if l1[:5] == l2[:5] and l1[-5:] == l2[-5:]:
                # Add the column to the list of columns to be dropped
                drop_cols.append(col1)
                break

    # Drop the columns from the dataframe
    df = df.drop(drop_cols, axis=1)

    df = df.applymap(lambda x: None if x == 'Non' else x)

    return raw_df, df


# list of file names to be read in
fnames = ['201914.xls.pkl.gz', '附件2-合格-2019年11号食品抽检信息.xlsx.pkl.gz',
          '附件2-合格-2019年25号信息公布.xlsx.pkl.gz', '附件2-合格-2019年28号食品抽检信息公布.xlsx.pkl.gz',
          '附件2-合格-2019年31号信息公布.xlsx.pkl.gz', '附件2-合格-2019年35号信息公布.xlsx.pkl.gz',
          '附件2-合格-2019年38号信息公布.xlsx.pkl.gz', '附件2-合格-2019年41号信息公布.xlsx.pkl.gz',
          '附件2-合格-2019年44号信息公布1.xlsx.pkl.gz', '附件2-合格-2019年47号信息公布.xlsx.pkl.gz',
          '附件2-合格-2019年50号信息公布.xlsx.pkl.gz', '附件2-合格-2019年56号信息公布.xlsx.pkl.gz',
          '附件2-合格-2019年9号信息公布.xlsx.pkl.gz', '附件2-合格-2020年8号信息公布.xlsx.pkl.gz',
          '附件3-不合格-2019年11号食品抽检信息.xlsx.pkl.gz', '附件3-不合格-2019年38号信息公布.xlsx.pkl.gz',
          '附件3-不合格-2019年46号信息公布.xlsx.pkl.gz', '附件3-不合格-2019年47号信息公布.xlsx.pkl.gz',
          '附件3-不合格-2019年53号信息公布.xlsx.pkl.gz', '附件3-不合格-2019年6号食品抽检信息.xlsx.pkl.gz',
          '附件3-不合格-2019年8号信息公布.xlsx.pkl.gz', '附件3-不合格-2020年12号信息公布.xlsx.pkl.gz',
          '附件3-不合格-2020年15号信息公布.xlsx.pkl.gz', '附件3-不合格-2020年4号信息公布.xlsx.pkl.gz',
          '附件3-不合格-2020年8号信息公布.xlsx.pkl.gz']

# Define the directory where the parsed files are located
dir = ROOT + 'Sichuan_Sichuan_msb_20220814'

# Set pandas option to display all columns
pd.set_option('display.max_columns', None)

fname = fnames[16]

# Read the raw Excel file
raw_df = read_excel(dir, fname)

# Read the dataframe from the pickle file
df = get_df(dir, fname)

# Check substrings for unmatched columns
review_cols = substring(df, get_known_cols(), raw_df.columns)

# Drop unnecessary columns from the dataframe
df = drop_columns(df, review_cols)

# Drop common columns from the dataframe
raw_df, df = drop_common(df, raw_df)

# Print the dataframe
print_df(df, raw_df)
