# Import necessary libraries
import json
import os
import random
import warnings
from datetime import datetime
from utils import *

import pandas as pd

# Ignore all warnings
warnings.filterwarnings("ignore")

# Define the root directory where the parsed files are located
ROOT = '/Users/narayansajeev/Desktop/MIT/parsed_files/'


# Function to print the dataframe
def print_df(df, raw_df):
    # Print the file path
    s = 'open %s' % get_path(dir, fname)
    txt = s.split('/')[-1]
    print(txt)

    # Drop columns that have all duplicate cells
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
    try:
        idx = raw_df[(raw_df == '序号').any(axis=1)].index[0]
        # Set the column headers to be the entries in the row that contains '序号'
        raw_df.columns = raw_df.iloc[idx]
        raw_df = raw_df.loc[idx + 1:]
    except:
        pass
    raw_df = raw_df.fillna('/')
    return raw_df


# Function to process the date column
def process_date(date):
    # If the date column is empty, return None
    if date in ['/', '-']:
        return None
    # Convert the date column to a string
    date = str(date).split('：')[-1].strip()
    if date.startswith('/'):
        date = date[1:]
    date = date.replace('加工日期', '').replace('检疫日期', '').replace('购进日期', '')
    date = date[:10].replace(' ', '').replace('//', '').replace('/', '-').replace('.', '-')
    date = date.replace('T', '').replace('J', '').replace('D', '')
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
    if date.count('-') == 1:
        return None
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
        return datetime.strptime(date, '%Y-%m%d').strftime('%Y-%m-%d')
    except:
        pass
    try:
        return datetime.strptime(date, '%m-%d-%Y').strftime('%Y-%m-%d')
    except:
        pass
    try:
        return datetime.strptime(date, '%Y-%m-%d').strftime('%Y-%m-%d')
    # Return the original value
    except ValueError:
        return date


# Function to drop common columns
def drop_common(df, raw_df):
    # Process the production date columns
    for col in ['生产日期/批号', '生产日期', '生产日期或批号', '生产(购进）日期/批号', '标称生产日期/批号',
                '生产日期（批号）', '生产日期/批号/购进日期']:
        # Try to process the date column
        try:
            # Apply the process_date function to the date column
            raw_df[col] = raw_df[col].apply(process_date)
        except:
            pass

    df = df.applymap(lambda x: x.strip() if isinstance(x, str) else x)
    df = df.applymap(lambda x: x.replace('，', '_') if isinstance(x, str) else x)
    df = df.applymap(lambda x: x.replace(',', '_') if isinstance(x, str) else x)
    df.replace('\t', '', regex=True, inplace=True)
    df = df.fillna('/')
    df = df.applymap(lambda x: None if x == '/' else x)

    try:
        df['production_date'] = df['production_date'].apply(process_date)
    except:
        pass

    raw_df = raw_df.applymap(lambda x: x.strip() if isinstance(x, str) else x)
    raw_df = raw_df.applymap(lambda x: x.replace('，', '_') if isinstance(x, str) else x)
    raw_df = raw_df.applymap(lambda x: x.replace(',', '_') if isinstance(x, str) else x)
    raw_df.replace('\t', '', regex=True, inplace=True)
    raw_df = raw_df.fillna('/')
    raw_df = raw_df.applymap(lambda x: None if x == '/' else x)
    raw_df = raw_df.replace({pd.NaT: None})

    try:
        raw_df[['adulterant', 'test_outcome', 'legal_limit']] = raw_df["不合格项目‖检验结果‖标准值"].str.split('‖',
                                                                                                               expand=True)
    except:
        pass

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

            elif col1 == col2:
                debug(l1, l2)

    # Drop the columns from the dataframe
    df = df.drop(drop_cols, axis=1)

    df = df.applymap(lambda x: None if x == 'Non' else x)

    return raw_df, df


# list of file names to be read in
fnames = ['023101.xlsx.pkl.gz', '023352.xls.pkl.gz', '023402.xls.pkl.gz', '023405.xls.pkl.gz', '023407.xls.pkl.gz',
          '023409.xls.pkl.gz', '023411.xls.pkl.gz', '023413.xls.pkl.gz',
          '023428监督抽检（国抽）合格_不合格产品信息统计表.xls.pkl.gz',
          '1.西藏自治区２０２１年月饼抽检合格产品信息.xlsx.pkl.gz',
          '2021年产品质量自治区专项监督抽查检验结果送达公告名单.xls.pkl.gz',
          '2021年产品质量自治区监督抽查检验结果送达公告名单.xlsx.pkl.gz',
          '监督抽检（国抽）合格_不合格产品信息统计表.xls.pkl.gz', '监督抽检（省抽）合格_不合格产品信息统计表.xls.pkl.gz',
          '监督抽检（省检）合格_不合格产品信息表.xls.pkl.gz', '送达公告附件.xlsx.pkl.gz',
          '２.西藏自治区２０２１年月饼抽检不合格产品信息.xlsx.pkl.gz']

# Define the directory where the parsed files are located
dir = ROOT + 'Tibet_Tibet_msb_20220317'

# Set pandas option to display all columns
pd.set_option('display.max_columns', None)

fname = fnames[0]

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
