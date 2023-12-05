# Import necessary libraries
import re
from datetime import datetime

from utils import *

# List of file names to be read in
FILE_NAMES = ['02-0合格名单（畜牧养殖环节）.xls.pkl.gz', '2020食品安全监督抽检合格情况公示第10期.xlsx.pkl.gz',
              '2020食品安全监督抽检合格情况公示第5期.xlsx.pkl.gz',
              '常德市食品安全监督抽检合格产品名单（2020年第1期）.xlsx.pkl.gz',
              '食品安全监督抽检合格产品名单（2019年第17期）.xlsx.pkl.gz',
              '食品安全监督抽检合格产品名单（2019年第26期）.xlsx.pkl.gz',
              '食品安全监督抽检合格产品名单（2019年第32期）.xlsx.pkl.gz',
              '食品安全监督抽检合格产品名单（2019年第35期）.xlsx.pkl.gz',
              '食品安全监督抽检合格产品名单（2019年第37期）.xlsx.pkl.gz',
              '食品安全监督抽检合格产品名单（2019年第39期）.xlsx.pkl.gz',
              '食品安全监督抽检合格产品名单（2020年第2期）.xlsx.pkl.gz',
              '食品安全监督抽检合格产品名单（2020年第3期）.xlsx.pkl.gz']

# Province name
PROV = 'Changde_Hunan_msb'


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


# Set current file number
NUM = 2

# Initialize the parsed and raw dataframes
parsed_df, raw_df = init(PROV, FILE_NAMES, NUM)

# Drop common columns from the dataframe
parsed_df, raw_df = drop_common(parsed_df, raw_df)

# Print the results
print_results(parsed_df, raw_df)
