import re
# Import necessary libraries
import warnings
from datetime import datetime

from utils import *

# Ignore all warnings
warnings.filterwarnings("ignore")

# Define the root directory where the parsed files are located
ROOT_DIR = '/Users/narayansajeev/Desktop/MIT/parsed_files/'

# List of file names to be read in
FILE_NAMES = ['023101.xlsx.pkl.gz', '023352.xls.pkl.gz', '023402.xls.pkl.gz', '023405.xls.pkl.gz', '023407.xls.pkl.gz',
              '023409.xls.pkl.gz', '023411.xls.pkl.gz', '023413.xls.pkl.gz',
              '023428监督抽检（国抽）合格_不合格产品信息统计表.xls.pkl.gz',
              '1.西藏自治区２０２１年月饼抽检合格产品信息.xlsx.pkl.gz',
              '2021年产品质量自治区专项监督抽查检验结果送达公告名单.xls.pkl.gz',
              '2021年产品质量自治区监督抽查检验结果送达公告名单.xlsx.pkl.gz',
              '监督抽检（国抽）合格_不合格产品信息统计表.xls.pkl.gz',
              '监督抽检（省抽）合格_不合格产品信息统计表.xls.pkl.gz',
              '监督抽检（省检）合格_不合格产品信息表.xls.pkl.gz', '送达公告附件.xlsx.pkl.gz',
              '２.西藏自治区２０２１年月饼抽检不合格产品信息.xlsx.pkl.gz']

# Define the directory where the parsed files are located
DIR = ROOT_DIR + 'Tibet_Tibet_msb_20220317'

# Set pandas option to display all columns
pd.set_option('display.max_columns', None)

FILE_NAME = FILE_NAMES[0]


def process_date(date):
    """
    Function to process the date column.
    """
    # If the date column is empty, return None
    if date in ['/', '-']:
        return None

    # Convert the date column to a string and remove unnecessary parts
    date = str(date).split('：')[-1].strip()
    date = date.lstrip('/')
    date = date.replace('加工日期', '').replace('检疫日期', '').replace('购进日期', '')
    date = re.sub(r'[ /.-]', '', date[:10])
    date = date.replace('T', '').replace('J', '').replace('D', '')
    date = date.rstrip('-')
    date = date.split('～')[0]
    date = date.replace('年', '-').replace('月', '-')
    date = date.rstrip('-')
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
    """
    Function to drop common columns.
    """
    # Process the production date columns
    for col in ['生产日期/批号', '生产日期', '生产日期或批号', '生产(购进）日期/批号', '标称生产日期/批号',
                '生产日期（批号）', '生产日期/批号/购进日期']:
        # Try to process the date column
        try:
            # Apply the process_date function to the date column
            raw_df[col] = raw_df[col].apply(process_date)
        except:
            pass

    # Strip all whitespace characters from the parsed dataframe
    parsed_df = parsed_df.applymap(lambda x: x.strip() if isinstance(x, str) else x)
    # Replace all '，' with '_'
    parsed_df = parsed_df.applymap(lambda x: x.replace('，', '_') if isinstance(x, str) else x)
    # Replace all ',' with '_'
    parsed_df = parsed_df.applymap(lambda x: x.replace(',', '_') if isinstance(x, str) else x)
    # Replace all '\t' with ''
    parsed_df.replace('\t', '', regex=True, inplace=True)
    # Fill all NaN values with '/'
    parsed_df = parsed_df.fillna('/')
    # Replace all '/' with None
    parsed_df = parsed_df.applymap(lambda x: None if x == '/' else x)

    # Try to process the production date column
    try:
        # Apply the process_date function to the production date column
        parsed_df['production_date'] = parsed_df['production_date'].apply(process_date)
    except:
        pass

    # Strip all whitespace characters from the raw dataframe
    raw_df = raw_df.applymap(lambda x: x.strip() if isinstance(x, str) else x)
    # Replace all '，' with '_'
    raw_df = raw_df.applymap(lambda x: x.replace('，', '_') if isinstance(x, str) else x)
    # Replace all ',' with '_'
    raw_df = raw_df.applymap(lambda x: x.replace(',', '_') if isinstance(x, str) else x)
    # Replace all '\t' with ''
    raw_df.replace('\t', '', regex=True, inplace=True)
    # Fill all NaN values with '/'
    raw_df = raw_df.fillna('/')
    # Replace all '/' with None
    raw_df = raw_df.applymap(lambda x: None if x == '/' else x)
    # Replace all pd.NaT with None
    raw_df = raw_df.replace({pd.NaT: None})

    # Try to split the '不合格项目‖检验结果‖标准值' column into three columns
    try:
        raw_df[['adulterant', 'test_outcome', 'legal_limit']] = raw_df["不合格项目‖检验结果‖标准值"].str.split('‖',
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

    # If the number of rows in the raw dataframe is the same as the number of rows in the dataframe with all empty rows dropped
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

            # If the column headers are the same, print the column headers
            elif col1 == col2:
                debug(l1, l2)

    # Drop the columns from the dataframe
    parsed_df = parsed_df.drop(drop_cols, axis=1)

    # Replace all 'Non' with None
    parsed_df = parsed_df.applymap(lambda x: None if x == 'Non' else x)

    return raw_df, parsed_df


def print_df(parsed_df, raw_df):
    """
    Function to print the dataframe.
    """
    # Print the file path
    s = 'open %s' % get_path(DIR, FILE_NAME)
    txt = s.split('/')[-1]
    print(txt)

    # Drop columns that have all duplicate cells
    no_dup_df = parsed_df.loc[:, parsed_df.nunique() != 1]

    # Get the column headers
    cols = list(parsed_df.columns)

    # If the dataframe has no columns or only has the inspection results column or has duplicate values, quit
    if len(cols) < 1 or cols == ['inspection_results'] or len(no_dup_df.columns) < 1:
        quit()

    # Set the dataframe to be the dataframe with no duplicate columns
    parsed_df = no_dup_df

    # Open the file
    os.system(s)

    # Print first few rows of the dataframe
    print()
    print(parsed_df.head())
    hr()
    print(raw_df.head())

    # Print last few rows of the dataframe
    if len(parsed_df) > 10:
        hr()
        print(parsed_df.tail())
        hr()
        print(raw_df.tail())

    elif len(parsed_df) > 5:
        hr()
        print(parsed_df.tail(len(parsed_df) - 5))
        hr()
        print(raw_df.tail(len(parsed_df) - 5))


# Read the raw Excel file
raw_df = read_excel(DIR, FILE_NAME)

# Read the dataframe from the pickle file
parsed_df = get_df(DIR, FILE_NAME)

# Check substrings for unmatched columns
review_cols = substring(parsed_df, get_known_cols(), raw_df.columns)

# Drop unnecessary columns from the dataframe
parsed_df = drop_columns(parsed_df, review_cols)

# Drop common columns from the dataframe
raw_df, parsed_df = drop_common(parsed_df, raw_df)

# Print the dataframe
print_df(parsed_df, raw_df)
