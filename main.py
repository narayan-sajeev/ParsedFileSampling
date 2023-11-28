# Import necessary libraries
import re
from datetime import datetime

from utils import *

# List of file names to be read in
FILE_NAMES = ['041524附件21_淀粉及淀粉制品监督抽检产品合格信息.xlsx.pkl.gz',
              '041951附件6_u2002食用油、油脂及其制品监督抽检产品合格信息.xlsx.pkl.gz',
              '0423333.___食用农产品监督抽检不合格产品信息.xlsx.pkl.gz',
              '0427428.___乳制品监督抽检产品合格信息.xlsx.pkl.gz', '11._糖果制品监督抽检产品合格信息.xlsx.pkl.gz',
              '12.__肉制品监督抽检产品合格信息.xlsx.pkl.gz', '23.__炒货食品及坚果制品监督抽检产品合格信息.xlsx.pkl.gz',
              '3.__食用油、油脂及其制品监督抽检产品合格信息.xlsx.pkl.gz',
              '6.______粮食加工品监督抽检产品合格信息.xlsx.pkl.gz', '6.___粮食加工品监督抽检产品合格信息.xlsx.pkl.gz',
              '7.__________食用农产品评价性抽检产品合格信息.xlsx.pkl.gz',
              '附件10_u2002食用油、油脂及其制品监督抽检产品合格信息.xlsx.pkl.gz',
              '附件10_速冻食品监督抽检产品合格信息.xlsx.pkl.gz', '附件10_食用农产品监督抽检不合格产品信息.xlsx.pkl.gz',
              '附件12_速冻食品监督抽检产品合格信息.xlsx.pkl.gz',
              '附件12炒货食品及坚果制品监督抽检产品合格信息.xlsx.pkl.gz',
              '附件13_食用农产品监督抽检不合格产品信息.xlsx.pkl.gz',
              '附件17_u2002淀粉及淀粉制品监督抽检产品合格信息.xlsx.pkl.gz',
              '附件18_乳制品监督抽检产品合格信息.xlsx.pkl.gz',
              '附件23_可可及焙烤咖啡产品监督抽检产品合格信息.xlsx.pkl.gz',
              '附件24_食用农产品监督抽检产品合格信息.xlsx.pkl.gz', '附件2_食品农产品监督抽检产品合格信息.xlsx.pkl.gz',
              '附件4_糖果制品监督抽检产品合格信息.xlsx.pkl.gz', '附件5_乳制品监督抽检产品合格信息.xlsx.pkl.gz',
              '附件5_豆制品监督抽检产品合格信息.xlsx.pkl.gz']

# Province name
PROV = 'Xinjiang_Xinjiang_msb_20220630'


def process_date(date):
    """
    Function to process the date column.
    """
    # If the date column is empty, return None
    if date in ['/', '-', '不详'] or date is None:
        return None

    # Convert the date column to a string and remove unnecessary parts
    date = str(date).split('：')[-1].strip()
    date = date.lstrip('/')
    date = date.replace('加工日期', '').replace('检疫日期', '').replace('购进日期', '')
    date = date.replace('日', '')
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
    date_cols = ['生产日期/批号', '生产日期', '生产日期或批号', '生产(购进）日期/批号', '标称生产日期/批号',
                 '生产日期（批号）', '生产日期/批号/购进日期', '生产/购进日期']

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
NUM = 1

# Initialize the parsed and raw dataframes
parsed_df, raw_df = init(PROV, FILE_NAMES, NUM)

# Drop common columns from the dataframe
parsed_df, raw_df = drop_common(parsed_df, raw_df)

# Print the results
print_results(parsed_df, raw_df)
