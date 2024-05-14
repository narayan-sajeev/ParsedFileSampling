import json
import os
import re
from datetime import datetime
from random import shuffle

import pandas as pd

ROOT_DIR = '/Users/narayansajeev/Desktop/MIT/parsed_files/'
DIR = ''
FILE_NAME = ''
FILES_PER_PROV = 25


def get_files(PROV):
    global DIR
    DIR = ROOT_DIR + PROV

    # Retrieve all files in the directory
    files = os.listdir(DIR)
    # Unique files with corresponding pickle file
    files = [f for f in os.listdir(DIR) if files.count(f) == 1 and files.count(f + '.pkl.gz') == 1]
    # Valid extensions
    files = [f for f in files if any(e in f for e in ['xls', 'xlsx', 'doc', 'docx', 'html', 'pdf'])]
    # Remove files with 'http', '商', '饮', or '酒'
    files = [f for f in files if not any(c in f for c in ['http', '商', '饮', '酒', '╜'])]

    # Randomly select from valid files
    shuffle(files)
    files = sorted(files[:FILES_PER_PROV])

    if files:
        print('\n'.join(files))
        hr()
        print('\'%s\'' % '\', \''.join(files))

    quit()


def pkl_to_df():
    return pd.read_pickle('%s/%s' % (DIR, FILE_NAME))


def get_known_cols():
    known_cols_fn = '/Users/narayansajeev/Desktop/MIT/known_columns.json'
    with open(known_cols_fn) as f:
        return json.load(f)


def clean_cols(headers):
    return [n.replace('\n', '').replace('\r', '').replace('\xa0', '').replace(' ', '').strip('\u2003') for n in headers]


def substring(substr_sets, k):
    substr_dict = {}
    for s in substr_sets:
        substr_dict[s] = any([i in k for i in substr_sets[s]])
    return substr_dict


def check_substring(df, col_headers):
    known_cols = get_known_cols()
    col_headers = clean_cols(col_headers)
    col_headers = remove_useless_cols(col_headers)

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

    unmatched_cols = []

    review_cols = []

    # check for unmatched column headers
    for term in col_headers:
        if term not in known_cols:
            unmatched_cols.append(term)

    # check substrings for unmatched columns
    for k in unmatched_cols:
        substr_dict = substring(substr_sets, k)
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


def remove_cols(df, col_headers):
    keep_cols = ['manufacturer_name', 'manufacturer_address', 'sampled_location_name', 'sampled_location_address',
                 'food_name', 'specifications_model', 'announcement_date', 'production_date',
                 'product_classification', 'task_source_or_project_name', 'testing_agency', 'adulterant',
                 'inspection_results', 'failing_results', 'test_outcome', 'legal_limit']

    # select only the existing columns from the DataFrame
    df = df[[col for col in keep_cols if col in df.columns]]

    # Drop columns that have all NaN values
    empty_df = df.dropna(axis=1, how='all')

    removed_cols = []

    # Check for removed column headers
    for col in df.columns:
        if col not in empty_df.columns:
            removed_cols.append(col)

    col_headers = sorted(col_headers)

    if len(removed_cols) > 0 and len(col_headers) > 0:
        print('\'%s\'' % ('\', \''.join(col_headers)))
        print(removed_cols)

    return empty_df


def remove_useless_cols(col_headers):
    col_headers = list(col_headers)

    remove_cols = ['商标', '备注', '序号', '抽样编号', '购进日期', '被抽样单位省', '被抽样单位盟市',
                   '被抽样单位所在盟市', '公告网址链接', '产品具体名称', '销售单位/电商', '通告号', '通告日期', '号',
                   '地址', '序', '抽查领域', '统一社会信用代码', '产品细类', '企业所在市', '抽样单编号', '属地',
                   '任务类别', '地市', '检验报告编号', '抽查结果', '户外低帮休闲鞋', '采样时间', '计量单位', '样品数量',
                   '样品编号', '检验标准', '检验判定依据', '注册商标', '检验报告单编号', '型号', '等级',
                   '不合格样品数量/批次', '合格样品数量/批次', '样品合格率', '样品抽检数量/批次',
                   '不合格样品数量（批次）', '监督抽检样品总量（批次）', '住所', '有效期至', '检验方式', '注销原因',
                   '证书编号', '公告文号', '食品亚类（二级）', '食品品种（三级）', '食品细类（四级）', '不合格样品（批次）',
                   '合格样品（批次）', '监督抽检样品（批次）', '备案人', '备案人地址', '备案登记号',
                   '监督抽检样品总量/批次', '省级匹配任务增加检验项目', '风险等级']

    return [header for header in col_headers if header not in remove_cols]


def hr():
    print('*' * 100)


def get_path():
    return '%s/%s' % (DIR, FILE_NAME.split('.pkl')[0])


def read_excel():
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


def init(PROV, FILE_NAMES, NUM, col_headers):
    # Set pandas option to display all columns
    pd.set_option('display.max_columns', None)

    global DIR
    DIR = ROOT_DIR + PROV

    NUM -= 1

    # Add extension to the file name
    FILE_NAMES = [f + '.pkl.gz' for f in FILE_NAMES]

    global FILE_NAME
    FILE_NAME = FILE_NAMES[NUM]

    raw_df = read_excel()
    parsed_df = pkl_to_df()
    review_cols = check_substring(parsed_df, col_headers)
    parsed_df = remove_cols(parsed_df, review_cols)
    parsed_df = remove_whitespace(parsed_df)

    if df_exists(raw_df):
        raw_columns = remove_useless_cols(raw_df.columns)

        # Select only the columns that are in the raw DataFrame
        raw_df = raw_df.loc[:, raw_columns]

        raw_df = remove_whitespace(raw_df)

    return parsed_df, raw_df


def process_date_col(date):
    # If the date column is empty, return None
    if date in ['/', '-', '不详'] or date is None:
        return None

    # Convert the date column to a string and remove unnecessary parts
    date = str(date).split('：')[-1]
    date = date.lstrip('/')

    remove = ['D', 'J', 'T', '产', '加', '号', '品', '工', '批', '日', '期', '样', '检', '生', '疫', '购', '进',
              '00:00:00', '(', ')']

    # Remove unnecessary parts from the date column
    for r in remove:
        date = date.replace(r, '')

    date = date.strip().split(' ')[-1]

    date = date.replace('年', '-').replace('月', '-')
    date = re.sub(r'[ /.-]', '', date[:10]).rstrip('-').split('～')[0].split()[0]

    if date.count('-') == 1:
        return None

    if date.isdigit():
        try:
            return datetime.strptime(date, '%Y%m%d').strftime('%Y-%m-%d')
        except:
            return None

    date = '-'.join(date.split('-')[:3])

    date_formats = ['%Y-%m%d', '%m-%d-%Y', '%Y-%m-%d']
    for format in date_formats:
        try:
            return datetime.strptime(date, format).strftime('%Y-%m-%d')
        except ValueError:
            pass

    return date


def remove_common_cols(parsed_df, raw_df):
    date_cols = ['生产日期/批号', '生产日期', '生产日期或批号', '生产(购进）日期/批号', '标称生产日期/批号',
                 '生产日期（批号）', '生产日期/批号/购进日期', '生产/购进日期', '生产日期(批号)', '生产日期\n（购进日期）',
                 '生产/加工/检疫/购进日期']

    for col in date_cols:
        try:
            # Apply the process_date function to the date column
            raw_df[col] = raw_df[col].apply(process_date_col)
        except:
            pass

    if 'production_date' in parsed_df.columns:
        parsed_df['production_date'] = parsed_df['production_date'].apply(process_date_col)

    parsed_df = process_df(parsed_df)
    raw_df = process_df(raw_df)

    raw_df = raw_df.replace({pd.NaT: None})

    # Try to split the '不合格项目‖检验结果‖标准值' column into three columns
    try:
        raw_df[['adulterant', 'test_outcome', 'legal_limit']] = raw_df['不合格项目‖检验结果‖标准值'].str.split('‖',
                                                                                                               expand=True)
    except:
        pass

    # Find the number of unique rows to be the number of rows in the parsed DataFrame
    unique_rows = len(parsed_df)

    empty_rows = raw_df.dropna(how='all')

    # Find the number of unique rows to be the number of rows in the parsed DataFrame
    try:
        unique_rows = parsed_df['failing_results'].nunique()
    except:
        pass

    # If the number of unique rows in the parsed DataFrame is the same as the number of rows in the raw DataFrame
    if unique_rows == len(raw_df):
        print('1\t0/%s\t1' % len(parsed_df))

    # If the number of rows in the raw DataFrame is the same as the number of rows in the DataFrame with all empty rows removed
    elif len(empty_rows) == len(parsed_df):
        print('1\t0/%s\t1' % len(parsed_df))
        raw_df = empty_rows

    else:
        print('Parsed:', unique_rows)
        print('Raw:', len(raw_df))

    remove_cols = []

    # Remove columns that are common to both the parsed and raw DataFrames
    for col1 in parsed_df.columns:
        # Iterate through the columns in the raw DataFrame
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

            # If the cells are the same, remove the column
            if l1[:5] == l2[:5] and l1[-5:] == l2[-5:]:
                # Add the column to the list of columns to be removed
                remove_cols.append(col1)
                break

    # Remove the columns from the DataFrame
    parsed_df = parsed_df.drop(remove_cols, axis=1)

    # Replace all 'Non' with None
    parsed_df = parsed_df.applymap(lambda x: None if x == 'Non' else x)

    return parsed_df, raw_df


def remove_whitespace(df):
    df.columns = df.columns.str.strip()
    return df


def process_df(df):
    # Define the replacements
    replacements = {
        '，': '_',
        ',': '_',
        '\t': ''
    }

    # Apply the replacements
    for tup in replacements.items():
        df = df.applymap(lambda x: x.replace(tup[0], tup[1]) if isinstance(x, str) else x)

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


def head(df, rows_to_print=5):
    print()
    print(df.head(rows_to_print))
    hr()


def tail(df, rows_to_print=5):
    if len(df) > rows_to_print * 2:
        print(df.tail(rows_to_print))
        hr()

    elif len(df) > rows_to_print:
        print(df.tail(len(df) - rows_to_print))
        hr()


def edit_df(parsed_df, file_path):
    # Drop columns that have all duplicate cells
    no_dup_df = parsed_df.loc[:, parsed_df.nunique() != 1]

    # If the DataFrame has no columns or only has the inspection results column or has duplicate values, quit
    if not list(no_dup_df.columns) or list(parsed_df.columns) == ['inspection_results']:
        quit()

    # Set the DataFrame to be the DataFrame with no duplicate columns
    parsed_df = no_dup_df

    # Open the file
    os.system(file_path)

    return parsed_df


def df_exists(df):
    return isinstance(df, pd.DataFrame)


def results(parsed_df, raw_df):
    file_path = print_file_path()

    parsed_df = edit_df(parsed_df, file_path)

    if df_exists(raw_df):
        head(parsed_df)
        head(raw_df)

        tail(parsed_df)
        tail(raw_df)

    else:
        print('Parsed:', len(parsed_df))
        head(parsed_df, 10)
        tail(parsed_df, 10)
