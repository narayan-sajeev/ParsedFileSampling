import pandas as pd


def filter_df(parsed_df):
    lst = []
    # Iterate through each row in the DataFrame
    for index, row in parsed_df.iterrows():
        # Check if the row is valid
        if check_row(row):
            # Append the row to the list
            lst.append(row)

    # Create a new DataFrame from the list
    filtered = pd.DataFrame(lst)

    # Return the new DataFrame
    return filtered


def has_column(row, c):
    # Check if the column exists in the row
    if c in row:
        # Check if the value in the column is not NaN
        try:
            if pd.isna(row[c]):
                return False
            else:
                return True
        except TypeError:
            return True
    return False


def check_row(row):
    # Define the vital columns
    vital_a = ['food_name', 'product_classification']
    vital_b = ['sampled_location_name', 'sampled_location_address', 'sampled_location_province']
    vital_c = ['manufacturer_name', 'manufacturer_address']

    # Check if the row contains any of the vital columns
    if any(has_column(row, c) for c in vital_a):
        return True
    if any(has_column(row, c) for c in vital_b) and any(has_column(row, c) for c in vital_c):
        return True
    return False