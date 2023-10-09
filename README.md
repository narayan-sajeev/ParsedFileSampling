# Python Script for Data Processing

This script is designed to process data from Excel and pickle files. It includes functions for reading data, cleaning column headers, checking substrings, and printing the dataframe.

## Libraries Used
- `json`: Used for handling JSON files.
- `os`: Provides functions for interacting with the operating system.
- `random`: Used for generating random numbers.
- `pandas`: Used for data manipulation and analysis.

## Directory Structure
The root directory where the parsed files are located is defined as `ROOT`.

## Functions
The script contains several functions:

- `get_fnames(DIR)`: Gets file names from a directory.
- `print_fname(i)`: Prints a specific file name from a list of file names.
- `get_df(DIR, fname)`: Reads a dataframe from a pickle file.
- `get_known_cols()`: Gets known column classifiers from a JSON file.
- `clean(col_headers)`: Cleans column headers by removing newline, carriage return, non-breaking space and space characters.
- `substr_check(substr_sets, k)`: Checks substrings in column headers.
- `substring(df, known_cols, col_headers)`: Checks substrings for unmatched columns.
- `drop_columns(df, col_headers)`: Drops columns from the dataframe.
- `hr()`: Prints a horizontal line.
- `newline()`: Prints a newline.
- `print_df(DIR, df, fname)`: Prints the dataframe.

## Usage
To use this script, you need to define the directory where the parsed files are located. Then you can use the functions to process your data.

Please note that this script is specifically designed for certain data processing tasks and may need to be modified to suit your specific needs. 

## Dependencies
This script requires Python 3.x and the following Python libraries installed:
- pandas
- os
- json
- random

You can install these libraries using pip:

`pip install pandas os json random`


## Author
This script was written by Narayan Sajeev. For any queries or further clarifications regarding the script, please reach out to Narayan directly at [nsajeev@mit.edu](mailto:nsajeev@mit.edu). 

Please note that this README is intended as a basic overview of the script and its functionality. For detailed information on the code and its application, please refer to comments within the script itself. 

Happy coding! 🚀`