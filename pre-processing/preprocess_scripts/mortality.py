import pandas as pd
import os 

# check for missing values
# TODO: add typing here for function calls
def get_metrics(mortality_df):
    print(mortality_df.isnull().sum())
    null_count = mortality_df.isnull().sum()
    contains_nulls = (null_count > 0).any()
    print("Checking for Nulls Contains Nulls ", contains_nulls)
    types_df = dict(mortality_df.dtypes)
    print(types_df)

# cast to int
# TODO: add typing here for function calls
def safe_convert_int(value):
    try:
        return int(value)  
    except (ValueError, TypeError):  
        return None  

# clean the dataset
def mortality_df_clean():
    # grab file path name
    script_dir = os.path.dirname(os.path.realpath(__file__))
    one_folder_up = os.path.dirname(script_dir)
    mortality_data_raw_path = os.path.join(one_folder_up, "preprocess_data_raw", "HDHI_Mortality_data.csv")

    mortality_df = pd.read_csv(mortality_data_raw_path)
    # strip white spaces in column names
    mortality_df.columns = mortality_df.columns.str.strip()
    mortality_df['DATE OF BROUGHT DEAD'] = pd.to_datetime(mortality_df['DATE OF BROUGHT DEAD'], errors='coerce', dayfirst=False)  # MM/DD/YYYY
    mortality_df = mortality_df.dropna(subset=['DATE OF BROUGHT DEAD'], how='any')
    #convert to appropriate types (int or string)
    mortality_df['MRD'] = mortality_df['MRD'].apply(safe_convert_int)
    mortality_df = mortality_df.dropna(subset=['MRD'])
    mortality_df['MRD'] = mortality_df['MRD'].astype('int64')
    mortality_df[['GENDER', 'RURAL/URBAN']] = mortality_df[['GENDER', 'RURAL/URBAN']].astype('string')
    
    return mortality_df

if __name__ == "__main__":
    mortality_df_clean()