import pandas as pd
import os

# converts to the correct datetime format
# TODO: add typing here for function calls
def select_correct_date(row):
    valid_1 = not pd.isna(row['D.O.A_1']) and not pd.isna(row['D.O.D_1']) and row['D.O.D_1'] >= row['D.O.A_1']
    valid_2 = not pd.isna(row['D.O.A_2']) and not pd.isna(row['D.O.D_2']) and row['D.O.D_2'] >= row['D.O.A_2']    
    if valid_1:
        return row['D.O.A_1'], row['D.O.D_1']
    elif valid_2:
        return row['D.O.A_2'], row['D.O.D_2']
    else:
        return pd.NaT, pd.NaT  # If both are invalid

# gets the type of each object and stores them in 2 lists 
def get_types(obj):
    obj_list = list()
    int_list = list()
    for key in obj.keys():
        # print(key)
        if obj[key] == object:
            obj_list.append(key)
        elif obj[key] == int:
            int_list.append(key)
    return obj_list, int_list

# casts to int
# TODO: add typing here for function calls
def safe_convert_int(value):
    try:
        return int(value)  
    except (ValueError, TypeError):  
        return None  

# cleans the dataset    
def admission_df_clean():
    # find the current path
    script_dir = os.path.dirname(os.path.realpath(__file__))
    one_folder_up = os.path.dirname(script_dir)
    admissions_data_raw = os.path.join(one_folder_up, "preprocess_data_raw", "HDHI_Admission_data.csv")
    
    admissions_df = pd.read_csv(admissions_data_raw)

    # drop null DOA and DOD, convert to datetime based on format
    admissions_df = admissions_df.dropna(subset=['D.O.A', 'D.O.D'], how='all')
    admissions_df['D.O.A_1'] = pd.to_datetime(admissions_df['D.O.A'], errors='coerce', dayfirst=False)  # MM/DD/YYYY
    admissions_df['D.O.D_1'] = pd.to_datetime(admissions_df['D.O.D'], errors='coerce', dayfirst=False)  # MM/DD/YYYY

    admissions_df['D.O.A_2'] = pd.to_datetime(admissions_df['D.O.A'], errors='coerce', dayfirst=True)  # DD/MM/YYYY
    admissions_df['D.O.D_2'] = pd.to_datetime(admissions_df['D.O.D'], errors='coerce', dayfirst=True)  # DD/MM/YYYY

    # check that DOD >= DOA
    admissions_df[['D.O.A_correct', 'D.O.D_correct']] = admissions_df.apply(select_correct_date, axis=1, result_type="expand")
    admissions_df = admissions_df.drop(columns=['D.O.A_1', 'D.O.D_1', 'D.O.A_2', 'D.O.D_2','D.O.D','D.O.A'])
    admissions_df = admissions_df.dropna(subset=['D.O.A_correct', 'D.O.D_correct'], how='any')
    admissions_df['Delta'] = (admissions_df['D.O.D_correct'] - admissions_df['D.O.A_correct']).dt.days
    # check against existing column DURATION OF STAY
    admissions_df['check'] = admissions_df['DURATION OF STAY'] == (admissions_df['Delta'] + 1)
    admissions_df = admissions_df[admissions_df['check'] == True]
    admissions_df = admissions_df.drop(columns=['check','Delta'])
    # cast appropriate columns to numeric
    admissions_df[['HB', 'TLC', 'PLATELETS', 'GLUCOSE', 'UREA', 'CREATININE', 'BNP', 'EF', 
                   'CHEST INFECTION']] = admissions_df[['HB', 'TLC', 'PLATELETS', 'GLUCOSE', 'UREA', 
                    'CREATININE', 'BNP', 'EF', 'CHEST INFECTION']].apply(pd.to_numeric, errors='coerce')
    # cast appropriate columns to string
    admissions_df['MRD No.'] = admissions_df['MRD No.'].apply(safe_convert_int)
    # drop the null row in MRD No.
    admissions_df = admissions_df.dropna(subset=['MRD No.'])
    admissions_df['MRD No.'] = admissions_df['MRD No.'].astype('int64')
    admissions_df[['GENDER', 'RURAL', 'TYPE OF ADMISSION-EMERGENCY/OPD', 'month year', 
                   'OUTCOME']] = admissions_df[['GENDER', 'RURAL', 'TYPE OF ADMISSION-EMERGENCY/OPD', 
                    'month year', 'OUTCOME']].astype('string')
    return admissions_df

if __name__ == "__main__":
    admission_df_clean()