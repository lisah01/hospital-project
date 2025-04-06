import pandas as pd
import os
def select_correct_date(row):
    valid_1 = not pd.isna(row['D.O.A_1']) and not pd.isna(row['D.O.D_1']) and row['D.O.D_1'] >= row['D.O.A_1']
    valid_2 = not pd.isna(row['D.O.A_2']) and not pd.isna(row['D.O.D_2']) and row['D.O.D_2'] >= row['D.O.A_2']    
    if valid_1:
        return row['D.O.A_1'], row['D.O.D_1']
    elif valid_2:
        return row['D.O.A_2'], row['D.O.D_2']
    else:
        return pd.NaT, pd.NaT  # If both are invalid
    
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
    
def safe_convert_int(value):
    try:
        return int(value)  
    except (ValueError, TypeError):  
        return None  
def safe_convert_str(value):
    try:
        return str(value)  
    except (ValueError, TypeError):  
        return ""  

def main():
    # find the current path
    script_dir = os.path.dirname(os.path.realpath(__file__))
    one_folder_up = os.path.dirname(script_dir)
    admissions_data_raw = os.path.join(one_folder_up, "preprocess_data_raw", "HDHI_Admission_data.csv")
    
    # read csv
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
    admissions_df['check'] = admissions_df['DURATION OF STAY'] == (admissions_df['Delta'] + 1)
    admissions_df = admissions_df[admissions_df['check'] == True]
    admissions_df = admissions_df.drop(columns=['check','Delta'])
    # print(admissions_df.columns)
    # print(admissions_df.apply(pd.Series.unique))

    types_df = dict(admissions_df.dtypes)
    # print(dict(types_df))
    obj_list, int_list = get_types(types_df)
    print(obj_list, "\n-------\n", int_list)
    # print(admissions_df.isnull().sum())

    admissions_df[['HB', 'TLC', 'PLATELETS', 'GLUCOSE', 'UREA', 'CREATININE', 'BNP', 'EF', 
                   'CHEST INFECTION']] = admissions_df[['HB', 'TLC', 'PLATELETS', 'GLUCOSE', 'UREA', 
                    'CREATININE', 'BNP', 'EF', 'CHEST INFECTION']].apply(pd.to_numeric, errors='coerce')
    
    admissions_df['MRD No.'] = admissions_df['MRD No.'].apply(safe_convert_int)
    admissions_df = admissions_df.dropna(subset=['MRD No.'])
    admissions_df['MRD No.'] = admissions_df['MRD No.'].astype('int64')
    admissions_df[['GENDER', 'RURAL', 'TYPE OF ADMISSION-EMERGENCY/OPD', 'month year', 
                   'OUTCOME']] = admissions_df[['GENDER', 'RURAL', 'TYPE OF ADMISSION-EMERGENCY/OPD', 
                    'month year', 'OUTCOME']].astype('string')
    #apply(lambda col: col.apply(safe_convert_str))
    print(admissions_df.dtypes)

    # drop if not int or float
    # convert all the objects to strings or floats
    
    # SMOKING                                                                       [0, 1]
    # ALCOHOL                                                                       [0, 1]
    # DM                                                                            [1, 0]
    # HTN                                                                           [0, 1]
    # CAD                                                                           [0, 1]
    # PRIOR CMP                                                                     [0, 1]
    # CKD                                                                           [0, 1]
    # HB                                 [9.5, 13.7, 10.6, 12.8, 13.6, 13.5, 13.3, 12.6...
    # TLC                                [16.1, 9, 14.7, 9.9, 9.1, 22.3, 12.6, 9.5, nan...
    # PLATELETS                          [337, 149, 329, 286, 26, 322, 166, 328, nan, 1...
    # GLUCOSE                            [80, 112, 187, 130, 144, 217, 277, 159, 156, 2...
    # UREA                               [34, 18, 93, 27, 55, 51, 28, 30, nan, 29, 45, ...
    # CREATININE                         [0.9, 2.3, 0.6, 1.25, 1, nan, 0.8, 1.3, 1.2, 0...
    # BNP                                [1880, nan, 210, 1840, 1720, 518, 780, 534, 82...
    # RAISED CARDIAC ENZYMES                                                        [1, 0]
    # EF                                 [35, 42, nan, 16, 25, 30, 45, 60, 32, 40, 36, ...
    # SEVERE ANAEMIA                                                                [0, 1]
    # ANAEMIA                                                                       [1, 0]
    # STABLE ANGINA                                                                 [0, 1]
    # ACS                                                                           [1, 0]
    # STEMI                                                                         [0, 1]
    # ATYPICAL CHEST PAIN                                                           [0, 1]
    # HEART FAILURE                                                                 [1, 0]
    # HFREF                                                                         [1, 0]
    # HFNEF                                                                         [0, 1]
    # VALVULAR                                                                      [0, 1]
    # CHB                                                                           [0, 1]
    # SSS                                                                           [0, 1]
    # AKI                                                                           [0, 1]
    # CVA INFRACT                                                                   [0, 1]
    # CVA BLEED                                                                     [0, 1]
    # AF                                                                            [0, 1]
    # VT                                                                            [0, 1]
    # PSVT                                                                          [0, 1]
    # CONGENITAL                                                                    [0, 1]
    # UTI                                                                           [0, 1]
    # NEURO CARDIOGENIC SYNCOPE                                                     [0, 1]
    # ORTHOSTATIC                                                                   [0, 1]
    # INFECTIVE ENDOCARDITIS                                                        [0, 1]
    # DVT                                                                           [0, 1]
    # CARDIOGENIC SHOCK                                                             [0, 1]
    # SHOCK                                                                         [0, 1]
    # PULMONARY EMBOLISM                                                            [0, 1]
    # CHEST INFECTION                                                            [0, 1, \]

if __name__ == "__main__":
    main()