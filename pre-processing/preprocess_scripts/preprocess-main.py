from mortality import mortality_df_clean
from admission import admission_df_clean
from weather import weather_df_clean
import pandas as pd
import os 

def main():
    # load cleaned data
    cleaned_weather_df = weather_df_clean()
    cleaned_admission_df= admission_df_clean()
    cleaned_mortaility_df = mortality_df_clean()

    # join datasets on dates
    result_no_mortality = pd.merge(cleaned_admission_df, cleaned_weather_df, left_on='D.O.A_correct', right_on='date', how='left')
    result_no_mortality = result_no_mortality.drop(columns=['date','month year','duration of intensive unit stay'])

    # join on MRD
    final = pd.merge(result_no_mortality, cleaned_mortaility_df, left_on='MRD No.', right_on='MRD', how='left')
    final = final.drop(columns=['AGE_y','GENDER_y'])
    script_dir = os.path.dirname(os.path.realpath(__file__))
    one_folder_up = os.path.dirname(script_dir)
    final_path = os.path.join(one_folder_up, "final_data", "final_data.csv")
    print(final.dtypes)

    # provisional SQL data types
    with open("column_types.txt", "w") as f:
        for col in final.columns:
            dtype = final[col].dtype
            if pd.api.types.is_integer_dtype(dtype):
                sql_type = "INTEGER"
            elif pd.api.types.is_float_dtype(dtype):
                sql_type = "FLOAT"
            elif pd.api.types.is_bool_dtype(dtype):
                sql_type = "BOOLEAN"
            elif pd.api.types.is_datetime64_any_dtype(dtype):
                sql_type = "DATE"
            else:
                sql_type = "VARCHAR"

            f.write(f"{col} {sql_type},\n")

    # save df to a CSV file
    final.to_csv(final_path, index=False)

    print(f"Data saved to {final_path}")

if __name__ == "__main__":
    main()