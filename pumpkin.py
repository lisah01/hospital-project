import pandas as pd
import numpy as np
import datetime
from dateutil import parser

admissions_data_raw = "HDHI Admission data.csv"
mortality_data_raw = "HDHI Mortality data.csv"
hospital_api_raw = "hospital-api.csv"

admissions_df = pd.read_csv(admissions_data_raw)
mortality_df = pd.read_csv(mortality_data_raw)
api_df = pd.read_csv(hospital_api_raw)

print(admissions_df)
# print(mortality_df)
# print(api_df)

date_string = admissions_df["D.O.A"]
# date_string = admissions_df.loc[15753]["D.O.A"]
admissions_df['D.O.A'] = admissions_df['D.O.A'].apply(lambda x: parser.parse(x).strftime('%Y-%m-%d'))
admissions_df['D.O.D'] = admissions_df['D.O.D'].apply(lambda x: parser.parse(x).strftime('%Y-%m-%d'))
# print(admissions_df)
admissions_df.describe()
admissions_df.info()
