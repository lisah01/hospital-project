import pandas as pd
import numpy as np
import os

def weather_df_clean():
    script_dir = os.path.dirname(os.path.realpath(__file__))
    one_folder_up = os.path.dirname(script_dir)
    weather_data_raw_path = os.path.join(one_folder_up, 
                                         "preprocess_data_raw", "weather-api.csv")
    weather_df = pd.read_csv(weather_data_raw_path)
    weather_df = weather_df.drop(columns=['ID'])
    weather_df['date'] = pd.to_datetime(weather_df['date'], 
                                        errors='coerce', dayfirst=False)  # MM/DD/YYYY
    weather_df['date'] = weather_df['date'].dt.strftime('%Y-%m-%d')

    # aggregate columns appropriately (dataset is hourly)
    aggregated_df = weather_df.groupby('date').agg({
        'temperature_2m': 'mean', # average temp of the day
        'precipitation': 'sum', # total precipitation in the day
        'rain': 'sum', 
        'snowfall': 'sum', 
        'dew_point_2m': 'mean', # avg dew point
        'relative_humidity_2m': 'mean' # avg humidity
    }).reset_index()

    # convert to datetime
    aggregated_df['date'] = pd.to_datetime(aggregated_df['date'])
    return aggregated_df

if __name__ == "__main__":
    weather_df_clean()