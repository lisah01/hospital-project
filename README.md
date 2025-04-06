# hospital-project

ETL process of hospital admissions, mortality, and weather data

Dataset from: [Kaggle](https://www.kaggle.com/datasets/ashishsahani/hospital-admissions-data?select=table_headings.csv)
weather data from [Open Meteo API](https://open-meteo.com/)

To run:

1. Start PostgreSQL
`sudo service postgresql start`

2. `sudo -i -u postgres`

3. `psql` to log

4. Create the table with SQL script `weather_mortality_db=# \i <path-to-file>/setup_table.sql`

5. Run the Spark job 
