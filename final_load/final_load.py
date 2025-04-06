from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import os
# init Spark Session
current_dir = os.path.dirname(os.path.realpath(__file__))

# set JDBC driver path to script path
jdbc_driver_path = os.path.join(current_dir, 'postgresql-42.7.5.jar')
print(jdbc_driver_path)

# initiliaze SparkSession with the JDBC driver
spark = SparkSession.builder \
    .appName("Load Data into SQL Database") \
    .config("spark.jars", jdbc_driver_path) \
    .config("spark.sql.debug.maxToStringFields", "1000") \
    .getOrCreate()

script_dir = os.path.dirname(os.path.realpath(__file__))
one_folder_up = os.path.dirname(script_dir)
final_path = os.path.join(one_folder_up, "final_data", "final_data.csv")
# load the CSV data
df = spark.read.option("header", "true").csv(final_path)

# rename columns to match the specified column names
df = df.toDF(*[c.strip() for c in df.columns])

df = df.select(
    col("SNO").alias("SNO"),
    col("MRD No").alias("MRD No"),
    col("AGE_x").alias("AGE_x"),
    col("GENDER_x").alias("GENDER_x"),
    col("RURAL").alias("RURAL"),
    col("TYPE OF ADMISSION-EMERGENCY/OPD").alias("TYPE OF ADMISSION-EMERGENCY/OPD"),
    col("DURATION OF STAY").alias("DURATION OF STAY"),
    col("OUTCOME").alias("OUTCOME"),
    col("SMOKING").alias("SMOKING"),
    col("ALCOHOL").alias("ALCOHOL"),
    col("DM").alias("DM"),
    col("HTN").alias("HTN"),
    col("CAD").alias("CAD"),
    col("PRIOR CMP").alias("PRIOR CMP"),
    col("CKD").alias("CKD"),
    col("HB").alias("HB"),
    col("TLC").alias("TLC"),
    col("PLATELETS").alias("PLATELETS"),
    col("GLUCOSE").alias("GLUCOSE"),
    col("UREA").alias("UREA"),
    col("CREATININE").alias("CREATININE"),
    col("BNP").alias("BNP"),
    col("RAISED CARDIAC ENZYMES").alias("RAISED CARDIAC ENZYMES"),
    col("EF").alias("EF"),
    col("SEVERE ANAEMIA").alias("SEVERE ANAEMIA"),
    col("ANAEMIA").alias("ANAEMIA"),
    col("STABLE ANGINA").alias("STABLE ANGINA"),
    col("ACS").alias("ACS"),
    col("STEMI").alias("STEMI"),
    col("ATYPICAL CHEST PAIN").alias("ATYPICAL CHEST PAIN"),
    col("HEART FAILURE").alias("HEART FAILURE"),
    col("HFREF").alias("HFREF"),
    col("HFNEF").alias("HFNEF"),
    col("VALVULAR").alias("VALVULAR"),
    col("CHB").alias("CHB"),
    col("SSS").alias("SSS"),
    col("AKI").alias("AKI"),
    col("CVA INFRACT").alias("CVA INFRACT"),
    col("CVA BLEED").alias("CVA BLEED"),
    col("AF").alias("AF"),
    col("VT").alias("VT"),
    col("PSVT").alias("PSVT"),
    col("CONGENITAL").alias("CONGENITAL"),
    col("UTI").alias("UTI"),
    col("NEURO CARDIOGENIC SYNCOPE").alias("NEURO CARDIOGENIC SYNCOPE"),
    col("ORTHOSTATIC").alias("ORTHOSTATIC"),
    col("INFECTIVE ENDOCARDITIS").alias("INFECTIVE ENDOCARDITIS"),
    col("DVT").alias("DVT"),
    col("CARDIOGENIC SHOCK").alias("CARDIOGENIC SHOCK"),
    col("SHOCK").alias("SHOCK"),
    col("PULMONARY EMBOLISM").alias("PULMONARY EMBOLISM"),
    col("CHEST INFECTION").alias("CHEST INFECTION"),
    col("DOA_correct").alias("DOA_correct"),
    col("DOD_correct").alias("DOD_correct"),
    col("temperature_2m").alias("temperature_2m"),
    col("precipitation").alias("precipitation"),
    col("rain").alias("rain"),
    col("snowfall").alias("snowfall"),
    col("dew_point_2m").alias("dew_point_2m"),
    col("relative_humidity_2m").alias("relative_humidity_2m"),
    col("SNO1").alias("SNO1"),
    col("MRD").alias("MRD"),
    col("RURAL/URBAN").alias("RURAL/URBAN"),
    col("DATE OF BROUGHT DEAD").alias("DATE OF BROUGHT DEAD")
)

# set up JDBC
jdbc_url = "jdbc:postgresql://localhost:5432/weather_mortality_db"  # PostgreSQL URL

properties = {
    "user": "postgres",                            
    "password": os.getenv("POSTGRES_PASSWORD"),    
    "driver": "org.postgresql.Driver"              
}

# write to SQL server
df.write.jdbc(url=jdbc_url, table="patient_data", mode="overwrite", properties=properties)

print("Data successfully loaded into the SQL database.")