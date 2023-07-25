
from pyspark.sql import SparkSession
from configparser import ConfigParser
from pyspark.sql.functions import to_date,date_format,to_timestamp
#from pyspark.sql import Window
from pyspark.sql import functions as F
import os
import glob
import shutil
from datetime import datetime

config = ConfigParser()

try:
    config.read("detailconfig.ini")
except:
    print("detailconfig.ini format error")
    raise SystemExit()

print(f"[INFO] Starting SPARK process ...")

master = config.get ("spark","master")
sparkjars = config.get ("spark","sparkjars")
url = config.get ("spark","url")
user = config.get ("spark","user")
password = config.get ("spark","password")
driver = config.get ("spark","driver")

spark = SparkSession \
    .builder \
    .master(master) \
    .appName('sparkreadpostgre') \
    .config('spark.jars', sparkjars) \
    .getOrCreate()

df_postgre = spark.read \
    .format('jdbc') \
    .option('url', url) \
    .option('dbtable', 'public.datamidasindx') \
    .option('user', user) \
    .option('password', password) \
    .option('driver', driver) \
    .load()

df_postgre.createOrReplaceTempView('datamidasindx')

print(f"[INFO] LOADING data from postgres...")

df_postgre = spark.sql("""
     SELECT trim(`Nama Segment`) AS `Nama Segment`, kec_saat_ini, waktu_saat_ini,
        waktu_minggu_1, waktu_minggu_2, waktu_minggu_3, waktu_minggu_4, waktu_minggu_5,
        waktu_minggu_6, waktu_minggu_7, waktu_minggu_8, waktu_minggu_9, waktu_minggu_10,
        waktu_minggu_11, waktu_minggu_12, kec_1, kec_2, kec_3, kec_4, kec_5, kec_6,
        kec_7, kec_8, kec_9, kec_10, kec_11, kec_12, rata2_prosen_kec,
        (kec_1 + kec_2 + kec_3 + kec_4 + kec_5 + kec_6 + kec_7 + kec_8 + kec_9 + kec_10 + kec_11 + kec_12) / 12 AS avg_kec
    FROM `datamidasindx`

""")

df_postgre.show(20)

print(f"[INFO] LOADING data from .CSV...")

df_relasi_segmen_ruas = spark.read.csv("Relasi_Segmen_ruas.csv", header=True, sep=';')
df_relasi_segmen_ruas.show(20)

df_volume_lalin = spark.read.csv("volume_lalin.csv", header=True, sep=';')
df_volume_lalin.show(20)

# joined_df = df_postgre.join(df_relasi_segmen_ruas, df_postgre["Nama Segment"] == df_relasi_segmen_ruas["Nama Segmen"], "inner") \
#                      .filter(df_postgre["rata2_prosen_kec"] >= 50) \
#                      .select(df_postgre["Nama Segment"], df_postgre["kec_saat_ini"],df_postgre["waktu_saat_ini"],
#                              df_postgre["waktu_minggu_1"],df_postgre["waktu_minggu_2"],df_postgre["waktu_minggu_3"],
#                              df_postgre["waktu_minggu_4"],df_postgre["waktu_minggu_5"],df_postgre["waktu_minggu_6"],
#                              df_postgre["waktu_minggu_7"],df_postgre["waktu_minggu_8"],df_postgre["waktu_minggu_9"],
#                              df_postgre["waktu_minggu_10"],df_postgre["waktu_minggu_11"],df_postgre["waktu_minggu_12"], 
#                              df_postgre["rata2_prosen_kec"],df_postgre["avg_kec"],
#                              df_relasi_segmen_ruas["Nama Ruas"])

print(f"[INFO] PROCESSING data in SPARK...")


joined_df = df_postgre.join(df_relasi_segmen_ruas, df_postgre["Nama Segment"] == df_relasi_segmen_ruas["Nama Segmen"], "inner") \
                     .filter(df_postgre["rata2_prosen_kec"] >= 50) \
                     .select(df_postgre["Nama Segment"], df_postgre["kec_saat_ini"],df_postgre["waktu_saat_ini"], 
                             df_postgre["rata2_prosen_kec"],df_postgre["avg_kec"],
                             df_relasi_segmen_ruas["Nama Ruas"]) 


joined_df_date = joined_df.withColumn("current_time", to_timestamp("waktu_saat_ini", "M/d/yyyy hmmss a"))
joined_df_date.show(20)
joined_df_time = joined_df_date.withColumn("time_only", date_format("current_time", "HH:mm:ss"))
joined_df_time.show(20)

#windowSpec = Window.partitionBy("Nama Segment").orderBy("current_time")

# Group by "Nama Segment" and find the maximum and minimum times
grouped_df = joined_df_time.groupBy("Nama Segment").agg(
    F.max("current_time").alias("max_time"),
    F.min("current_time").alias("min_time"),
    F.first("current_time").alias("waktu") 
)
# Calculate the time difference in minutes using the max and min times
grouped_df = grouped_df.withColumn(
    "Durasi diatas 50%",
    (F.unix_timestamp("max_time") - F.unix_timestamp("min_time")) / 60
) \
.orderBy("Durasi diatas 50%")

# Show the result
fifty_percent_speed_df = grouped_df.withColumn("waktu", date_format("waktu", "HH:mm")) \
                        .select("Nama Segment", "waktu", "Durasi diatas 50%")
#fifty_percent_speed_df = grouped_df.select("Nama Segment", "waktu", "Durasi diatas 50%")
fifty_percent_speed_df.show()

# Get the current date and time
current_date = datetime.now().strftime('%Y-%m-%d')
current_time = datetime.now().strftime('%H-%M-%S')

# Specify the desired file name
desired_file_name = f'50_percent_speed_{current_date}_{current_time}'

# Specify the output directory path
output_directory = '/mnt/c/linux/lalin_test/traffic_ex/output'

# Generate the output file path
output_file_path = f'{output_directory}/fifty_percent_speed.csv'

# Export the DataFrame to CSV
fifty_percent_speed_df.write.csv('/mnt/c/linux/lalin_test/traffic_ex/sparkdf_csv', header=True, mode='overwrite')
#/mnt/c/Users/HP/AppData/Local/Temp/tmp/temp_csv

# Search for the output file with the pattern "part-00000"
output_files = glob.glob('/mnt/c/linux/lalin_test/traffic_ex/sparkdf_csv/part-00000*')

if output_files:
    # Get the first file in the list (assuming there is only one)
    output_file = output_files[0]

    # Extract the directory path and file extension
    dir_path = os.path.dirname(output_file)
    file_extension = os.path.splitext(output_file)[1]

    # Construct the new file path with the desired file name
    new_file_path = os.path.join(dir_path, desired_file_name + file_extension)

    # Rename the output file to the desired file name
    os.rename(output_file, new_file_path)

    # Move the file to the desired location
    shutil.move(new_file_path, output_file_path)
else:
    print("No output file found.")

# # Convert DataFrame to JSON string
# json_string_50speed = fifty_percent_speed_df.toJSON().collect()

# # Define the path to the output JSON file
# output_file = '/mnt/c/linux/lalin_test/traffic_ex/fifty_percent_speed.json'

# # Write JSON string to the output file
# with open(output_file, 'w') as file:
#     for json_obj in json_string_50speed:
#         file.write(json_obj + '\n')

# # Print the JSON string
# for json in json_string_50speed:
#     print(json)