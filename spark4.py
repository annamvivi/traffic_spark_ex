from pyspark.sql import SparkSession
from configparser import ConfigParser
from pyspark.sql.functions import to_date,date_format,to_timestamp
#from pyspark.sql import Window
from pyspark.sql import functions as F

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

#output
csv_file_name = '/mnt/c/linux/lalin_test/traffic_ex/fifty_percent_speed.csv'
fifty_percent_speed_df.write.csv(csv_file_name, header=True, mode="overwrite")

# Convert DataFrame to JSON string
json_string_50speed = fifty_percent_speed_df.toJSON().collect()

# Define the path to the output JSON file
output_file = '/mnt/c/linux/lalin_test/traffic_ex/fifty_percent_speed.json'

# Write JSON string to the output file
with open(output_file, 'w') as file:
    for json_obj in json_string_50speed:
        file.write(json_obj + '\n')

# Print the JSON string
for json in json_string_50speed:
    print(json)