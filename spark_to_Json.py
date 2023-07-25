import csv
import json

# Path to the CSV file
csv_file = '/mnt/c/linux/lalin_test/traffic_ex/fifty_percent_speed.csv/part-00000-ea8664a2-9ec7-4bd1-8d6b-e5f1b8603d89-c000.csv'

# Path to the output JSON file
json_file = '/mnt/c/linux/lalin_test/traffic_ex/fifty_percent_speed.json'

# Read the CSV file
csv_data = []
with open(csv_file, 'r') as file:
    reader = csv.DictReader(file)
    for row in reader:
        csv_data.append(row)

# Convert CSV data to JSON
json_data = []
for row in csv_data:
    json_obj = {
        "Nama Segment": row["Nama Segment"],
        "waktu": row["waktu"],
        "Durasi diatas 50%": float(row["Durasi diatas 50%"])
    }
    json_data.append(json_obj)

# Write JSON data to the output file
with open(json_file, 'w') as file:
    for json_obj in json_data:
        json_string = json.dumps(json_obj)
        file.write(json_string + '\n')

# Print the JSON data
for json_obj in json_data:
    print(json.dumps(json_obj))
