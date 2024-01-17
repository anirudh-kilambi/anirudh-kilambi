import os
import json
import boto3
from datetime import datetime

table = "CDTL"
with open("prefixes.json", "r") as inFile:
    jsonString = [i for i in inFile][0]
    prefixes = json.loads(jsonString)
    inFile.close()

start_date = datetime.strptime("2023-09-18", "%Y-%m-%d")
desired_prefixes = []
for p in prefixes:
    if table in p:
        file = p.split("/", 3)[-1]
        if file[-2:] != "_1":
            continue
        else:
            date_substring = file.split("_", 5)[2].split(".", 2)[0]
            date = datetime.strptime(date_substring, "%y%m%d")
            if date > start_date:
                desired_prefixes.append(p)
                print("we good", p)
            else:
                continue
    else:
        continue
outList = [f"s3://cdl-data-ops-ftp-sync/{i}" for i in desired_prefixes]
outData = json.dumps(outList)

with open(f"{table}.json", "w") as outFile:
    outFile.write(outData)
    outFile.close()

