import os
import json

table = "CHDR"
with open(f"{table}.json", "r") as inFile:
    jsonString = [i for i in inFile][0]
    files = json.loads(jsonString)
    inFile.close()

print("Creating directory", table)
os.mkdir(table)
os.chdir(table)

for f in files:
    os.system(f"aws s3 cp {f} ./")


