import os
import json
import boto3


client = boto3.client("s3")
bucket = "cdl-data-ops-ftp-sync"
prefix = "micros/in/"

obj = client.list_objects_v2(Bucket=bucket, Prefix=prefix)
prefixes = []

while "NextContinuationToken" in obj.keys():
    for i in obj["Contents"]:
        prefixes.append(i["Key"])

    token = obj["NextContinuationToken"]
    print(token)
    obj = client.list_objects_v2(Bucket=bucket,Prefix=prefix, ContinuationToken=token)
    continue

for i in obj["Contents"]:
    prefixes.append(i["Key"])

print(len(prefixes))
print("writing JSON")
outData = json.dumps(prefixes)
with open("prefixes.json", "w") as outFile:
    outFile.write(outData)
    outFile.close()




