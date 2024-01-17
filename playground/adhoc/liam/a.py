import csv
import json
def convertCSV(fileName:str):

    # output JSON should look like:
    # {
        # "name" : value, layer2:  {valueSource, opcItemPath, dataType, tagType, opcServer, layer3 : {keys, if you had a third layer}}
    # }
    # open file in python
    outJson = {"tags" : []}
    with open(fileName, "r") as inFile:
        reader = csv.DictReader(inFile)
        #iterate through rows from csv

        for row in reader:
            cleaned_data = {}

            cleaned_data["historyProvider" ]= row["historyProvider"]

            layer2Data = {
                'setpointA': row['setpointA'],
                'label': row['label'],
                'name1' : row['name1'],
                'priority' : row['priority'],
            }
            cleaned_data["alarms"] = layer2Data
            cleaned_data["opcItemPath"] = row["opcItemPath"]
            cleaned_data["dataType"] = row["dataType"]
            cleaned_data["documentation"] = row["documentation"]
            cleaned_data["name"] = row["name"]
            cleaned_data["tooltip" ] = row["tooltip"]
            cleaned_data["historyEnabled"] = row["historyEnabled"]
            cleaned_data["tagType"] = row["tagType"]
            cleaned_data["opcServer"] = row["opcServer"]

            print(cleaned_data)
            outJson["tags"].append(cleaned_data)

        inFile.close()

    with open("out.json", "w") as outFile:
        json.dump(outJson, outFile)
        outFile.close()
    

if __name__ == "__main__":
    convertCSV(fileName="faultListCsv.csv")
