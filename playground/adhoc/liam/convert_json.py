import csv
import json
from datetime import datetime

def convertJson(fileName:str):
    """
    Converts a JSON that is a list of dictionaries into a CSV.
    """

    # open file in python
    with open(fileName, "r") as inFile:
        jsonList = [i for i in inFile]
        inFile.close()

    jsonString = "".join(jsonList) 
    dataDict = json.loads(jsonString)
    #data dict format is Dict["key" : List of dictionaries]
    records = dataDict["tags"]
    print(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}: Number of records => {len(records)}") 

    # need to grab field names
    print(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}: Retrieve column names (dictionary keys) from one record") 
    keys = list(records[0].keys())


    with open("out.csv", "w") as outFile:
        writer = csv.DictWriter(outFile, delimiter=",", fieldnames=keys)
        writer.writeheader()
        writer.writerows(records)
        outFile.close()
    

if __name__ == "__main__":
    convertJson("tags.json")
