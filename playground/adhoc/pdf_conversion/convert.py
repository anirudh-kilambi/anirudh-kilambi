import csv
import time
import PyPDF2 as pdf

inFile = open("in.pdf", "rb")

reader = pdf.PdfReader(inFile )

counter = 0
data = []
default_dict = {}
ignore = ["GA Type:eCash(1)", "GA Class:eCash(1)", "GA Account Name", "GA Account Number", "GA Posting Account ", "Number", "Discount", "Price Level", "Profit Center Group", "Configured Limit", "Current Activity", "Available Credit", "Selcted For: Store = 8074 Cincinatti (31); GA Class = eCash (1); GA Type = eCash (1) "]
# while counter < 100:
    # page = reader.pages[counter]
    # line_count = 0
    # if counter == 0:
        # skip_until = 12
    # else:
        # skip_until = 

    # for line in page.extract_text().splitlines():
        # if line_count < skip_until:
            # line_count += 1
            # continue
        # else:
            # time.sleep(0.05)
    

    # counter += 1

fixLine = []
srirata = False
for page in reader.pages:
    print(counter)
    pushData = False
    row_count = 0
    for line in page.extract_text().splitlines():
        if line in ignore:
            continue
        else:
            if "Selected For" in line or "InfoGenesis" in line or "Executed By" in line or "/16088" in line or "GA Account Balances" in line or "Grouped by" in line or "Sorted by" in line or "10/11/2023" in line:
                continue
            elif "SRIRATANAKOUL" in line or 'VENKATESH'in line:
                srirata = True
                continue
            elif srirata == True:
                val = data[-1]
                newval = f"{val} {line}"
                data.append(newval)
                srirata = False
            else:
                data.append(line)

    counter += 1

row_count = 0

newData = []
subset = []
counter = 0
for i in data:
    if counter == 0:
        subset.append(i)
        counter += 1
    elif counter == 1:
        first_val_string = False
        try:
            int(subset[0])
        except ValueError:
            first_val_string = True

        try:
            int(i)
            subset.append(i)
            counter +=1
        except ValueError:
            if first_val_string:
                val = subset[0]
                newVal = f"{val} {i}"
                subset[0] = newVal
            else:
                subset.append(i)
                counter +=1

    elif counter ==2:
        subset.append(i)
        counter +=1

    elif counter ==3:
        subset.append(i)
        counter +=1

    elif counter ==4:
        subset.append(i)
        counter +=1

    elif counter ==5:
        if 'Profit' not in i:
            subset.append("")
            subset.append(i)
            counter +=2
        else:
            subset.append(i)
            counter +=1

    elif counter ==6:
        subset.append(i)
        counter +=1

    elif counter ==7:
        subset.append(i)
        counter +=1
    elif counter ==8:
        subset.append(i)
        newData.append(subset)
        subset = []
        counter = 0
# n = 9
# final = [data[i * n:(i + 1) * n] for i in range((len(data) + n - 1) // n )]  

# totals = final[-1]
# new_final = final[:-1]

with open("out.csv", "w") as outFile:
    writer = csv.writer(outFile)
    writer.writerows(newData)
