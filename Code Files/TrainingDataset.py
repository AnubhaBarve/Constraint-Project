# Created on: October 15, 2019
# Created by: Anubha Barve (anubha.barve@constellation - Intern 2019)
# File name: TrainingDataset.py
# Purpose: This file is developed to create a training dataset for the machine learning model.

# -----------------------------------------------------------------------------------------------------------------------------------------------------------------------------
# import statements

import pandas as pd
from tqdm import tqdm
import datetime
import string as str

# -----------------------------------------------------------------------------------------------------------------------------------------------------------------------------
# Code starts

def filter(constraints, outages, LODF_file):

    dataset = pd.DataFrame(data=None, columns=['facilityname', 'contingency', 'transmission_outage', 'date', 'time', 'facility_type', 'shadow_price', 'voltage', 'binding_status'])

    print(dataset)
    n = 0
    for i,r in tqdm(LODF_file.iterrows()):
        if abs(r['lodf']) >= 0.1:
            row = constraints.loc[constraints['interface_name'] == r['interface name']]

            for i1, r1 in row.iterrows():
                dataset.at[n, 'facilityname'] = r1['facilityname']
                dataset.at[n, 'contingency'] = r1['contingency']
                dataset.at[n, 'date'] = r1['date']
                dataset.at[n, 'time'] = r1['time']
                dataset.at[n, 'facility_type'] = r1['facilitytype']
                dataset.at[n, 'shadow_price'] = r1['shadowprice']
                dataset.at[n, 'voltage'] = r1['voltage']
                dataset.at[n, 'binding_status'] = 1

                row1 = outages.loc[outages['from_bus_number'] == r['from_bus_number']]
                fi = row1.drop_duplicates(subset="facility")
                for i2, r2 in fi.iterrows():
                    dataset.at[n, 'transmission_outage'] = r2['facility']
                n += 1

    return dataset



def main():
    constraints = pd.read_excel(r"S:\asset ops\GO_Group\Interns\2019\Anubha\Constraint Project\Constraint-Project\Data\Trial Data\Constraints.xlsx", sheet_name="2019 Sample", index= False)
    #constraints = constraints.columns.str.strip().str.lower().str.replace(" ", "_")

    constraints['datetime'] = pd.to_datetime(constraints['datetime'])
    constraints['time'] = constraints['datetime'].dt.time
    constraints['date'] = constraints['datetime'].dt.date

    #print(constraints['date'])
    #print(constraints['time'])

    outages = pd.read_excel(r"S:\asset ops\GO_Group\Interns\2019\Anubha\Constraint Project\Constraint-Project\Data\Trial Data\Outages.xlsx", sheet_name="2019 Sample", index=False)
    #outages = outages.columns.str.strip().str.lower().str.replace(" ", "_")

    outages['startdate'] = pd.to_datetime(outages['startdate'])
    outages['time'] = outages['startdate'].dt.time
    outages['date'] = outages['startdate'].dt.date

    #print(outages['date'])
    #print(outages['time'])


    lodf_file = pd.read_excel(r"S:\asset ops\GO_Group\Interns\2019\Anubha\Constraint Project\Constraint-Project\Data\Trial Data\CalculationLODF.xlsx", sheet_name="LODF", index=False)
    lodf_file.columns = [x.lower() for x in lodf_file.columns]

    result = filter(constraints, outages, lodf_file)

    print(result.info())
    result['time'] = pd.to_datetime(result['time'], format='%H:%M:%S')
    print(result.info())

    result = result.drop_duplicates(subset=['facilityname', 'transmission_outage', 'date', 'time'])

    writer = pd.ExcelWriter(r"S:\asset ops\GO_Group\Interns\2019\Anubha\Constraint Project\Constraint-Project\Data\Trial Data\TrainingDataset.xlsx", datetime_format='hh:mm:ss')
    result.to_excel(writer, "dataset")
    writer.save()

if __name__ == "__main__":
    main()

