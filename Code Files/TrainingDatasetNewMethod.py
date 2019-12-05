# Created on: November 25, 2019
# Created by: Anubha Barve (anubha.barve@constellation - Intern 2019)
# File name: TrainingDataset.py
# Purpose: This file is developed to create a training dataset for the machine learning model using different
# methodology

# -----------------------------------------------------------------------------------------------------------------------------------------------------------------------------
# import statements

import pandas as pd
from tqdm import tqdm
import datetime as dt
import numpy as np
import time
import multiprocessing as mp
from multiprocessing import Pool
from copy import deepcopy

# -----------------------------------------------------------------------------------------------------------------------------------------------------------------------------
# Code starts
# function filter_horizontal is used to create the training dataset which takes constraints, outages and LODF
# calculation file as parameters


def filter_horizontal(LODF_file,constraints, outages):

    # constraints - contains all the constraints ocnsidered for calculating LODF
    # outages - contains all the outages picked according to the constraint date which
    # were furhter considered for calculating LODF
    # LODF_file - contains the result of lodf calculation

    # dataset stores the final training data in the form of a dataframe
    dataset = pd.DataFrame(data=None, columns=['facilityname', 'contingency', 'date', 'time', 'facility_type', 'shadow_price', 'voltage', 'binding_status'])

    # dropping the duplicate rows to keep unique outages that are present in the LODF_file dataframe
    file = LODF_file.drop_duplicates(subset=['from_bus_number', 'to_bus_number'],keep='first')

    # dropping the duplicate rows in constraints dataframe to keep unique facilityname only
    cons = constraints.drop_duplicates(subset=['facilityname', 'date', 'time'])

    # iterating over unique outages obtained from the LODF_file dataframe
    for i,r in file.iterrows():

        # from outages dtaaframe extracting those outages which have equal "from_bus_number" and "to_bus_number"
        # when compared to file dataframe.
        outage = outages.loc[(outages['from_bus_number'] == r['from_bus_number']) & (outages['to_bus_number'] == r['to_bus_number'])]

        # keeping only the unique outages
        outage = outage.drop_duplicates(subset='facility', keep='first')

        # iterating through the unique outages obtained above and adding it as column
        # in the dataset dataframe, assigning it a value of 0
        for i1, r1 in outage.iterrows():

            dataset[r1['facility']] = 0

    # variable to keep the index of dtaaset dataframe
    n = 0

    # iterating through LODF_file dataframe
    for i, r in tqdm(LODF_file.iterrows()):

        # picking up all the rows where "interface name" is equivalent to the ith(i) row of LODF_file
        row1 = LODF_file.loc[LODF_file['interface name'] == r['interface name']]

        # keeping only those rows which have unique constraint-outage pair by removing duplicate rows
        row1 = row1.drop_duplicates(subset=['from_bus_number', 'to_bus_number', 'interface name'], keep='first')

        # getting all the constraint related information such that r['interface name'] is equal
        # to cons['interface name']
        cons_rows = cons.loc[cons['interface_name'] == r['interface name']]

        # iterating over cons_rows dataframe to populate the datatset dataframe
        for i1, r1 in cons_rows.iterrows():

            dataset.at[n, 'facilityname'] = r1['facilityname']
            dataset.at[n, 'contingency'] = r1['contingency']
            dataset.at[n, 'date'] = r1['date']
            dataset.at[n, 'time'] = r1['time']
            dataset.at[n, 'facility_type'] = r1['facilitytype']
            dataset.at[n, 'shadow_price'] = r1['shadowprice']
            dataset.at[n, 'voltage'] = r1['voltage']
            dataset.at[n, 'binding_status'] = 1

            # iterating over three lists simultaneously containing outage and lodf value
            for f1,t1 in row1.iterrows():

                out = outages.loc[(outages['from_bus_number'] == t1['from_bus_number']) & (outages['to_bus_number'] == t1['to_bus_number'])]

                for i2, r2 in out.iterrows():
                    dataset.at[n, r2['facility']] = 1
                    break

            # incrementing the dataset dataframe's index by 1
            n += 1

    # dropping duplicate values
    dataset = dataset.drop_duplicates(subset=['facilityname', 'contingency', 'date', 'time'])
    # filling blank cells with '0'
    dataset = dataset.fillna('0')
    # returning the final dataframe
    return dataset
# --------------------------------------------- function ends ----------------------------------------------------------

# main function


def main():
    # constraints stores the list of constraints (sample) in the form of a dataframe
    constraints = pd.read_excel(r"S:\asset ops\GO_Group\Interns\2019\Anubha\Constraint Project\Constraint-Project\Data\UniqueConstraintContingencyPair\ConstraintContingencyFinalList.xlsx",sheet_name="2019", index=False)

    # converting the "datetime" column to datetime object for ease of use
    constraints['datetime'] = pd.to_datetime(constraints['datetime'])
    # extracting time from the datetime object and storing it into a new column of constraints dataframe
    constraints['time'] = constraints['datetime'].dt.time
    # extracting date from the datetime object and storing it into a new column of constraints dataframe
    constraints['date'] = constraints['datetime'].dt.date

    # creating an empty dataframe to store date range to make comparison with inputfile dataframe easier and faster.
    date_range = pd.DataFrame()

    # populating "strtdate" column of date_range dataframe with the start date of 2019
    date_range.at[0, 'strtdate'] = "2019-01-01"

    # populating "enddate" column of date_range dataframe with the end date of 2019
    date_range.at[0, 'enddate'] = "2019-07-24"

    # "strtdate" column of date_range dataframe is converted to datetime format for ease of use.
    date_range['strtdate'] = pd.to_datetime(date_range['strtdate'])

    # "enddate" column of date_range dataframe is converted to datetime format for ease of use.
    date_range['enddate'] = pd.to_datetime(date_range['enddate'])

    constraints['date'] = pd.to_datetime(constraints['date'])

    constraints = deepcopy(constraints.loc[(constraints['date'] >= date_range.at[0, 'strtdate']) & (
                constraints['date'] <= date_range.at[0, 'enddate'])])

    constraints = constraints.loc[constraints['shadowprice'] >= 500]

    # outages stores the list of outages (sample) in the form of a dataframe
    outages = pd.read_excel(r"S:\asset ops\GO_Group\Interns\2019\Anubha\Constraint Project\Constraint-Project\Data\TransmissionOutagesList.xlsx",sheet_name="2019", index=False)

    # converting the "startdate" column to datetime object for ease of use
    outages['startdate'] = pd.to_datetime(outages['startdate'])
    # extracting time from the datetime object and storing it into a new column of outages dataframe
    outages['time'] = outages['startdate'].dt.time
    # extracting date from the datetime object and storing it into a new column of outages dataframe
    outages['date'] = outages['startdate'].dt.date

    # loadf_file stores the calculation result in the form of a dataframe
    lodf_file = pd.read_excel(r"S:\asset ops\GO_Group\Interns\2019\Anubha\Constraint Project\Constraint-Project\Data\Trial Data\CalculationDependentLODF.xlsx", sheet_name="LODF", index=False)
    # converting the column names to lower case for ease of use
    lodf_file.columns = [x.lower() for x in lodf_file.columns]

    lodf_file = lodf_file.loc[abs(lodf_file['lodf']) > 0.1]

    # creating processors for parallel processing
    pool = Pool(mp.cpu_count())

    # splitting the lodf file into chunk for parallel processing
    lodf_split = np.array_split(lodf_file, mp.cpu_count())

    # noting the starting time
    start = time.time()

    # call to filter_horizontal function
    result = filter_horizontal(lodf_file, constraints, outages)

    # dropping duplicates
    result = result.drop_duplicates(subset=['facilityname', 'contingency', 'date', 'time'])

    result = result.fillna('0')

    for col in result:
        lis = result[col].nunique()
        if lis == 1:
            lis = result[col].unique()
            if "0" in lis and "1" not in lis:
                result.drop(col, 1, inplace=True)

    hours = [(dt.time(i)) for i in range(24)]

    n = len(result)

    '''for id, ro in result.iterrows():
        for i in hours:
            if ro['time'] != i:
                n += 1
                result.at[n] = ro
                result.at[n, 'binding_status'] = 0
                result.at[n, 'time'] = i'''

    # converting the "time" column of result dataframe into datetime object formatted as '%H:%M:%S'
    result['time'] = pd.to_datetime(result['time'], format='%H:%M:%S')

    result = result.drop_duplicates(subset=['facilityname', 'contingency', 'date', 'time', 'binding_status'])

    # creating excel file
    writer = pd.ExcelWriter(r"S:\asset ops\GO_Group\Interns\2019\Anubha\Constraint Project\Constraint-Project\Data\Trial Data\TrainingDatasetNew.xlsx")
    # writing the result to the created excel file
    result.to_excel(writer, "dataset")

    result_unique = result.drop_duplicates(subset='facilityname', keep='first')

    for i, r in result_unique.iterrows():

        print("Hello")

        dataset = result.loc[result['facilityname'] == r['facilityname']]

        dataset.to_excel(writer, str(i))

    # saving the excel file
    writer.save()

    time_elapsed = (time.time() - start)
    print(time_elapsed)

    #result = pd.read_excel(r"S:\asset ops\GO_Group\Interns\2019\Anubha\Constraint Project\Constraint-Project\Data\Trial Data\TrainingDataset3.xlsx", sheet_name= 'dataset', index=False)


# call to main function


if __name__ == "__main__":
    main()

