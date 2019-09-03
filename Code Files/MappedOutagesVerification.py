# Created on: August 07,2019
# Created by: Anubha Barve (anubha.barve@constellation.com - Intern 2019)
# Purpose - this file is created to verfiy the mapping which was done using vlookup in excel sheet.
#           Verification is done by approximate string matching.

# import statements
# --------------------------------------------------------------------------------------------------------------------------------------------------------------
import pandas as pd
from fuzzywuzzy import fuzz
import multiprocessing as mp
import numpy as np
from multiprocessing import Pool

# Code Starts
# -----------------------------------------------------------------------------------------------------------------------------------------------------------------
# approximatematching takes inputFile parameter which stores the mapped transmission outages in the form of dataframe.


def approximatematching(inputFile):

    # Looping through the inputFile dataframe
    for index,row in inputFile.iterrows():
        # String matching for line facilities
        if str(row['facility_type']) == 'LINE':
            if fuzz.ratio(str(row['from_split_1']),str(row['from_bus_name'])) > fuzz.ratio(str(row['from_split_2']), str(row['from_bus_name'])):
                inputFile.at[index,'from_ratio'] = fuzz.ratio(str(row['from_split_1']),str(row['from_bus_name']))
                inputFile.at[index, 'fromstation_ratio'] = fuzz.ratio(str(row['fromstation']), str(row['from_bus_name']))
            else:
                inputFile.at[index, 'from_ratio'] = fuzz.ratio(str(row['from_split_2']), str(row['from_bus_name']))
                inputFile.at[index, 'fromstation_ratio'] = fuzz.ratio(str(row['fromstation']),str(row['from_bus_name']))

            if  fuzz.ratio(str(row['from_split_1']), str(row['to_bus_name'])) > fuzz.ratio(str(row['from_split_2']), str(row['to_bus_name'])):
                inputFile.at[index, 'to_ratio'] = fuzz.ratio(str(row['from_split_1']), str(row['to_bus_name']))
                inputFile.at[index, 'tostation_ratio'] = fuzz.ratio(str(row['tostation']), str(row['to_bus_name']))
            else:
                inputFile.at[index, 'to_ratio'] = fuzz.ratio(str(row['from_split_2']), str(row['to_bus_name']))
                inputFile.at[index, 'tostation_ratio'] = fuzz.ratio(str(row['tostation']), str(row['to_bus_name']))

        # String matching for transformer facilities
        if str(row['facility_type']) == 'XFMR':
            inputFile.at[index,'from_ratio'] = fuzz.ratio(str(row['from']),str(row['from_bus_name']))
            inputFile.at[index, 'fromstation_ratio'] = fuzz.ratio(str(row['fromstation']), str(row['from_bus_name']))
            inputFile.at[index, 'to_ratio'] = fuzz.ratio(str(row['from']), str(row['to_bus_name']))
            inputFile.at[index, 'tostation_ratio'] = fuzz.ratio(str(row['tostation']), str(row['to_bus_name']))

    # Storing the maximum percentage out of 'from_ratio' and 'fromstation_ratio'
    for i1,r1 in inputFile.iterrows():
        if r1['from_ratio'] > r1['fromstation_ratio']:
            inputFile.at[i1,'from_matching'] = r1['from_ratio']
        else:
            inputFile.at[i1,'from_matching'] = r1['fromstation_ratio']
    # storing the maximum percentage out of 'to_ratio' and 'tostation_ratio'
    for i1, r1 in inputFile.iterrows():
        if r1['to_ratio'] > r1['tostation_ratio']:
            inputFile.at[i1, 'to_matching'] = r1['to_ratio']
        else:
            inputFile.at[i1, 'to_matching'] = r1['tostation_ratio']

    # return the changes inputFile dataframe
    return inputFile

# main function
# -----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------


def main():

    # inputFile stores the mapped transmission outages from the year 2014 to 2019 in the form of dataframe
    inputFile = pd.read_excel(r"S:\asset ops\GO_Group\Interns\2019\Anubha\Constraint Project\Constraint-Project\Data\Transmission Outages Verification\MappedOutagesVerification.xlsx",sheet_name="Outages", index=False)
    # column names are formatted by changing them into lower case, replacing the spaces with underscore and brackets are removed, if any
    inputFile.columns = inputFile.columns.str.strip().str.lower().str.replace(' ', '_').str.replace('(','').str.replace(')','')
    # this preserves the index and converts it into a column
    inputFile.reset_index(inplace=True, drop=True)

    # parallel processing - to enable faster execution
    # this gives the number of processors available to use in the system
    cores = mp.cpu_count()
    # creating processes depending on the number of processors
    pool = Pool(cores)
    # splitting the dataframe into a number equivalent to the number of processors
    inputfile_split = np.array_split(inputFile, cores)
    # using map function for parallel execution and concatenating the result back to a dataframe
    result = pd.concat(pool.map(approximatematching, inputfile_split))
    # closing all processes
    pool.close()
    # joining all processes
    pool.join()

    # create an excel file to store the resulting dataframe
    writer = pd.ExcelWriter(r"S:\asset ops\GO_Group\Interns\2019\Anubha\Constraint Project\Constraint-Project\Data\Transmission Outages Verification\MappedOutagesVerificationResult.xlsx")
    # write the dataframe to an excel sheet
    result.to_excel(writer, 'Sheet1')
    # save the excel file
    writer.save()

# call to main function
# -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------


if __name__ == '__main__':
    main()


