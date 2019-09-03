# Created on: August 28,2019
# Created by: Anubha Barve (anubha.barve@constellation.com - Intern 2019)
# File name: ConstraintContingencySeparation.py
# Purpose: This file is created to separate the mapped constraints, mapped contingency, unmapped constraints,
#          unmapped contingency and also create list of both mapped constraints and contingencies.

# import statements
import pandas as pd
from tqdm import tqdm
import multiprocessing as mp
import numpy as np
from multiprocessing import Pool
# Code starts
# ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

# function mappedconstraint stores the mapped constraints in a dataframe
def mappedconstraint(inputfile):

    # creating an empty dataframe
    constraints = pd.DataFrame()
    # storing the mapped constraints in the dataframe created above
    for i1,r1 in tqdm(inputfile.iterrows()):
        if str(r1['constraint_from_bus_number']) != " ":
            constraints = constraints.append(r1)
    return constraints

# function mappedcontingency stores the mapped contingency in a dataframe
def mappedcontingecny(inputfile):

    # creating an empty dataframe
    contingency = pd.DataFrame()
    # storing the mapped constraints in the dataframe created above
    for i1, r1 in tqdm(inputfile.iterrows()):
        if str(r1['contingency_from_bus_number']) != " ":
            contingency = contingency.append(r1)
    return contingency

# function unmappedconstraint stores the unmapped constraints in a dataframe
def unmappedconstraint(inputfile):

    # creating an empty dataframe
    constraints = pd.DataFrame()
    # storing the unmapped constraints in the dataframe created above
    for i1, r1 in tqdm(inputfile.iterrows()):
        if str(r1['constraint_from_bus_number']) == " ":
            constraints = constraints.append(r1)
    return constraints

# function unmappedcontingency stores the unmapped contingency in a dataframe
def unmappedcontingency(inputfile):

    # creating an empty dataframe
    contingency = pd.DataFrame()
    # storing unmapped contingency except the 'BASE CASE' contingencies in the dataframe created above
    for i1, r1 in tqdm(inputfile.iterrows()):
        if str(r1['contingency']) != "BASE CASE":
            if str(r1['contingency_from_bus_number']) == " ":
                contingency = contingency.append(r1)
    return contingency

# function mappedboth stores mapped constraints and mapped contingency in a dataframe
def mappedboth(inputfile):

    # creating an empty dataframe
    mapped = pd.DataFrame()

    # storing mapped constraint and contingency including 'BASE CASE' contingency as well in the dataframe created above
    for i1, r1 in tqdm(inputfile.iterrows()):
        if str(r1['constraint_from_bus_number']) != " " and str(r1['contingency_from_bus_number']) != " ":
            mapped = mapped.append(r1)
        if str(r1['contingency']) == "BASE CASE" and str(r1['constraint_from_bus_number']) != " ":
            mapped = mapped.append(r1)
    return mapped

# main function
# ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------


def main():
    # inputfile stores the unique constraint=contingency pairs from the year 2014 to 2019.
    inputfile = pd.read_excel(r"S:\asset ops\GO_Group\Interns\2019\Anubha\Constraint Project\Constraint-Project\Data\UniqueConstraintContingencyPair\uniquePairList2014-2019NewChanged.xlsx", sheet_name="Constraint-Contingency", index= False)
    # Formatting the column names of excel sheet into one form to make it easier to access column names (all lower case letters with words separated using underscore)
    inputfile.columns = inputfile.columns.str.strip().str.lower().str.replace(' ', '_').str.replace('(','').str.replace(')', '')
    # preserves the index and converts into a column
    inputfile.reset_index(inplace=True, drop=True)

    # parallel processing - to enable faster execution
    # this gives the number of processors available to use in the system
    cores = mp.cpu_count()
    # creating processes depending on the number of processors
    pool = Pool(cores)
    # splitting the inputfile dataframe into parts equivalent to the number of processes created
    inputfile_split = np.array_split(inputfile,cores)
    # call to mappedconstraint function and concatenating all resulting dataframes into one
    result = pd.concat(pool.map(mappedconstraint, inputfile_split))
    # call to mappedcontingency function and concatenating all resulting dataframes into one
    result1 = pd.concat(pool.map(mappedcontingecny, inputfile_split))
    # call to unmappedconstraint function and concatenating all resulting dataframes into one
    result2 = pd.concat(pool.map(unmappedconstraint, inputfile_split))
    # call to unmappedcontingency function and concatenating all resulting dataframes into one
    result3 = pd.concat(pool.map(unmappedcontingency, inputfile_split))
    # call to mappedboth fucntion and concatenating all resulting dataframes into one
    result4 = pd.concat(pool.map(mappedboth, inputfile_split))
    # closing all processes
    pool.close()
    # wait until all processes are finished
    pool.join()

    # creating an excel file to store the resultant dataframes
    writer = pd.ExcelWriter(r"S:\asset ops\GO_Group\Interns\2019\Anubha\Constraint Project\Constraint-Project\Data\UniqueConstraintContingencyPair\uniquePairList2014-2019Separation.xlsx")
    # writing the resulting dataframe obtained when mappedconstraint function is called to an excel sheet
    result.to_excel(writer,'Sheet1')
    # writing the resulting dataframe obtained when mappedcontingency function is called to an excel sheet
    result1.to_excel(writer,'Sheet2')
    # writing the resulting dataframe obtained when unmappedcontraint function is called to an excel sheet
    result2.to_excel(writer, 'Sheet3')
    # writing the resulting dataframe obtained when unmappedcontingency function is called to an excel sheet
    result3.to_excel(writer, 'Sheet4')
    # writing the resulting dataframe obtained when mappedboth function is called to an excel sheet
    result4.to_excel(writer, 'Sheet5')
    # saving the excel file
    writer.save()

# call to main function
# ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------


if __name__ == "__main__":
    main()
