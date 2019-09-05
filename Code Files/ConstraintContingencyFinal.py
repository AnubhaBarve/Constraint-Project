# Created on: September 04,2019
# Created by: Anubha Barve (anubha.barve@constellation.com - Intern 2019)
# File name: ConstraintContingencyFinal.py
# Purpose: This file is created to generate a final separated list of the mapped constraints, mapped contingency, unmapped constraints,
#          unmapped contingency and also create list of both mapped constraints and contingencies.

# import statements
# ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
import pandas as pd
from tqdm import tqdm
import multiprocessing as mp
import numpy as np
from multiprocessing import Pool
# Code starts
# ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

# function mappedconstraint stores the mapped constraints in a dataframe
def mappedconstraint(inputfile, input):

    # creating an empty dataframe
    constraints = pd.DataFrame()
    # storing the mapped constraints in the dataframe created above
    for i1,r1 in tqdm(input.iterrows()):
        if str(r1['constraint_from_bus_number']) != " ":
            constraints = constraints.append(r1)
    # adding a column to show that mapping is done manually for which verification was not possible
    constraints['verification'] = "no"
    # merging the previously mapped constraints with the newly mapped constraints
    new_constraints = inputfile.merge(constraints, how='outer')
    # dropping columns with NaN value
    new_constraints = new_constraints.dropna(axis='columns', how='any', thresh=576)
    # returning the final dataframe
    return new_constraints

# function unmappedconstraint stores the unmapped constraints in a dataframe
def unmappedconstraint(inputfile):

    # creating an empty dataframe
    constraints = pd.DataFrame()
    # storing the unmapped constraints in the dataframe created above
    for i1, r1 in tqdm(inputfile.iterrows()):
        if str(r1['constraint_from_bus_number']) == " ":
            constraints = constraints.append(r1)
    # returning the final dataframe
    return constraints

# function mappedboth stores mapped constraints and mapped contingency in a dataframe
def mappedboth(inputfile, input):

    # creating an empty dataframe
    mapped = pd.DataFrame()

    # storing mapped constraint and contingency including 'BASE CASE' contingency as well in the dataframe created above
    for i1, r1 in tqdm(input.iterrows()):
        if str(r1['constraint_from_bus_number']) != " " and str(r1['contingency_from_bus_number']) != " ":
            mapped = mapped.append(r1)
        if str(r1['contingency']) == "BASE CASE" and str(r1['constraint_from_bus_number']) != " ":
            mapped = mapped.append(r1)
    # adding a column to show that mapping was done manually and verification was not possible
    mapped['verification'] = "no"
    # merging the previously mapped constraint-contingency pairs with newly mapped pairs
    new_mapped = inputfile.merge(mapped, how='outer')
    # dropping columns with NaN value
    new_mapped = new_mapped.dropna(axis='columns', how='any', thresh= 576)
    # returning the final dataframe
    return new_mapped

# main function
# ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------


def main():
    # inputfile1 stores the unique constraint-contingency pairs from the year 2014 to 2019 where constraints are mapped
    inputfile1 = pd.read_excel(r"S:\asset ops\GO_Group\Interns\2019\Anubha\Constraint Project\Constraint-Project\Data\UniqueConstraintContingencyPair\uniquePairList2014-2019Separation.xlsx", sheet_name="mappedConstraints", index= False)
    # Formatting the column names of excel sheet into one form to make it easier to access column names (all lower case letters with words separated using underscore)
    inputfile1.columns = inputfile1.columns.str.strip().str.lower().str.replace(' ', '_').str.replace('(','').str.replace(')', '')
    # preserves the index and converts into a column
    inputfile1.reset_index(inplace=True, drop=True)
    # adding a column showing that mapping was done using vlookup
    inputfile1['verification'] = "yes"

    # inputfile2 stores the unique constraint-contingency pairs from the year 2014 to 2019 where both constraint-contingency pair is mapped
    inputfile2 = pd.read_excel(r"S:\asset ops\GO_Group\Interns\2019\Anubha\Constraint Project\Constraint-Project\Data\UniqueConstraintContingencyPair\uniquePairList2014-2019Separation.xlsx",sheet_name="mappedBoth", index=False)
    # Formatting the column names of excel sheet into one form to make it easier to access column names (all lower case letters with words separated using underscore)
    inputfile2.columns = inputfile2.columns.str.strip().str.lower().str.replace(' ', '_').str.replace('(','').str.replace(')','')
    # preserves the index and converts into a column
    inputfile2.reset_index(inplace=True, drop=True)
    # adding a column showing that mapping was done using vlookup
    inputfile2['verification'] = "yes"

    # result1 stores the unique constraint-contingency pairs from the year 2014 to 2019 where contingencies are mapped
    result1 = pd.read_excel(r"S:\asset ops\GO_Group\Interns\2019\Anubha\Constraint Project\Constraint-Project\Data\UniqueConstraintContingencyPair\uniquePairList2014-2019Separation.xlsx", sheet_name="mappedContingency", index=False)
    # Formatting the column names of excel sheet into one form to make it easier to access column names (all lower case letters with words separated using underscore)
    result1.columns = result1.columns.str.strip().str.lower().str.replace(' ', '_').str.replace('(','').str.replace(')', '')
    # preserves the index and converts into a column
    result1.reset_index(inplace=True, drop=True)

    # result3 stores the unique constraint=contingency pairs from the year 2014 to 2019 where contingencies are not mapped
    result3 = pd.read_excel(r"S:\asset ops\GO_Group\Interns\2019\Anubha\Constraint Project\Constraint-Project\Data\UniqueConstraintContingencyPair\uniquePairList2014-2019Separation.xlsx", sheet_name="unmappedContingency", index=False)
    # Formatting the column names of excel sheet into one form to make it easier to access column names (all lower case letters with words separated using underscore)
    result3.columns = result3.columns.str.strip().str.lower().str.replace(' ', '_').str.replace('(', '').str.replace(')', '')
    # preserves the index and converts into a column
    result3.reset_index(inplace=True, drop=True)

    # inputfile3 stores the unique unmapped constraint-contingency pairs from the year 2014 to 2019
    inputfile3 = pd.read_excel(r"S:\asset ops\GO_Group\Interns\2019\Anubha\Constraint Project\Constraint-Project\Data\UniqueConstraintContingencyPair\UnmappedConstraintsApproximateStringMatching.xlsx",sheet_name="All", index=False)
    # Formatting the column names of excel sheet into one form to make it easier to access column names (all lower case letters with words separated using underscore)
    inputfile3.columns = inputfile3.columns.str.strip().str.lower().str.replace(' ', '_').str.replace('(','').str.replace(')','')
    # preserves the index and converts into a column
    inputfile3.reset_index(inplace=True, drop=True)

    # call to mappedconstraint function
    result = mappedconstraint(inputfile1, inputfile3)
    # call to unmappedconstraint function
    result2 = unmappedconstraint(inputfile3)
    # call to mappedboth fucntion
    result4 = mappedboth(inputfile2, inputfile3)

    # creating an excel file to store the resultant dataframes
    writer = pd.ExcelWriter(r"S:\asset ops\GO_Group\Interns\2019\Anubha\Constraint Project\Constraint-Project\Data\UniqueConstraintContingencyPair\Constraint-ContingencyFinal.xlsx")
    # writing the resulting dataframe obtained when mappedconstraint function is called to an excel sheet
    result.to_excel(writer,'mappedConstraints')
    # writing the resulting dataframe obtained when mappedcontingency function is called to an excel sheet
    result1.to_excel(writer,'mappedContingency')
    # writing the resulting dataframe obtained when unmappedconstraint function is called to an excel sheet
    result2.to_excel(writer, 'unmappedConstraints')
    # writing the resulting dataframe obtained when unmappedcontingency function is called to an excel sheet
    result3.to_excel(writer, 'unmappedContingency')
    # writing the resulting dataframe obtained when mappedboth function is called to an excel sheet
    result4.to_excel(writer, 'mappedBoth')
    # saving the excel file
    writer.save()

# call to main function
# ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------


if __name__ == "__main__":
    main()
