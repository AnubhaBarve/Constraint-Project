# Created on: September 05,2019
# Created by: Anubha Barve (anubha.barve@constellation.com - Intern 2019)
# File name: TransmissionOutagesFinal.py
# Purpose: This file is created to generate a final separated list of the mapped outages and unmapped outages.

# import statements
# ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
import pandas as pd
from tqdm import tqdm
import multiprocessing as mp
import numpy as np
from multiprocessing import Pool
# Code starts
# ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

# function mappedoutages stores the mapped outages in a dataframe
def mappedoutages(inputfile, input):

    # creating an empty dataframe
    outages = pd.DataFrame()
    # storing the mapped outages in the dataframe created above
    for i1,r1 in tqdm(input.iterrows()):
        if str(r1['from_bus_number']) != " ":
            outages = outages.append(r1)
    # adding a column to show that this mapping was done manually
    outages['verification'] = "manual"
    # merging the already mapped outages with manually mapped outages
    new_outages = inputfile.merge(outages, how='outer')
    # dropping columns with NaN values
    new_outages = new_outages.dropna(axis='columns', how='any', thresh=452)
    # dropping column 'reported_name_duplicate'
    new_outages = new_outages.drop(['reported_name_duplicate'], axis=1)
    # returning the final dataframe
    return new_outages

# function unmappedoutages stores the unmapped outages in a dataframe
def unmappedoutages(inputfile):

    # creating an empty dataframe
    outages = pd.DataFrame()
    # storing the unmapped outages in the dataframe created above
    for i1, r1 in tqdm(inputfile.iterrows()):
        if str(r1['from_bus_number']) == " ":
            outages = outages.append(r1)
    # dropping columns that are not required
    outages = outages.drop(['match_partialratio', 'match_ratio', 'match_setratio', 'match_sortratio','match', 'matching_code', 'matching_code_mapped', 'open_close', 'reliability_score', 'reliability_accuracy', 'reported_name_duplicate'], axis=1)
    # returning final dataframe
    return outages

# main function
# ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------


def main():
    # inputfile1 stores the unique transmission outages from the year 2014 to 2019.
    inputfile1 = pd.read_excel(r"S:\asset ops\GO_Group\Interns\2019\Anubha\Constraint Project\Constraint-Project\Data\UniqueTransmissionOutagesMapped2014-2019.xlsx", sheet_name="Mapped", index= False)
    # Formatting the column names of excel sheet into one form to make it easier to access column names (all lower case letters with words separated using underscore)
    inputfile1.columns = inputfile1.columns.str.strip().str.lower().str.replace(' ', '_').str.replace('(','').str.replace(')', '')
    # preserves the index and converts into a column
    inputfile1.reset_index(inplace=True, drop=True)
    # adding a column to show that the mapping was done using vlookup
    inputfile1['verification'] = "vlookup"

    # inputfile2 stores the unmapped transmission outages from the year 2014 to 2019 which were mapped manually
    inputfile2 = pd.read_excel(r"S:\asset ops\GO_Group\Interns\2019\Anubha\Constraint Project\Constraint-Project\Data\UnmappedTransmissionOutagesAprroximateSeparation.xlsx",sheet_name="mapped", index=False)
    # Formatting the column names of excel sheet into one form to make it easier to access column names (all lower case letters with words separated using underscore)
    inputfile2.columns = inputfile2.columns.str.strip().str.lower().str.replace(' ', '_').str.replace('(','').str.replace(')','')
    # preserves the index and converts into a column
    inputfile2.reset_index(inplace=True, drop=True)

    # inputfile3 stores the unmapped transmission outages from the year 2014 to 2019 which didn't map manually as well
    inputfile3 = pd.read_excel(r"S:\asset ops\GO_Group\Interns\2019\Anubha\Constraint Project\Constraint-Project\Data\UnmappedTransmissionOutagesAprroximateSeparation.xlsx",sheet_name="unmapped", index=False)
    # Formatting the column names of excel sheet into one form to make it easier to access column names (all lower case letters with words separated using underscore)
    inputfile3.columns = inputfile3.columns.str.strip().str.lower().str.replace(' ', '_').str.replace('(','').str.replace(')','')
    # preserves the index and converts into a column
    inputfile3.reset_index(inplace=True, drop=True)

    # call to mappedoutages function
    result = mappedoutages(inputfile1, inputfile2)
    # call to unmappedoutages function
    result2 = unmappedoutages(inputfile3)

    # creating an excel file to store the resultant dataframes
    writer = pd.ExcelWriter(r"S:\asset ops\GO_Group\Interns\2019\Anubha\Constraint Project\Constraint-Project\Data\TransmissionOutagesFinal.xlsx")
    # writing the resulting dataframe obtained when mappedoutages function is called to an excel sheet
    result.to_excel(writer,'mapped')
    # writing the resulting dataframe obtained when unmappedoutages function is called to an excel sheet
    result2.to_excel(writer, 'unmapped')
    # saving the excel file
    writer.save()

# call to main function
# ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------


if __name__ == "__main__":
    main()
