# Created on: August 20,2019
# Created by: Anubha Barve (anubha.barve@constellation.com - Intern 2019)
# Purpose - this file is created to perform approximate similarity matching of the unmapped transmission outages with the 'operations_name' and 'op_eqcode'
#           columns of autos  and lines sheets respectively of the auction mapping excel file.

# -------------------------------------------------------------------------------------------------------------------------------------------------------------

# import statements
# -------------------------------------------------------------------------------------------------------------------------------------------------------------
import pandas as pd
from fuzzywuzzy import fuzz
from tqdm import tqdm
import multiprocessing as mp
import numpy as np
from multiprocessing import Pool

# code starts
# --------------------------------------------------------------------------------------------------------------------------------------------------------------

# function approximatematching takes 'inputfile' as parameter which stores the unmapped transmission outages in the form of a dtaframe


def approximatematching(inputfile):

    # inputfile1 stores the mapping document for line facilities for the year 2019 September as a dataframe
    inputfile1 = pd.read_excel(r"S:\asset ops\GO_Group\Interns\2019\Anubha\Constraint Project\Constraint-Project\Data\UniqueTransmissionOutagesMapped2014-2019.xlsx", sheet_name="AuctionMapping2019SEP_LINES", index=False)
    # column names are formatted by changing them into lower case, replacing the spaces with underscore and brackets are removed, if any
    inputfile1.columns = inputfile1.columns.str.strip().str.lower().str.replace(' ', '_').str.replace('(','').str.replace(')', '')
    # this line preserves the index and converts it into a column
    inputfile1.reset_index(inplace=True, drop=True)

    # inputfile2 stores the mapping document for transformer facilities for the year 2019 September as a dataframe
    inputfile2 = pd.read_excel(r"S:\asset ops\GO_Group\Interns\2019\Anubha\Constraint Project\Constraint-Project\Data\UniqueTransmissionOutagesMapped2014-2019.xlsx", sheet_name="AuctionMapping2019SEP_AUTOS", index=False)
    # column names are formatted by changing them into lower case, replacing the spaces with underscore and brackets are removed, if any
    inputfile2.columns = inputfile2.columns.str.strip().str.lower().str.replace(' ', '_').str.replace('(','').str.replace(')', '')
    # this line preserves the index and converts it into a column
    inputfile2.reset_index(inplace=True, drop=True)

    # creating new columns in the unmapped transmission outages dataframe to store similarity percentages of the matched 'operations_name' or 'op_eqcode'
    # and also store the matched 'operations_name' or 'op_eqcode'
    inputfile['matching_code'] = " "
    # different functions of fuzzywuzzy library are used and hence different columns for each function is created
    inputfile['match_ratio'] = " "
    inputfile['match_partialratio'] = " "
    inputfile['match_sortratio'] = " "
    inputfile['match_setratio'] = " "

    # Looping through the inputfile dataframe which stores the unmapped transmission outages
    for index, row in tqdm(inputfile.iterrows()):
        c1 = 0
        c2 = 0

        # approximate string matching code for transformer facilities
        if str(row['facility_type']) == 'LINE':
            for i1, r1 in inputfile1.iterrows():
                # 5 unique 'op_eqcode' are stored which have a string matching greater than or equal to 80%
                if fuzz.ratio(str(row['match']),str(r1['op_eqcode'])) >= 80:
                    c1 = c1 + 1
                    if c1 <= 5:
                        if str(r1['op_eqcode']) not in str(inputfile.at[index, 'matching_code']):
                            inputfile.at[index,'match_ratio'] = str(inputfile.at[index, 'match_ratio']) + str(fuzz.ratio(str(row['match']),str(r1['op_eqcode']))) + " * "
                            inputfile.at[index, 'match_partialratio'] = str(inputfile.at[index, 'match_partialratio']) + str(fuzz.partial_ratio(str(row['match']), str(r1['op_eqcode']))) + " * "
                            inputfile.at[index, 'match_sortratio'] = str(inputfile.at[index, 'match_sortratio']) + str(fuzz.token_sort_ratio(str(row['match']), str(r1['op_eqcode']))) + " * "
                            inputfile.at[index, 'match_setratio'] = str(inputfile.at[index, 'match_setratio']) + str(fuzz.token_set_ratio(str(row['match']), str(r1['op_eqcode']))) + " * "
                            inputfile.at[index, 'matching_code'] = str(inputfile.at[index, 'matching_code']) + str(r1['op_eqcode']) + " * "

        # approximate string matching code for transformer facilities
        if str(row['facility_type']) == 'XFMR':
            for i1, r1 in inputfile2.iterrows():
                # 5 unique 'op_eqcode' are stored which have a string matching greater than or equal to 80%
                if fuzz.ratio(str(row['match']), str(r1['operations_name'])) >= 80:
                    c2 = c2 + 1
                    if c2 <= 5:
                        if str(r1['operations_name']) not in str(inputfile.at[index, 'matching_code']):
                            inputfile.at[index, 'match_ratio'] = str(inputfile.at[index, 'match_ratio']) + str(fuzz.ratio(str(row['match']), str(r1['operations_name']))) + " * "
                            inputfile.at[index, 'match_partialratio'] = str(inputfile.at[index, 'match_partialratio']) + str(fuzz.partial_ratio(str(row['match']), str(r1['operations_name']))) + " * "
                            inputfile.at[index, 'match_sortratio'] = str(inputfile.at[index, 'match_sortratio']) + str(fuzz.token_sort_ratio(str(row['match']), str(r1['operations_name']))) + " * "
                            inputfile.at[index, 'match_setratio'] = str(inputfile.at[index, 'match_setratio']) + str(fuzz.token_set_ratio(str(row['match']), str(r1['operations_name']))) + " * "
                            inputfile.at[index, 'matching_code'] = str(inputfile.at[index, 'matching_code']) + str(r1['operations_name']) + " * "

    return inputfile

# main function
# ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------


def main():

    # inputfile stores the unmappped transmission outages from the year 2014-2019 in the form of a dataframe
    inputfile = pd.read_excel(r"S:\asset ops\GO_Group\Interns\2019\Anubha\Constraint Project\Constraint-Project\Data\UniqueTransmissionOutagesMapped2014-2019.xlsx",sheet_name="Unmapped", index=False)
    # column names are formatted by changing them into lower case, replacing the spaces with underscore and brackets are removed, if any
    inputfile.columns = inputfile.columns.str.strip().str.lower().str.replace(' ', '_').str.replace('(', '').str.replace(')', '')
    # this line preserves the index and converts it into a column
    inputfile.reset_index(inplace=True, drop=True)

    # parallel processing - to enable faster execution
    # this gives the number of processors available to use in the system
    cores = mp.cpu_count()
    # creating processes depending on the number of processors
    pool = Pool(cores)
    # splitting the dataframe into a number equivalent to the number of processors
    inputfile_split = np.array_split(inputfile, cores)
    # using map function for parallel execution and concatenating the result back to a dataframe
    result = pd.concat(pool.map(approximatematching, inputfile_split))
    # closing all processes
    pool.close()
    # joining all processes
    pool.join()

    # creating an excel file to store the result
    writer = pd.ExcelWriter(r"S:\asset ops\GO_Group\Interns\2019\Anubha\Constraint Project\Constraint-Project\Data\UnmappedTransmissionOutagesAprroximateStringMatch.xlsx")
    # writing the result to an excel sheet
    result.to_excel(writer, 'unmappedAll')
    # saving the excel file
    writer.save()

# call to main function
# ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------


if __name__ == '__main__':
    main()


