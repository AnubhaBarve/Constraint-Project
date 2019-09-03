# Created on: August 27,2019
# Created by: Anubha Barve (anubha.barve@constellation.com - Intern 2019)
# Purpose - this file is created to separate the mapped and unmapped transmission outages from the Unmapped transmission Outages file. Also, verify the mapped outages by
# approximately matching the 'fromstation' and 'tostation' with the 'from bus name' and 'to bus name' of the mapping document.

#import statement
# -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
import pandas as pd
from fuzzywuzzy import fuzz
from tqdm import tqdm
import multiprocessing as mp
import numpy as np
from multiprocessing import Pool

# separationmapped function stores the mapped outages into a dataframe
def separationmapped(inputfile):

    # creating an empty dataframe
    mapped = pd.DataFrame()

    # storing mapped outages in the dataframe created above
    for i, r in tqdm(inputfile.iterrows()):
        if str(r['matching_code_mapped']) != " ":
            mapped = mapped.append(r)

    # creating new columns for storing results of approximate string matching
    mapped['from_matching'] = " "
    mapped['from_ratio'] = " "
    mapped['to_matching'] = " "
    mapped['to_ratio'] = " "

    # obtaining string approximate matching results for verfiying the mapping done.
    for index,row in tqdm(mapped.iterrows()):
                if fuzz.ratio(str(row['fromstation']),str(row['from_bus_name'])) >= fuzz.ratio(str(row['tostation']),str(row['from_bus_name'])):
                    mapped.at[index, 'from_ratio'] = str(fuzz.ratio(str(row['fromstation']),str(row['from_bus_name'])))
                    mapped.at[index, 'from_matching'] = str(row['fromstation'])

                else:
                    mapped.at[index, 'from_ratio'] = str(fuzz.ratio(str(row['tostation']),str(row['from_bus_name'])))
                    mapped.at[index, 'from_matching'] = str(row['tostation'])

                if fuzz.ratio(str(row['fromstation']),str(row['to_bus_name'])) >= fuzz.ratio(str(row['tostation']),str(row['to_bus_name'])):
                    mapped.at[index, 'to_ratio'] = str(fuzz.ratio(str(row['fromstation']),str(row['to_bus_name'])))
                    mapped.at[index, 'to_matching'] = str(row['fromstation'])

                else:
                    mapped.at[index, 'to_ratio'] = str(fuzz.ratio(str(row['tostation']),str(row['to_bus_name'])))
                    mapped.at[index, 'to_matching'] = str(row['tostation'])
    return mapped

# separationunmapped function stores the unmapped outages in a dataframe
def separationunmapped(inputfile):

    # creating an empty dataframe
    unmapped = pd.DataFrame()

    # inputfile1 stores the mapping document with line facilities in the form of a dataframe (Year 2019 September)
    inputfile1 = pd.read_excel(r"S:\asset ops\GO_Group\Interns\2019\Anubha\Constraint Project\Constraint-Project\Data\UniqueTransmissionOutagesMapped2014-2019.xlsx",sheet_name="AuctionMapping2019SEP_LINES", index=False)
    # Formatting the column names of excel sheet into one form to make it easier to access column names (all lower case letters with words separated using underscore)
    inputfile1.columns = inputfile1.columns.str.strip().str.lower().str.replace(' ', '_').str.replace('(','').str.replace(')', '')
    # preserves the index and converts into column
    inputfile1.reset_index(inplace=True, drop=True)

    # inputfile2 stores the mapping document with transformer facilities in the form of a dataframe (Year 2019 September)
    inputfile2 = pd.read_excel(r"S:\asset ops\GO_Group\Interns\2019\Anubha\Constraint Project\Constraint-Project\Data\UniqueTransmissionOutagesMapped2014-2019.xlsx",sheet_name="AuctionMapping2019SEP_AUTOS", index=False)
    # Formatting the column names of excel sheet into one form to make it easier to access column names (all lower case letters with words separated using underscore)
    inputfile2.columns = inputfile2.columns.str.strip().str.lower().str.replace(' ', '_').str.replace('(','').str.replace(')', '')
    # preserves the index and converts into a column
    inputfile2.reset_index(inplace=True, drop=True)

    # storing the unmapped transmission outages in a dataframe
    for index, row in tqdm(inputfile.iterrows()):
        if str(row['matching_code_mapped']) == " ":
            unmapped = unmapped.append(row)

    # approximate string matching implemented to see if unmapped outages can be mapped
    for index, row in tqdm(unmapped.iterrows()):
        c1 = 0
        c2 = 0
        if str(row['facility_type']) == 'LINE':
            for i1, r1 in inputfile1.iterrows():
                if fuzz.ratio(str(row['match']),str(r1['op_eqcode'])) >= 50:
                    c1 = c1 + 1
                    if c1 <= 5:
                        if str(r1['op_eqcode']) not in str(unmapped.at[index, 'matching_code']):
                            unmapped.at[index,'match_ratio'] = str(unmapped.at[index, 'match_ratio']) + str(fuzz.ratio(str(row['match']),str(r1['op_eqcode']))) + " * "
                            unmapped.at[index, 'match_partialratio'] = str(unmapped.at[index, 'match_partialratio']) + str(fuzz.partial_ratio(str(row['match']), str(r1['op_eqcode']))) + " * "
                            unmapped.at[index, 'match_sortratio'] = str(unmapped.at[index, 'match_sortratio']) + str(fuzz.token_sort_ratio(str(row['match']), str(r1['op_eqcode']))) + " * "
                            unmapped.at[index, 'match_setratio'] = str(unmapped.at[index, 'match_setratio']) + str(fuzz.token_set_ratio(str(row['match']), str(r1['op_eqcode']))) + " * "
                            unmapped.at[index, 'matching_code'] = str(unmapped.at[index, 'matching_code']) + str(r1['op_eqcode']) + " * "

        if str(row['facility_type']) == 'XFMR':
            for i1, r1 in inputfile2.iterrows():
                if fuzz.ratio(str(row['match']), str(r1['operations_name'])) >= 50:
                    c2 = c2 + 1
                    if c2 <= 5:
                        if str(r1['operations_name']) not in str(unmapped.at[index, 'matching_code']):
                            unmapped.at[index, 'match_ratio'] = str(unmapped.at[index, 'match_ratio']) + str(fuzz.ratio(str(row['match']), str(r1['operations_name']))) + " * "
                            unmapped.at[index, 'match_partialratio'] = str(unmapped.at[index, 'match_partialratio']) + str(fuzz.partial_ratio(str(row['match']), str(r1['operations_name']))) + " * "
                            unmapped.at[index, 'match_sortratio'] = str(unmapped.at[index, 'match_sortratio']) + str(fuzz.token_sort_ratio(str(row['match']), str(r1['operations_name']))) + " * "
                            unmapped.at[index, 'match_setratio'] = str(unmapped.at[index, 'match_setratio']) + str(fuzz.token_set_ratio(str(row['match']), str(r1['operations_name']))) + " * "
                            unmapped.at[index, 'matching_code'] = str(unmapped.at[index, 'matching_code']) + str(r1['operations_name']) + " * "

    return unmapped

# main function
# ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------


def main():

    # inputfile stores the transmission outages file which contains unmapped outages which are mapped manually
    inputfile = pd.read_excel(r"S:\asset ops\GO_Group\Interns\2019\Anubha\Constraint Project\Constraint-Project\Data\UnmappedTransmissionOutagesAprroximateStringMatch1.xlsx",sheet_name="unmappedAll", index=False)
    # Formatting the column names of excel sheet into one form to make it easier to access column names (all lower case letters with words separated using underscore)
    inputfile.columns = inputfile.columns.str.strip().str.lower().str.replace(' ', '_').str.replace('(', '').str.replace(')', '')
    # preserves index and converts into column
    inputfile.reset_index(inplace=True, drop=True)

    # parallel processing - to enable faster execution
    # this gives the number of processors available to use in the system
    cores = mp.cpu_count()
    # creating processes depending on the number of processors
    pool = Pool(cores)
    # splitting the dataframe into a number equivalent to the number of processors
    inputfile_split = np.array_split(inputfile, cores)
    # using map function for parallel execution and concatenating the result back to a dataframe
    result = pd.concat(pool.map(separationmapped, inputfile_split))
    # using map function for parallel execution and concatenating the result back to a dataframe
    result1 = pd.concat(pool.map(separationunmapped, inputfile_split))
    # closing all processes
    pool.close()
    # wait until all process finish
    pool.join()

    # creating an excel file to store the result
    writer = pd.ExcelWriter(r"S:\asset ops\GO_Group\Interns\2019\Anubha\Constraint Project\Constraint-Project\Data\UnmappedTransmissionOutagesAprroximateSeparation.xlsx")
    # writing the result to an excel sheet
    result.to_excel(writer, 'Sheet1')
    # writing the result to an excel sheet
    result1.to_excel(writer, 'Sheet2')
    # saving the excel file
    writer.save()

# call to main function
# ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------


if __name__ == '__main__':
    main()


