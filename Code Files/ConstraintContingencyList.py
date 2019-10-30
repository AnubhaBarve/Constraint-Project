# Created on: September 16, 2019
# Created by: Anubha Barve (anubha.barve@constellation - Intern 2019)
# File name: ConstraintContingencyList.py
# Purpose: This file is developed to create a super-list of constraint-contingency pairs for all the
# years which are mapped.

# -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

import pandas as pd
from tqdm import tqdm
import pandas as pd
from tqdm import tqdm
import multiprocessing as mp
import numpy as np
from multiprocessing import Pool

# -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
# code starts

# the function combine is used to pull only mapped constraints from the given dataframe


def combine(df):

    # finaloutput_all is an empty dataframe created to store the final output
    finaloutput_all = pd.DataFrame()
    # Iterating the df dataframe row-wise (where tqdm displays progress of the loop during execution)
    for i1, r1 in tqdm(df.iterrows()):
        if str(r1['constraint_from_bus_number']) != " ":
            finaloutput_all = finaloutput_all.append(r1)
    return finaloutput_all

# main function


def main():
    # final_file is a string that stores the name of the constraint-contingency hourly data for all years file
    final_file = r"S:\asset ops\GO_Group\Interns\2019\Anubha\Constraint Project\Constraint-Project\Data\UniqueConstraintContingencyPair\ConstraintContingencyList.xlsx"

    # dataFrame14 reads the excel file whose name is stored in final_file as a dataframe
    dataFrame14 = pd.read_excel(final_file, sheet_name="2014", index=False)
    # Formatting the column names of excel sheet into one form to make it easier to
    # access column names (all lower case letters with words separated using underscore)
    dataFrame14.columns = dataFrame14.columns.str.strip().str.lower().str.replace(' ', '_').str.replace('(','').str.replace(')', '')
    # this line preserves the index and converts it into a column
    dataFrame14.reset_index(inplace=True, drop=True)
    # cores stores the value of the number of processors available in the system
    cores = mp.cpu_count()
    # dataframe_14 is split into chunks for parallel processing
    dataFrame14_split = np.array_split(dataFrame14, cores)
    # creating process pools equivalent to processors available
    pool = Pool(cores)
    # call to combine function
    result14 = pd.concat(pool.map(combine, dataFrame14_split))
    # dropping duplicate rows in resultant dataframe
    resultmapped14 = result14.drop_duplicates(subset=['facilityname', 'datetime'], keep='first', inplace=False)
    # closing all pools
    pool.close()
    pool.join()

    # dataFrame15 reads the excel file whose name is stored in year_2015 as a dataframe
    dataFrame15 = pd.read_excel(final_file, sheet_name="2015", index=False)
    # Formatting the column names of excel sheet into one form to make it easier to access column names (all lower case letters with words separated using underscore)
    dataFrame15.columns = dataFrame15.columns.str.strip().str.lower().str.replace(' ', '_').str.replace('(','').str.replace(')', '')
    # this line preserves the index and converts it into a column
    dataFrame15.reset_index(inplace=True, drop=True)
    # cores stores the value of the number of processors available in the system
    cores = mp.cpu_count()
    # dataframe_15 is split into chunks for parallel processing
    dataFrame15_split = np.array_split(dataFrame15, cores)
    # creating process pools equivalent to processors available
    pool = Pool(cores)
    # call to combine function
    result15 = pd.concat(pool.map(combine, dataFrame15_split))
    # dropping duplicate rows in resultant dataframe
    resultmapped15 = result15.drop_duplicates(subset=['facilityname', 'datetime'], keep='first', inplace=False)
    # closing all pools
    pool.close()
    pool.join()

    # dataFrame16 reads the excel file whose name is stored in year_2016 as a dataframe
    dataFrame16 = pd.read_excel(final_file, sheet_name="2016", index=False)
    # Formatting the column names of excel sheet into one form to make it easier to access column names (all lower case letters with words separated using underscore)
    dataFrame16.columns = dataFrame16.columns.str.strip().str.lower().str.replace(' ', '_').str.replace('(','').str.replace(')', '')
    # this line preserves the index and converts it into a column
    dataFrame16.reset_index(inplace=True, drop=True)
    # cores stores the value of the number of processors available in the system
    cores = mp.cpu_count()
    # dataframe_16 is split into chunks for parallel processing
    dataFrame16_split = np.array_split(dataFrame16, cores)
    # creating process pools equivalent to processors available
    pool = Pool(cores)
    # call to combine function
    result16 = pd.concat(pool.map(combine, dataFrame16_split))
    # dropping duplicate rows in resultant dataframe
    resultmapped16 = result16.drop_duplicates(subset=['facilityname', 'datetime'], keep='first', inplace=False)
    # closing all pools
    pool.close()
    pool.join()

    # dataFrame17 reads the excel file whose name is stored in year_2017 as a dataframe
    dataFrame17 = pd.read_excel(final_file, sheet_name="2017", index=False)
    # Formatting the column names of excel sheet into one form to make it easier to access column names (all lower case letters with words separated using underscore)
    dataFrame17.columns = dataFrame17.columns.str.strip().str.lower().str.replace(' ', '_').str.replace('(','').str.replace(')', '')
    # this line preserves the index and converts it into a column
    dataFrame17.reset_index(inplace=True, drop=True)
    # cores stores the value of the number of processors available in the system
    cores = mp.cpu_count()
    # dataframe_17 is split into chunks for parallel processing
    dataFrame17_split = np.array_split(dataFrame17, cores)
    # creating process pools equivalent to processors available
    pool = Pool(cores)
    # call to combine function
    result17 = pd.concat(pool.map(combine, dataFrame17_split))
    # dropping duplicate rows in resultant dataframe
    resultmapped17 = result17.drop_duplicates(subset=['facilityname', 'datetime'], keep='first', inplace=False)
    # closing all pools
    pool.close()
    pool.join()

    # dataFrame18 reads the excel file whose name is stored in year_2018 as a dataframe
    dataFrame18 = pd.read_excel(final_file, sheet_name="2018", index=False)
    # Formatting the column names of excel sheet into one form to make it easier to access column names (all lower case letters with words separated using underscore)
    dataFrame18.columns = dataFrame18.columns.str.strip().str.lower().str.replace(' ', '_').str.replace('(','').str.replace(')', '')
    # this line preserves the index and converts it into a column
    dataFrame18.reset_index(inplace=True, drop=True)
    # cores stores the value of the number of processors available in the system
    cores = mp.cpu_count()
    dataFrame18_split = np.array_split(dataFrame18, cores)
    # creating process pools equivalent to processors available
    pool = Pool(cores)
    # call to combine function
    result18 = pd.concat(pool.map(combine, dataFrame18_split))
    # dropping duplicate rows in resultant dataframe
    resultmapped18 = result18.drop_duplicates(subset=['facilityname', 'datetime'], keep='first', inplace=False)
    # closing all pools
    pool.close()
    pool.join()

    # dataFrame19 reads the excel file whose name is stored in year_2019 as a dataframe
    dataFrame19 = pd.read_excel(final_file, sheet_name="2019", index=False)
    # Formatting the column names of excel sheet into one form to make it easier to access column names (all lower case letters with words separated using underscore)
    dataFrame19.columns = dataFrame19.columns.str.strip().str.lower().str.replace(' ', '_').str.replace('(','').str.replace(')', '')
    # this line preserves the index and converts it into a column
    dataFrame19.reset_index(inplace=True, drop=True)
    # cores stores the value of the number of processors available in the system
    cores = mp.cpu_count()
    dataFrame19_split = np.array_split(dataFrame19, cores)
    # creating process pools equivalent to processors available
    pool = Pool(cores)
    # call to combine function
    result19 = pd.concat(pool.map(combine, dataFrame19_split))
    # dropping duplicate rows in resultant dataframe
    resultmapped19 = result19.drop_duplicates(subset=['facilityname', 'datetime'], keep='first', inplace=False)
    # closing all pools
    pool.close()
    pool.join()

    # creating an excel sheet
    writer = pd.ExcelWriter(r"S:\asset ops\GO_Group\Interns\2019\Anubha\Constraint Project\Constraint-Project\Data\UniqueConstraintContingencyPair\ConstraintContingencyFinalList.xlsx")
    # writing to the created excel file
    resultmapped14.to_excel(writer, '2014')
    resultmapped15.to_excel(writer, '2015')
    resultmapped16.to_excel(writer, '2016')
    resultmapped17.to_excel(writer, '2017')
    resultmapped18.to_excel(writer, '2018')
    resultmapped19.to_excel(writer, '2019')
    # saving the excel file
    writer.save()

# call to main function


if __name__ == '__main__':
    main()


