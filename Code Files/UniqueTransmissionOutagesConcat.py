# Created on: August 05, 2019
# Created by: Anubha Barve (anubha.barve@constellation - Intern 2019)
# File name: UniqueTransmissionOutagesConcat.py
# Purpose: This file is developed to create a super-list of transmission outages i.e combine unique transmission outages of all years into one file which are mapped (for now only storing
# data for line and transformer facilities)

import pandas as pd
from tqdm import tqdm

import pandas as pd
from tqdm import tqdm
import multiprocessing as mp
import numpy as np
from multiprocessing import Pool

# This class define a function named concatenate which combines the unique transmission outages that are mapped of all years
#class TransmissionOutagesConcatenation():
def concatenateall(df):
    # finalOutages is an empty dataframe created to store the final output
    finalOutages_all = pd.DataFrame()

    # Iterating the dataFrame14 dataframe row-wise (where tqdm displays progress of the loop during execution)
    for i1, r1 in tqdm(df.iterrows()):
        # Storing only those transmission outages that are mapped i.e. the ones that have a value for 'from_bus_number' column
        if str(r1['facility_type']) == 'LINE' or str(r1['facility_type']) == 'XFMR':
            finalOutages_all = finalOutages_all.append(r1)
    return finalOutages_all

def concatenatemapped(df):
    # finalOutages is an empty dataframe created to store the final output
    finalOutages_mapped = pd.DataFrame()

    # Iterating the dataFrame14 dataframe row-wise (where tqdm displays progress of the loop during execution)
    for i1, r1 in tqdm(df.iterrows()):
        # Storing only those transmission outages that are mapped i.e. the ones that have a value for 'from_bus_number' column
        if str(r1['facility_type']) == 'LINE' or str(r1['facility_type']) == 'XFMR':
            if str(r1['from_bus_number']) != " ":
                finalOutages_mapped = finalOutages_mapped.append(r1)
    return finalOutages_mapped


def concatenateunmapped(df):
    # finalOutages is an empty dataframe created to store the final output
    finalOutages_unmapped = pd.DataFrame()

    # Iterating the dataFrame14 dataframe row-wise (where tqdm displays progress of the loop during execution)
    for i1, r1 in tqdm(df.iterrows()):
        if str(r1['facility_type']) == 'LINE' or str(r1['facility_type']) == 'XFMR':
            if str(r1['from_bus_number']) == " ":
                finalOutages_unmapped = finalOutages_unmapped.append(r1)
    return finalOutages_unmapped


# main function which calls the class's function named 'concatenate()'
def main():
    # year_2014 is a string that stores the name of the unique transmission outage file for the year 2014
    year_2014 = r"S:\asset ops\GO_Group\Interns\2019\Anubha\Constraint Project\Constraint-Project\Data\UniqueTransmissionOutagesMapped2014.xlsx"

    # year_2015 is a string that stores the name of the unique transmission outage file for the year 2015
    year_2015 = r"S:\asset ops\GO_Group\Interns\2019\Anubha\Constraint Project\Constraint-Project\Data\UniqueTransmissionOutagesMapped2015.xlsx"

    # year_2016 is a string that stores the name of the unique transmission outage file for the year 2016
    year_2016 = r"S:\asset ops\GO_Group\Interns\2019\Anubha\Constraint Project\Constraint-Project\Data\UniqueTransmissionOutagesMapped2016.xlsx"

    # year_2017 is a string that stores the name of the unique transmission outage file for the year 2017
    year_2017 = r"S:\asset ops\GO_Group\Interns\2019\Anubha\Constraint Project\Constraint-Project\Data\UniqueTransmissionOutagesMapped2017.xlsx"

    # year_2018 is a string that stores the name of the unique transmission outage file for the year 2018
    year_2018 = r"S:\asset ops\GO_Group\Interns\2019\Anubha\Constraint Project\Constraint-Project\Data\UniqueTransmissionOutagesMapped2018.xlsx"

    # year_2019 is a string that stores the name of the unique transmission outage file for the year 2019
    year_2019 = r"S:\asset ops\GO_Group\Interns\2019\Anubha\Constraint Project\Constraint-Project\Data\UniqueTransmissionOutagesMapped2019.xlsx"

    # dataFrame14 reads the excel file whose name is stored in year_2014 as a dataframe
    dataFrame14 = pd.read_excel(year_2014, sheet_name="Outages", index=False)
    # Formatting the column names of excel sheet into one form to make it easier to access column names (all lower case letters with words separated using underscore)
    dataFrame14.columns = dataFrame14.columns.str.strip().str.lower().str.replace(' ', '_').str.replace('(','').str.replace(')', '')
    # this line preserves the index and converts it into a column
    dataFrame14.reset_index(inplace=True, drop=True)
    cores = mp.cpu_count()
    dataFrame14_split = np.array_split(dataFrame14, cores)
    pool = Pool(cores)
    resultall14 = pd.concat(pool.map(concatenateall, dataFrame14_split))
    resultmapped14 = pd.concat(pool.map(concatenatemapped, dataFrame14_split))
    resultunmapped14 = pd.concat(pool.map(concatenateunmapped, dataFrame14_split))
    pool.close()
    pool.join()

    # dataFrame15 reads the excel file whose name is stored in year_2015 as a dataframe
    dataFrame15 = pd.read_excel(year_2015, sheet_name="Outages", index=False)
    # Formatting the column names of excel sheet into one form to make it easier to access column names (all lower case letters with words separated using underscore)
    dataFrame15.columns = dataFrame15.columns.str.strip().str.lower().str.replace(' ', '_').str.replace('(','').str.replace(')', '')
    # this line preserves the index and converts it into a column
    dataFrame15.reset_index(inplace=True, drop=True)
    cores = mp.cpu_count()
    dataFrame15_split = np.array_split(dataFrame15, cores)
    pool = Pool(cores)
    resultall15 = pd.concat(pool.map(concatenateall, dataFrame15_split))
    resultmapped15 = pd.concat(pool.map(concatenatemapped, dataFrame15_split))
    resultunmapped15 = pd.concat(pool.map(concatenateunmapped, dataFrame15_split))
    pool.close()
    pool.join()

    # dataFrame16 reads the excel file whose name is stored in year_2016 as a dataframe
    dataFrame16 = pd.read_excel(year_2016, sheet_name="Outages", index=False)
    # Formatting the column names of excel sheet into one form to make it easier to access column names (all lower case letters with words separated using underscore)
    dataFrame16.columns = dataFrame16.columns.str.strip().str.lower().str.replace(' ', '_').str.replace('(','').str.replace(')', '')
    # this line preserves the index and converts it into a column
    dataFrame16.reset_index(inplace=True, drop=True)
    cores = mp.cpu_count()
    dataFrame16_split = np.array_split(dataFrame16, cores)
    pool = Pool(cores)
    resultall16 = pd.concat(pool.map(concatenateall, dataFrame16_split))
    resultmapped16 = pd.concat(pool.map(concatenatemapped, dataFrame16_split))
    resultunmapped16 = pd.concat(pool.map(concatenateunmapped, dataFrame16_split))
    pool.close()
    pool.join()

    # dataFrame17 reads the excel file whose name is stored in year_2017 as a dataframe
    dataFrame17 = pd.read_excel(year_2017, sheet_name="Outages", index=False)
    # Formatting the column names of excel sheet into one form to make it easier to access column names (all lower case letters with words separated using underscore)
    dataFrame17.columns = dataFrame17.columns.str.strip().str.lower().str.replace(' ', '_').str.replace('(','').str.replace(')', '')
    # this line preserves the index and converts it into a column
    dataFrame17.reset_index(inplace=True, drop=True)
    cores = mp.cpu_count()
    dataFrame17_split = np.array_split(dataFrame17, cores)
    pool = Pool(cores)
    resultall17 = pd.concat(pool.map(concatenateall, dataFrame17_split))
    resultmapped17 = pd.concat(pool.map(concatenatemapped, dataFrame17_split))
    resultunmapped17 = pd.concat(pool.map(concatenateunmapped, dataFrame17_split))
    pool.close()
    pool.join()

    # dataFrame18 reads the excel file whose name is stored in year_2018 as a dataframe
    dataFrame18 = pd.read_excel(year_2018, sheet_name="Outages", index=False)
    # Formatting the column names of excel sheet into one form to make it easier to access column names (all lower case letters with words separated using underscore)
    dataFrame18.columns = dataFrame18.columns.str.strip().str.lower().str.replace(' ', '_').str.replace('(','').str.replace(')', '')
    # this line preserves the index and converts it into a column
    dataFrame18.reset_index(inplace=True, drop=True)
    cores = mp.cpu_count()
    dataFrame18_split = np.array_split(dataFrame18, cores)
    pool = Pool(cores)
    resultall18 = pd.concat(pool.map(concatenateall, dataFrame18_split))
    resultmapped18 = pd.concat(pool.map(concatenatemapped, dataFrame18_split))
    resultunmapped18 = pd.concat(pool.map(concatenateunmapped, dataFrame18_split))
    pool.close()
    pool.join()

    # dataFrame19 reads the excel file whose name is stored in year_2019 as a dataframe
    dataFrame19 = pd.read_excel(year_2019, sheet_name="Outages", index=False)
    # Formatting the column names of excel sheet into one form to make it easier to access column names (all lower case letters with words separated using underscore)
    dataFrame19.columns = dataFrame19.columns.str.strip().str.lower().str.replace(' ', '_').str.replace('(','').str.replace(')', '')
    # this line preserves the index and converts it into a column
    dataFrame19.reset_index(inplace=True, drop=True)
    cores = mp.cpu_count()
    dataFrame19_split = np.array_split(dataFrame19, cores)
    pool = Pool(cores)
    resultall19 = pd.concat(pool.map(concatenateall, dataFrame19_split))
    resultmapped19 = pd.concat(pool.map(concatenatemapped, dataFrame19_split))
    resultunmapped19 = pd.concat(pool.map(concatenateunmapped, dataFrame19_split))
    pool.close()
    pool.join()

    finalOutages_all = pd.DataFrame()
    finalOutages_mapped = pd.DataFrame()
    finalOutages_unmapped = pd.DataFrame()

    finalOutages_all = finalOutages_all.append(resultall14)
    finalOutages_all = finalOutages_all.append(resultall15)
    finalOutages_all = finalOutages_all.append(resultall16)
    finalOutages_all = finalOutages_all.append(resultall17)
    finalOutages_all = finalOutages_all.append(resultall18)
    finalOutages_all = finalOutages_all.append(resultall19)

    finalOutages_mapped = finalOutages_mapped.append(resultmapped14)
    finalOutages_mapped = finalOutages_mapped.append(resultmapped15)
    finalOutages_mapped = finalOutages_mapped.append(resultmapped16)
    finalOutages_mapped = finalOutages_mapped.append(resultmapped17)
    finalOutages_mapped = finalOutages_mapped.append(resultmapped18)
    finalOutages_mapped = finalOutages_mapped.append(resultmapped19)

    finalOutages_unmapped = finalOutages_unmapped.append(resultunmapped14)
    finalOutages_unmapped = finalOutages_unmapped.append(resultunmapped15)
    finalOutages_unmapped = finalOutages_unmapped.append(resultunmapped16)
    finalOutages_unmapped = finalOutages_unmapped.append(resultunmapped17)
    finalOutages_unmapped = finalOutages_unmapped.append(resultunmapped18)
    finalOutages_unmapped = finalOutages_unmapped.append(resultunmapped19)


    # Creating a new dataframe that stores unique transmission outages present in finalOutages dataframe by dropping the duplicates based on value of 'facility' column
    finalOutages_all = finalOutages_all.drop_duplicates(subset='facility', keep='first', inplace=False)
    finalOutages_mapped = finalOutages_mapped.drop_duplicates(subset='facility', keep='first', inplace=False)
    finalOutages_unmapped = finalOutages_unmapped.drop_duplicates(subset='facility', keep='first', inplace=False)
    # Saving the finalOutagesUnique and finalOutages dataframe to an excel file
    writer = pd.ExcelWriter(r"S:\asset ops\GO_Group\Interns\2019\Anubha\Constraint Project\Constraint-Project\Data\UniqueTransmissionOutagesMapped2014-2019New.xlsx")
    # Saving mapped transmission outages with duplicates to Sheet1 of excel file
    finalOutages_all.to_excel(writer, 'Sheet1')
    # Saving mapped transmission outages without duplicates to Sheet2 of the same excel file
    finalOutages_mapped.to_excel(writer, 'Sheet2')
    finalOutages_unmapped.to_excel(writer, 'Sheet3')
    writer.save()


if __name__ == '__main__':
    main()


