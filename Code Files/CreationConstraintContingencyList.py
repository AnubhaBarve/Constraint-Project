# Not used yet
 #Created on: September 17, 2019
# Created by: Anubha Barve (anubha.barve@constellation - Intern 2019)
# File name: CreationConstraintContingencyList.py
# Purpose: This file is developed to create a super-list of transmission outages i.e combine unique transmission outages(facility and start date unique) of all years into one file which are mapped (for now only storing
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
def combine(df):

    finalList = r"S:\asset ops\GO_Group\Interns\2019\Anubha\Constraint Project\Constraint-Project\Data\UniqueConstraintContingencyPair\ConstraintContingencyList.xlsx"
    # dataFrame14 reads the excel file whose name is stored in year_2014 as a dataframe
    final = pd.read_excel(finalList, sheet_name="mappedBoth", index=False)
    # Formatting the column names of excel sheet into one form to make it easier to access column names (all lower case letters with words separated using underscore)
    final.columns = final.columns.str.strip().str.lower().str.replace(' ', '_').str.replace('(','').str.replace(')', '')
    # this line preserves the index and converts it into a column
    final.reset_index(inplace=True, drop=True)

    for i1,r1 in tqdm(df.iterrows()):
        df.at[i1, 'lookup'] = str(r1['facilityname']).join(str(r1['contingency']))
        break

    # finalOutages is an empty dataframe created to store the final output
    finalOutages_all = pd.DataFrame()
    # Iterating the df dataframe row-wise (where tqdm displays progress of the loop during execution)
    for i1, r1 in tqdm(df.iterrows()):
        # Storing only those transmission outages that are mapped i.e. the ones that have a value for 'from_bus_number' column
            for i2,r2 in tqdm(final.iterrows()):
                    if str(r1['lookup']) == str(r2['lookup']):
                        df.at[i1, 'constraint_from_bus_number'] = r2['constraint_from_bus_number']
                        df.at[i1, 'constraint_from_bus_name'] = r2['constraint_from_bus_name']
                        df.at[i1, 'constraint_to_bus_number'] = r2['constraint_to_bus_number']
                        df.at[i1, 'constraint_to_bus_name'] = r2['constraint_to_bus_name']
                        df.at[i1, 'contingency_from_bus_number'] = r2['contingency_from_bus_number']
                        df.at[i1, 'contingency_from_bus_name'] = r2['contingency_from_bus_name']
                        df.at[i1, 'contingency_to_bus_number'] = r2['contingency_to_bus_number']
                        df.at[i1, 'contingency_to_bus_name'] = r2['contingency_to_bus_name']
                        df.at[i1, 'contingency_circuit_id'] = r2['contingency_circuit_id']
                        df.at[i1, 'constraint_circuit_id'] = r2['constraint_circuit_id']
                        df.at[i1, 'element_a_contingency'] = r2['element_a_contingency']
                        df.at[i1, 'element_b_monitored'] = r2['element_b_monitored']
                        df.at[i1, 'interface_name'] = r2['interface_name']
                        df.at[i1, 'meter_far'] = r2['meter_far']
                        df.at[i1, 'weight'] = r2['weight']
                        df.at[i1, 'verification'] = r2['verification']
                    break
            break
    return df

# main function which calls the class's function named 'combine()'
def main():
    # final_file is a string that stores the name of the final transmission outages file
    final_file = r"S:\asset ops\GO_Group\Interns\2019\Anubha\Constraint Project\Constraint-Project\Data\UniqueConstraintContingencyPair\ConstraintContingencyList.xlsx"

    # dataFrame14 reads the excel file whose name is stored in year_2014 as a dataframe
    dataFrame14 = pd.read_excel(final_file, sheet_name="2014", index=False)
    # Formatting the column names of excel sheet into one form to make it easier to access column names (all lower case letters with words separated using underscore)
    dataFrame14.columns = dataFrame14.columns.str.strip().str.lower().str.replace(' ', '_').str.replace('(','').str.replace(')', '')
    # this line preserves the index and converts it into a column
    dataFrame14.reset_index(inplace=True, drop=True)


    # dataFrame15 reads the excel file whose name is stored in year_2015 as a dataframe
    dataFrame15 = pd.read_excel(final_file, sheet_name="2015", index=False)
    # Formatting the column names of excel sheet into one form to make it easier to access column names (all lower case letters with words separated using underscore)
    dataFrame15.columns = dataFrame15.columns.str.strip().str.lower().str.replace(' ', '_').str.replace('(','').str.replace(')', '')
    # this line preserves the index and converts it into a column
    dataFrame15.reset_index(inplace=True, drop=True)


    # dataFrame16 reads the excel file whose name is stored in year_2016 as a dataframe
    dataFrame16 = pd.read_excel(final_file, sheet_name="2016", index=False)
    # Formatting the column names of excel sheet into one form to make it easier to access column names (all lower case letters with words separated using underscore)
    dataFrame16.columns = dataFrame16.columns.str.strip().str.lower().str.replace(' ', '_').str.replace('(','').str.replace(')', '')
    # this line preserves the index and converts it into a column
    dataFrame16.reset_index(inplace=True, drop=True)


    # dataFrame17 reads the excel file whose name is stored in year_2017 as a dataframe
    dataFrame17 = pd.read_excel(final_file, sheet_name="2017", index=False)
    # Formatting the column names of excel sheet into one form to make it easier to access column names (all lower case letters with words separated using underscore)
    dataFrame17.columns = dataFrame17.columns.str.strip().str.lower().str.replace(' ', '_').str.replace('(','').str.replace(')', '')
    # this line preserves the index and converts it into a column
    dataFrame17.reset_index(inplace=True, drop=True)


    # dataFrame18 reads the excel file whose name is stored in year_2018 as a dataframe
    dataFrame18 = pd.read_excel(final_file, sheet_name="2018", index=False)
    # Formatting the column names of excel sheet into one form to make it easier to access column names (all lower case letters with words separated using underscore)
    dataFrame18.columns = dataFrame18.columns.str.strip().str.lower().str.replace(' ', '_').str.replace('(','').str.replace(')', '')
    # this line preserves the index and converts it into a column
    dataFrame18.reset_index(inplace=True, drop=True)


    # dataFrame19 reads the excel file whose name is stored in year_2019 as a dataframe
    dataFrame19 = pd.read_excel(final_file, sheet_name="2019", index=False)
    # Formatting the column names of excel sheet into one form to make it easier to access column names (all lower case letters with words separated using underscore)
    dataFrame19.columns = dataFrame19.columns.str.strip().str.lower().str.replace(' ', '_').str.replace('(','').str.replace(')', '')
    # this line preserves the index and converts it into a column
    dataFrame19.reset_index(inplace=True, drop=True)
    dataFrame19['lookup'] = " "
    dataFrame19['constraint_from_bus_number'] = " "
    dataFrame19['constraint_from_bus_name'] = " "
    dataFrame19['constraint_to_bus_number'] = " "
    dataFrame19['constraint_to_bus_name'] = " "
    dataFrame19['contingency_from_bus_number'] = " "
    dataFrame19['contingency_from_bus_name'] = " "
    dataFrame19['contingency_to_bus_number'] = " "
    dataFrame19['contingency_to_bus_name'] = " "
    dataFrame19['contingency_circuit_id'] = " "
    dataFrame19['constraint_circuit_id'] = " "
    dataFrame19['element_a_contingency'] = " "
    dataFrame19['element_b_monitored'] = " "
    dataFrame19['interface_name'] = " "
    dataFrame19['meter_far'] = " "
    dataFrame19['weight'] = " "
    dataFrame19['verification'] = " "
    cores = mp.cpu_count()
    dataFrame19_split = np.array_split(dataFrame19, cores)
    pool = Pool(cores)
    result19 = pd.concat(pool.map(combine, dataFrame19_split))
    pool.close()
    pool.join()

    finalList = r"S:\asset ops\GO_Group\Interns\2019\Anubha\Constraint Project\Constraint-Project\Data\UniqueConstraintContingencyPair\ConstraintContingencyList.xlsx"
    # dataFrame14 reads the excel file whose name is stored in year_2014 as a dataframe
    mapped = pd.read_excel(finalList, sheet_name="mappedBoth", index=False)
    # Formatting the column names of excel sheet into one form to make it easier to access column names (all lower case letters with words separated using underscore)
    mapped.columns = mapped.columns.str.strip().str.lower().str.replace(' ', '_').str.replace('(', '').str.replace(')',                                                                                                             '')
    # this line preserves the index and converts it into a column
    mapped.reset_index(inplace=True, drop=True)

    # Saving the finalOutagesUnique and finalOutages dataframe to an excel file
    writer = pd.ExcelWriter(r"S:\asset ops\GO_Group\Interns\2019\Anubha\Constraint Project\Constraint-Project\Data\UniqueConstraintContingencyPair\ConstraintContingencyListTry.xlsx")
    # Saving mapped transmission outages with duplicates to Sheet1 of excel file
    dataFrame14.to_excel(writer, '2014')
    dataFrame15.to_excel(writer, '2015')
    dataFrame16.to_excel(writer, '2016')
    dataFrame17.to_excel(writer, '2017')
    dataFrame18.to_excel(writer, '2018')
    result19.to_excel(writer, '2019')
    mapped.to_excel(writer, 'mappedBoth')
    # Saving mapped transmission outages without duplicates to Sheet2 of the same excel file
    #finalOutages_mapped.to_excel(writer, 'Mapped')
    # saving the excel sheet
    writer.save()


if __name__ == '__main__':
    main()


