# Created on: August 05, 2019
# Created by: Anubha Barve (anubha.barve@constellation - Intern 2019)
# File name: UniqueTransmissionOutagesConcat.py
# Purpose: This file is developed to create a super-list of transmission outages i.e combine unique transmission outages of all years into one file which are mapped (for now only storing
# data for line and transformer facilities)

import pandas as pd
from tqdm import tqdm

# This class define a function named concatenate which combines the unique transmission outages that are mapped of all years
class TransmissionOutagesConcatenation():
    def concatenate(self):
        # year_2014 is a string that stores the name of the unique transmission outage file for the year 2014
        year_2014 = r"S:\asset ops\GO_Group\Interns\2019\Anubha\Constraint Project\Data\UniqueTransmissionOutagesMapped2014.xlsx"

        # year_2015 is a string that stores the name of the unique transmission outage file for the year 2015
        year_2015 = r"S:\asset ops\GO_Group\Interns\2019\Anubha\Constraint Project\Data\UniqueTransmissionOutagesMapped2015.xlsx"

        # year_2016 is a string that stores the name of the unique transmission outage file for the year 2016
        year_2016 = r"S:\asset ops\GO_Group\Interns\2019\Anubha\Constraint Project\Data\UniqueTransmissionOutagesMapped2016.xlsx"

        # year_2017 is a string that stores the name of the unique transmission outage file for the year 2017
        year_2017 = r"S:\asset ops\GO_Group\Interns\2019\Anubha\Constraint Project\Data\UniqueTransmissionOutagesMapped2017.xlsx"

        # year_2018 is a string that stores the name of the unique transmission outage file for the year 2018
        year_2018 = r"S:\asset ops\GO_Group\Interns\2019\Anubha\Constraint Project\Data\UniqueTransmissionOutagesMapped2018.xlsx"

        # year_2019 is a string that stores the name of the unique transmission outage file for the year 2019
        year_2019 = r"S:\asset ops\GO_Group\Interns\2019\Anubha\Constraint Project\Data\UniqueTransmissionOutagesMapped2019.xlsx"

        #dataFrame14 reads the excel file whose name is stored in year_2014 as a dataframe
        dataFrame14 = pd.read_excel(year_2014, sheet_name="Outages", index=False)
        # Formatting the column names of excel sheet into one form to make it easier to access column names (all lower case letters with words separated using underscore)
        dataFrame14.columns = dataFrame14.columns.str.strip().str.lower().str.replace(' ', '_').str.replace('(', '').str.replace(')', '')
        # this line preserves the index and converts it into a column
        dataFrame14.reset_index(inplace=True, drop=True)

        # dataFrame15 reads the excel file whose name is stored in year_2015 as a dataframe
        dataFrame15 = pd.read_excel(year_2015, sheet_name="Outages", index=False)
        # Formatting the column names of excel sheet into one form to make it easier to access column names (all lower case letters with words separated using underscore)
        dataFrame15.columns = dataFrame15.columns.str.strip().str.lower().str.replace(' ', '_').str.replace('(','').str.replace(')', '')
        # this line preserves the index and converts it into a column
        dataFrame15.reset_index(inplace=True, drop=True)

        # dataFrame16 reads the excel file whose name is stored in year_2016 as a dataframe
        dataFrame16 = pd.read_excel(year_2016, sheet_name="Outages", index=False)
        # Formatting the column names of excel sheet into one form to make it easier to access column names (all lower case letters with words separated using underscore)
        dataFrame16.columns = dataFrame16.columns.str.strip().str.lower().str.replace(' ', '_').str.replace('(','').str.replace(')', '')
        # this line preserves the index and converts it into a column
        dataFrame16.reset_index(inplace=True, drop=True)

        # dataFrame17 reads the excel file whose name is stored in year_2017 as a dataframe
        dataFrame17 = pd.read_excel(year_2017, sheet_name="Outages", index=False)
        # Formatting the column names of excel sheet into one form to make it easier to access column names (all lower case letters with words separated using underscore)
        dataFrame17.columns = dataFrame17.columns.str.strip().str.lower().str.replace(' ', '_').str.replace('(','').str.replace(')', '')
        # this line preserves the index and converts it into a column
        dataFrame17.reset_index(inplace=True, drop=True)

        # dataFrame18 reads the excel file whose name is stored in year_2018 as a dataframe
        dataFrame18 = pd.read_excel(year_2018, sheet_name="Outages", index=False)
        # Formatting the column names of excel sheet into one form to make it easier to access column names (all lower case letters with words separated using underscore)
        dataFrame18.columns = dataFrame18.columns.str.strip().str.lower().str.replace(' ', '_').str.replace('(','').str.replace(')', '')
        # this line preserves the index and converts it into a column
        dataFrame18.reset_index(inplace=True, drop=True)

        # dataFrame19 reads the excel file whose name is stored in year_2019 as a dataframe
        dataFrame19 = pd.read_excel(year_2019, sheet_name="Outages", index=False)
        # Formatting the column names of excel sheet into one form to make it easier to access column names (all lower case letters with words separated using underscore)
        dataFrame19.columns = dataFrame19.columns.str.strip().str.lower().str.replace(' ', '_').str.replace('(','').str.replace(')', '')
        # this line preserves the index and converts it into a column
        dataFrame19.reset_index(inplace=True, drop=True)

        # finalOutages is an empty dataframe created to store the final output
        finalOutages = pd.DataFrame()

        # Iterating the dataFrame14 dataframe row-wise (where tqdm displays progress of the loop during execution)
        for i1,r1 in tqdm(dataFrame14.iterrows()):
            # Selecting only those rows which have a facility_type as 'LINE' or 'XFMR'
            if (str(r1['facility_type']) == "LINE") or (str(r1['facility_type']) == "XFMR"):
                # Storing only those transmission outages that are mapped i.e. the ones that have a value for 'from_bus_number' column
                if str(r1['from_bus_number']) != " ":
                    finalOutages = finalOutages.append(r1)

        # Iterating the dataFrame15 dataframe row-wise (where tqdm displays progress of the loop during execution)
        for i1,r1 in tqdm(dataFrame15.iterrows()):
            # Selecting only those rows which have a facility_type as 'LINE' or 'XFMR'
            if (str(r1['facility_type']) == "LINE") or (str(r1['facility_type']) == "XFMR"):
                # Storing only those transmission outages that are mapped i.e. the ones that have a value for 'from_bus_number' column
                if str(r1['from_bus_number']) != " ":
                    finalOutages = finalOutages.append(r1)

        # Iterating the dataFrame16 dataframe row-wise (where tqdm displays progress of the loop during execution)
        for i1,r1 in tqdm(dataFrame16.iterrows()):
            # Selecting only those rows which have a facility_type as 'LINE' or 'XFMR'
            if (str(r1['facility_type']) == "LINE") or (str(r1['facility_type']) == "XFMR"):
                # Storing only those transmission outages that are mapped i.e. the ones that have a value for 'from_bus_number' column
                if str(r1['from_bus_number']) != " ":
                    finalOutages = finalOutages.append(r1)

        # Iterating the dataFrame17 dataframe row-wise (where tqdm displays progress of the loop during execution)
        for i1,r1 in tqdm(dataFrame17.iterrows()):
            # Selecting only those rows which have a facility_type as 'LINE' or 'XFMR'
            if (str(r1['facility_type']) == "LINE") or (str(r1['facility_type']) == "XFMR"):
                # Storing only those transmission outages that are mapped i.e. the ones that have a value for 'from_bus_number' column
                if str(r1['from_bus_number']) != " ":
                    finalOutages = finalOutages.append(r1)

        # Iterating the dataFrame18 dataframe row-wise (where tqdm displays progress of the loop during execution)
        for i1,r1 in tqdm(dataFrame18.iterrows()):
            # Selecting only those rows which have a facility_type as 'LINE' or 'XFMR'
            if (str(r1['facility_type']) == "LINE") or (str(r1['facility_type']) == "XFMR"):
                # Storing only those transmission outages that are mapped i.e. the ones that have a value for 'from_bus_number' column
                if str(r1['from_bus_number']) != " ":
                    finalOutages = finalOutages.append(r1)

        # Iterating the dataFrame19 dataframe row-wise (where tqdm displays progress of the loop during execution)
        for i1,r1 in tqdm(dataFrame19.iterrows()):
            # Selecting only those rows which have a facility_type as 'LINE' or 'XFMR'
            if (str(r1['facility_type']) == "LINE") or (str(r1['facility_type']) == "XFMR"):
                # Storing only those transmission outages that are mapped i.e. the ones that have a value for 'from_bus_number' column
                if str(r1['from_bus_number']) != " ":
                    finalOutages = finalOutages.append(r1)

        # Creating a new dataframe that stores unique transmission outages present in finalOutages dataframe by dropping the duplicates based on value of 'facility' column
        finalOutagesUnique = finalOutages.drop_duplicates(subset='facility')
        # Saving the finalOutagesUnique and finalOutages dataframe to an excel file
        writer = pd.ExcelWriter(r"S:\asset ops\GO_Group\Interns\2019\Anubha\Constraint Project\Data\UniqueTransmissionOutagesMapped2014-2019.xlsx")
        # Saving mapped transmission outages with duplicates to Sheet1 of excel file
        finalOutages.to_excel(writer, 'Sheet1')
        # Saving mapped transmission outages without duplicates to Sheet2 of the same excel file
        finalOutagesUnique.to_excel(writer, 'Sheet2')
        writer.save()


# main function which calls the class's function named 'concatenate()'
def main():
    outages = TransmissionOutagesConcatenation()
    outages.concatenate()


if __name__ == '__main__':
    main()


