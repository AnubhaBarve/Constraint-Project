# Created on : August 22,2019
# Created by: Anubha Barve (anubha.barve@constellation.com - Intern 2019)
# File name: ConstraintContingencyMappingNew.py
# Purpose: This file is created to format the constraint-contingency file such that 'code' column values are changed by removing all special characters and similarly special characters are removed from
# auction mapping file's 'op_eqcode' column as well to make the mapping easier i.e the constraints can be mapped with their respective From Bus Number and To Bus Number

# import statement
# -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
import pandas as pd

# code starts
# -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

# This clas defines a function named mapping which formats the columns of two excel files in order to make the process of mapping easier.


class ConstraintContingencyMapping():
    def mapping(self):

        # The inputfile DataFrame stores the input file i.e. excel file that contains constraint-contingency data for the year 2014 to 2019
        inputfile = pd.read_excel(r"S:\asset ops\GO_Group\Interns\2019\Anubha\Constraint Project\Constraint-Project\Data\UniqueConstraintContingencyPair\uniquePairList2014-2019.xlsx", sheet_name="Constraint-Contingency", index=False)

        # Formatting the column names of excel sheet into one form to make it easier to access column names (all lower case letters with words separated using underscore)
        inputfile.columns = inputfile.columns.str.strip().str.lower().str.replace(' ', '_').str.replace('(','').str.replace(')', '')

        # preserves the index and converts it into a column
        inputfile.reset_index(inplace=True, drop=True)

        # auctionMappingFile DataFrame stores the excel file that contains the auction mapping data (mapping file used is of September 2019)
        auctionMappingFile = pd.read_excel(r"S:\asset ops\GO_Group\Interns\2019\Anubha\Constraint Project\Constraint-Project\Data\UniqueConstraintContingencyPair\uniquePairList2014-2019.xlsx", sheet_name="AuctionMapping2019SEP", index=False)

        # Formatting the column names of excel sheet into one form to make it easier to access column names (all lower case letters with words separated using underscore)
        auctionMappingFile.columns = auctionMappingFile.columns.str.strip().str.lower().str.replace(' ', '_').str.replace('(', '').str.replace(')', '')

        # preserves the index and converts it into a column
        auctionMappingFile.reset_index(inplace=True, drop=True)

        # The inputfile DataFrame stores the input file i.e. excel file that contains constraint-contingency data for the year 2014 to 2019
        auctionContingency = pd.read_excel(r"S:\asset ops\GO_Group\Interns\2019\Anubha\Constraint Project\Constraint-Project\Data\UniqueConstraintContingencyPair\uniquePairList2014-2019.xlsx", sheet_name="AuctionContingency2019SEP", index=False)

        # Formatting the column names of excel sheet into one form to make it easier to access column names (all lower case letters with words separated using underscore)
        auctionContingency.columns = auctionContingency.columns.str.strip().str.lower().str.replace(' ', '_').str.replace('(','').str.replace(')', '')

        # preserves the index and converts it into a column
        auctionContingency.reset_index(inplace=True, drop=True)

        # Changing the values of 'code' column in inputfile dataframe by removing all the special characters (to make mapping easier - which uses vlookup for searching a string)
        for index,data in inputfile.iterrows():
            string = str(data['code'])
            data['code'] = ''.join(s for s in string if s.isalnum())
            inputfile.at[index, 'code'] = data['code']

        # Changing the values of 'op_eqcode' column of auctionMappingFile DataFrame by removing all the special characters (to make mapping easier - which uses vlookup for searching a string)
        for index, data in auctionMappingFile.iterrows():
            string = str(data['op_eqcode'])
            data['op_eqcode'] = ''.join(s for s in string if s.isalnum())
            auctionMappingFile.at[index, 'op_eqcode'] = data['op_eqcode']

        # Saving the changes made to inputfile Dataframe in an excel file
        writer = pd.ExcelWriter(r"S:\asset ops\GO_Group\Interns\2019\Anubha\Constraint Project\Constraint-Project\Data\UniqueConstraintContingencyPair\uniquePairList2014-2019Changed.xlsx")
        # writing the changed inputfile dataframe to a sheet of created excel file
        inputfile.to_excel(writer, 'Constraint-Contingency')
        # writing the changed auctionMappingFile to a sheet of created excel file
        auctionMappingFile.to_excel(writer, 'AuctionMapping2019SEP')
        # writing the changed auctionMappingFile to a sheet of created excel file
        auctionContingency.to_excel(writer, 'AuctionContingency2019SEP')
        # Saving the excel sheet
        writer.save()

# main function
# ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------


def main():
    maps = ConstraintContingencyMapping()
    maps.mapping()

# call to main function
# -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------


if __name__ =='__main__':
    main()
