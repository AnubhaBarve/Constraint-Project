# Created on: August 22,2019
# Created by: Anubha Barve (anubha.barve@constellation.com - Intern 2019)
# File name: ConstraintContingencyUniquePairCreation.py
# Purpose: This file is created to extract the unique constraint-contingency pair from historical constraint data

# import statement
# -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
import pandas as pd

# Code starts
# --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
# class UniquePair defines a method named uniquePairCreation which extracts the unique constraint-contingency pair


class UniquePair():
    def uniquePairCreation(self):

        # inputFile14 variable contains the Input Excel File which consist of the Historical Constraint Data of the year 2014
        inputFile14 = pd.read_excel(r"S:\asset ops\GO_Group\Interns\2019\Anubha\Constraint Project\Constraint-Project\Data\New Constraint Data\constraint14.xls",sheet_name="Sheet0", index=False)

        # inputFile15 variable contains the Input Excel File which consist of the Historical Constraint Data of the year 2015
        inputFile15 = pd.read_excel(r"S:\asset ops\GO_Group\Interns\2019\Anubha\Constraint Project\Constraint-Project\Data\New Constraint Data\constraint15.xls",sheet_name="Sheet0", index=False)

        # inputFile16 variable contains the Input Excel File which consist of the Historical Constraint Data of the year 2016
        inputFile16 = pd.read_excel(r"S:\asset ops\GO_Group\Interns\2019\Anubha\Constraint Project\Constraint-Project\Data\New Constraint Data\constraint16.xls",sheet_name="Sheet0", index=False)

        # inputFile17 variable contains the Input Excel File which consist of the Historical Constraint Data of the year 2017
        inputFile17 = pd.read_excel(r"S:\asset ops\GO_Group\Interns\2019\Anubha\Constraint Project\Constraint-Project\Data\New Constraint Data\constraint17.xls",sheet_name="Sheet0", index=False)

        # inputFile18 variable contains the Input Excel File which consist of the Historical Constraint Data of the year 2018
        inputFile18 = pd.read_excel(r"S:\asset ops\GO_Group\Interns\2019\Anubha\Constraint Project\Constraint-Project\Data\New Constraint Data\constraint18.xls",sheet_name="Sheet0", index=False)

        # inputFile19 variable contains the Input Excel File which consist of the Historical Constraint Data of the year 2019
        inputFile19 = pd.read_excel(r"S:\asset ops\GO_Group\Interns\2019\Anubha\Constraint Project\Constraint-Project\Data\New Constraint Data\constraint19.xls",sheet_name="Sheet0", index=False)

        # auctionMappingFile_contingency DataFrame stores the excel file that contains the auction mapping data of transformer facilities (mapping file used is of September 2019)
        auctionMappingFile_contingency = pd.read_excel(r"S:\asset ops\GO_Group\Interns\2019\Anubha\Constraint Project\Constraint-Project\Data\Mapping Documents\2019.SEP.Monthly.Auction.Contingencies.CSV",sheet_name="2019.SEP.Monthly.Auction.Contin", index=False)
        # Formatting the column names of excel sheet into one form to make it easier to access column names (all lower case letters with words separated using underscore)
        auctionMappingFile_contingency.columns = auctionMappingFile_contingency.columns.str.strip().str.lower().str.replace(' ','_').str.replace(')', '').str.replace(')', '')
        # preserves index and converts into a column
        auctionMappingFile_contingency.reset_index(inplace=True, drop=True)

        # auctionMappingFile DataFrame stores the excel file that contains the auction mapping data of line facilities (mapping file used is of September 2019)
        auctionMappingFile = pd.read_excel(r"S:\asset ops\GO_Group\Interns\2019\Anubha\Constraint Project\Constraint-Project\Data\Mapping Documents\2019.SEP.Monthly.Auction.MappingDocument.xlsx",sheet_name="Lines", index=False)
        # Formatting the column names of excel sheet into one form to make it easier to access column names (all lower case letters with words separated using underscore)
        auctionMappingFile.columns = auctionMappingFile.columns.str.strip().str.lower().str.replace(' ','_').str.replace(')', '').str.replace(')', '')
        # preserves index and converts into a column
        auctionMappingFile.reset_index(inplace=True, drop=True)

        # Creating an empty dataframe to store the final result of all the years
        outputFile = pd.DataFrame()

        # Removing the duplicates based on Columns - CONSTRAINT and CONTINGENCY by only keeping the first occurences of unique pairs and copying it to a new dataframe
        sheet1 = inputFile18.drop_duplicates(subset=['Constraint', 'Contingency'], keep='first',inplace=False)
        sheet2 = inputFile17.drop_duplicates(subset=['Constraint', 'Contingency'], keep='first',inplace=False)
        sheet3 = inputFile16.drop_duplicates(subset=['Constraint', 'Contingency'], keep='first',inplace=False)
        sheet4 = inputFile15.drop_duplicates(subset=['Constraint', 'Contingency'], keep='first',inplace=False)
        sheet5 = inputFile14.drop_duplicates(subset=['Constraint', 'Contingency'], keep='first',inplace=False)
        sheet6 = inputFile19.drop_duplicates(subset=['Constraint', 'Contingency'], keep='first',inplace=False)

        # Appending all the sheets to the empty dataframe
        outputFile = outputFile.append(sheet1)
        outputFile = outputFile.append(sheet2)
        outputFile = outputFile.append(sheet3)
        outputFile = outputFile.append(sheet4)
        outputFile = outputFile.append(sheet5)
        outputFile = outputFile.append(sheet6)

        # Creating a new excel file for the list of unique pairs
        writer = pd.ExcelWriter(r"S:\asset ops\GO_Group\Interns\2019\Anubha\Constraint Project\Constraint-Project\Data\UniqueConstraintContingencyPair\uniquePairList2014-2019.xlsx")
        # Storing the outputFile dataframe to the newly created excel file above
        outputFile.to_excel(writer, 'Constraint-Contingency')
        # Storing the auction mapping file to the newly created excel file above
        auctionMappingFile.to_excel(writer, 'AuctionMapping2019SEP')
        # Storing the auction contingency file to the newly created excel file above
        auctionMappingFile_contingency.to_excel(writer, 'AuctionContingency2019SEP')
        # saving the excel file
        writer.save()

# main function
# -----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------


def main():
    # creating object of class UniquePair
    uniquePair = UniquePair()
    # calling uniquePairCreation method of class UniquePair
    uniquePair.uniquePairCreation()

# call to main function
# ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------


if __name__ == '__main__':
    main()





