# Created on: July 29,2019
# Created by: Anubha Barve (anubha.barve@constellation.com - Intern 2019)
# File name: ConstraintContingencyUniquePairCreation.py
# Purpose: This file is created to extract the unique constraint-contingency pair from historical constraint data

import pandas as pd

class UniquePair():
    def uniquePairCreation(self):
        # inputFile18 variable contains the Input Excel File which consist of the Historical Constraint Data and sheet name = 2018 denotes that data is taken for the year 2018
        inputFile18 = pd.read_excel(r"S:\asset ops\GO_Group\Interns\2019\Anubha\Constraint Project\Data\Outages + Constraint\ERCOT_DayAheadConstraintData_2015-2018.xlsx",sheet_name="2018", index=False)

        # inputFile15 variable contains the Input Excel File which consist of the Historical Constraint Data and sheet name = 2015 denotes that data is taken for the year 2015
        inputFile15 = pd.read_excel(r"S:\asset ops\GO_Group\Interns\2019\Anubha\Constraint Project\Data\Outages + Constraint\ERCOT_DayAheadConstraintData_2015-2018.xlsx",sheet_name="2015", index=False)

        # inputFile16 variable contains the Input Excel File which consist of the Historical Constraint Data and sheet name = 2016 denotes that data is taken for the year 2016
        inputFile16 = pd.read_excel(r"S:\asset ops\GO_Group\Interns\2019\Anubha\Constraint Project\Data\Outages + Constraint\ERCOT_DayAheadConstraintData_2015-2018.xlsx",sheet_name="2016", index=False)

        # inputFile17 variable contains the Input Excel File which consist of the Historical Constraint Data and sheet name = 2017 denotes that data is taken for the year 2017
        inputFile17 = pd.read_excel(r"S:\asset ops\GO_Group\Interns\2019\Anubha\Constraint Project\Data\Outages + Constraint\ERCOT_DayAheadConstraintData_2015-2018.xlsx",sheet_name="2017", index=False)

        # Creating an empty dataframe to store the final result of all the years
        outputFile = pd.DataFrame()

        # Removing the duplicates based on Columns - MONITORED ELEMENT NAME and CONTINGENCY by only keeping the first occurences of unique pairs and copying it to a new dataframe
        sheet1 = inputFile18.drop_duplicates(subset = ['Monitored Element Name','Contingency Name'],keep='first',inplace=False)
        sheet2 = inputFile17.drop_duplicates(subset=['Monitored Element Name', 'Contingency Name'], keep='first',inplace=False)
        sheet3 = inputFile16.drop_duplicates(subset=['Monitored Element Name', 'Contingency Name'], keep='first',inplace=False)
        sheet4 = inputFile15.drop_duplicates(subset=['Monitored Element Name', 'Contingency Name'], keep='first',inplace=False)

        # Appending all the sheets to the empty dataframe
        outputFile = outputFile.append(sheet1)
        outputFile = outputFile.append(sheet2)
        outputFile = outputFile.append(sheet3)
        outputFile = outputFile.append(sheet4)

        #Creating a new excel file for the list of unique pairs
        writer = pd.ExcelWriter(r"S:\asset ops\GO_Group\Interns\2019\Anubha\Constraint Project\Data\UniqueConstraintContingencyPair\uniquePairList2015-2018.xlsx")

        #Storing the outputFile dataframe to the newly created excel file above and saving it
        outputFile.to_excel(writer, 'Sheet1')
        writer.save()

def main():
    uniquePair = UniquePair()
    uniquePair.uniquePairCreation()

if __name__ == '__main__':
    main()





