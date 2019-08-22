# Created on: August 22,2019
# Created by: Anubha Barve (anubha.barve@constellation.com - Intern 2019)
# File name: ConstraintContingencyUniquePairCreation.py
# Purpose: This file is created to extract the unique constraint-contingency pair from historical constraint data

import pandas as pd


class UniquePair():
    def uniquePairCreation(self):
        # inputFile14 variable contains the Input Excel File which consist of the Historical Constraint Data and sheet name = 2018 denotes that data is taken for the year 2018
        inputFile14 = pd.read_excel(r"S:\asset ops\GO_Group\Interns\2019\Anubha\Constraint Project\Constraint-Project\Data\New Constraint Data\constraint14.xls",sheet_name="Sheet0", index=False)

        # inputFile15 variable contains the Input Excel File which consist of the Historical Constraint Data and sheet name = 2015 denotes that data is taken for the year 2015
        inputFile15 = pd.read_excel(r"S:\asset ops\GO_Group\Interns\2019\Anubha\Constraint Project\Constraint-Project\Data\New Constraint Data\constraint15.xls",sheet_name="Sheet0", index=False)

        # inputFile16 variable contains the Input Excel File which consist of the Historical Constraint Data and sheet name = 2016 denotes that data is taken for the year 2016
        inputFile16 = pd.read_excel(r"S:\asset ops\GO_Group\Interns\2019\Anubha\Constraint Project\Constraint-Project\Data\New Constraint Data\constraint16.xls",sheet_name="Sheet0", index=False)

        # inputFile17 variable contains the Input Excel File which consist of the Historical Constraint Data and sheet name = 2017 denotes that data is taken for the year 2017
        inputFile17 = pd.read_excel(r"S:\asset ops\GO_Group\Interns\2019\Anubha\Constraint Project\Constraint-Project\Data\New Constraint Data\constraint17.xls",sheet_name="Sheet0", index=False)

        # inputFile18 variable contains the Input Excel File which consist of the Historical Constraint Data and sheet name = 2017 denotes that data is taken for the year 2017
        inputFile18 = pd.read_excel(r"S:\asset ops\GO_Group\Interns\2019\Anubha\Constraint Project\Constraint-Project\Data\New Constraint Data\constraint18.xls",sheet_name="Sheet0", index=False)

        # inputFile19 variable contains the Input Excel File which consist of the Historical Constraint Data and sheet name = 2017 denotes that data is taken for the year 2017
        inputFile19 = pd.read_excel(r"S:\asset ops\GO_Group\Interns\2019\Anubha\Constraint Project\Constraint-Project\Data\New Constraint Data\constraint19.xls",sheet_name="Sheet0", index=False)

        # Creating an empty dataframe to store the final result of all the years
        outputFile = pd.DataFrame()

        # Removing the duplicates based on Columns - MONITORED ELEMENT NAME and CONTINGENCY by only keeping the first occurences of unique pairs and copying it to a new dataframe
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
        writer = pd.ExcelWriter(r"S:\asset ops\GO_Group\Interns\2019\Anubha\Constraint Project\Constraint-Project\Data\UniqueConstraintContingencyPair\uniquePairList2014-2019New.xlsx")

        # Storing the outputFile dataframe to the newly created excel file above and saving it
        outputFile.to_excel(writer, 'Sheet1')
        writer.save()


def main():
    uniquePair = UniquePair()
    uniquePair.uniquePairCreation()


if __name__ == '__main__':
    main()





