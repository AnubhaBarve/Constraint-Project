# Created on : July 30,2019
# Created by: Anubha Barve (anubha.barve@constellation.com - Intern 2019)
# File name: TransmissionOutagesMapping.py
# Purpose: This file is created to format the transmission outages file such that all the duplicate facilities are removed and 'to' column values are changed by removing all special characters and
# similarly special characters are removed from auction mapping file's 'op_eqcode' column as well to make the mapping easier i.e
# the transmission lines can be mapped in PowerWorld with their respective From Bus Number and To Bus Number

import pandas as pd

#This clas defines a function named outagesMapping which maps the transmission outages of year 2018 to the auction mapping file
class TransmissionOutagesMapping():
    def outagesMapping(self):

        # The outagesInputFile DataFrame stores the input file i.e. excel file that contains transmission outages data for the year 2018
        outagesInputFile = pd.read_excel(r"S:\asset ops\GO_Group\Interns\2019\Anubha\Constraint Project\Data\Outages + Constraint\ERCOT_TransmissionOutage_2019-01-01_to_2019-07-24.xlsx", sheet_name="2019", index=False)

        # Formatting the column names of excel sheet into one form to make it easier to access column names (all lower case letters with words separated using underscore)
        outagesInputFile.columns = outagesInputFile.columns.str.strip().str.lower().str.replace(' ', '_').str.replace('(','').str.replace(')', '')

        #dropping duplicates based on facility
        outagesInputFile = outagesInputFile.drop_duplicates(subset='facility')

        # auctionMappingFile DataFrame stores the excel file that contains the auction mapping data (mapping file used is of July 2019)
        auctionMappingFile = pd.read_excel(r"S:\asset ops\GO_Group\Interns\2019\Anubha\Constraint Project\Data\Outages + Constraint\ERCOT_TransmissionOutage_2019-01-01_to_2019-07-24.xlsx", sheet_name="AuctionMapping2019JUL", index=False)

        # Formatting the column names of excel sheet into one form to make it easier to access column names (all lower case letters with words separated using underscore)
        auctionMappingFile.columns = auctionMappingFile.columns.str.strip().str.lower().str.replace(' ', '_').str.replace('(', '').str.replace(')', '')

        print(outagesInputFile.columns)
        print(auctionMappingFile.columns)

        # Changing the row values of 'to' column in outagesInputFile by removing all the special characters (to make mapping easier)
        for index,data in outagesInputFile.iterrows():
            string = str(data['to'])
            data['to'] = ''.join(s for s in string if s.isalnum())
            outagesInputFile.at[index,'to'] = data['to']

        # Changing the row values of 'op_eqcode' column of auctionMappingFile DataFrame by removing all the special characters (to make mapping easier)
        for index, data in auctionMappingFile.iterrows():
            string = str(data['op_eqcode'])
            data['op_eqcode'] = ''.join(s for s in string if s.isalnum())
            auctionMappingFile.at[index, 'op_eqcode'] = data['op_eqcode']

        # Saving the changes outagesInputFile Dataframe to an excel file
        writer = pd.ExcelWriter(r"S:\asset ops\GO_Group\Interns\2019\Anubha\Constraint Project\Data\UniqueTransmissionOutagesMapped2019.xlsx")
        outagesInputFile.to_excel(writer, 'Sheet1')
        auctionMappingFile.to_excel(writer, 'Sheet2')
        writer.save()

def main():
    mapping = TransmissionOutagesMapping()
    mapping.outagesMapping()

if __name__ =='__main__':
    main()
