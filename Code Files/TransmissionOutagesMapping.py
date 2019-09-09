# Created on : July 30,2019
# Created by: Anubha Barve (anubha.barve@constellation.com - Intern 2019)
# File name: TransmissionOutagesMapping.py
# Purpose: This file is created to format the transmission outages file such that all the duplicate facilities are removed and 'to' column values are changed by removing all special characters and
# similarly special characters are removed from auction mapping file's 'op_eqcode' column as well to make the mapping easier i.e
# the transmission lines can be mapped in PowerWorld with their respective From Bus Number and To Bus Number

# import statement
# ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
import pandas as pd

# Code starts
# ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
# outagesMapping function changes the 'to' column value removing all special characters
def outagesMapping(outagesInputFile):

    # Changing the row values of 'to' column in outagesInputFile by removing all the special characters (to make mapping easier)
    for index, data in outagesInputFile.iterrows():
        string = str(data['to'])
        data['to'] = ''.join(s for s in string if s.isalnum())
        outagesInputFile.at[index,'to'] = data['to']

    # returning the changed dataframe
    return outagesInputFile

# auctionFormatting function changes the 'op_code' column values removing the special characters
def auctionFormatting():

    # auctionMappingFile DataFrame stores the excel file that contains the auction mapping data (mapping file used is of September 2019)
    auctionMappingFile = pd.read_excel(r"S:\asset ops\GO_Group\Interns\2019\Anubha\Constraint Project\Constraint-Project\Data\Mapping Documents\2019.SEP.Monthly.Auction.MappingDocument.xlsx",sheet_name="Lines", index=False)
    # Formatting the column names of excel sheet into one form to make it easier to access column names (all lower case letters with words separated using underscore)
    auctionMappingFile.columns = auctionMappingFile.columns.str.strip().str.lower().str.replace(' ', '_').str.replace(')', '').str.replace(')', '')
    # preserves index and converts into a column
    auctionMappingFile.reset_index(inplace=True, drop=True)

    # Changing the row values of 'op_eqcode' column of auctionMappingFile DataFrame by removing all the special characters (to make mapping easier)
    for index, data in auctionMappingFile.iterrows():
        string = str(data['op_eqcode'])
        data['op_eqcode'] = ''.join(s for s in string if s.isalnum())
        auctionMappingFile.at[index, 'op_eqcode'] = data['op_eqcode']

    # returning the changed dataframe
    return auctionMappingFile


def main():
    # The inputfile14 DataFrame stores the input file i.e. excel file that contains transmission outages data for the year 2014
    inputfile14 = pd.read_excel(r"S:\asset ops\GO_Group\Interns\2019\Anubha\Constraint Project\Constraint-Project\Data\Transmission Outages\ERCOT_TransmissionOutage_2014-01-01_to_2014-12-31.xlsx",sheet_name="2014", index=False)
    # Formatting the column names of excel sheet into one form to make it easier to access column names (all lower case letters with words separated using underscore)
    inputfile14.columns = inputfile14.columns.str.strip().str.lower().str.replace(' ', '_').str.replace('(','').str.replace(')', '')
    # preserves index and stores into a column
    inputfile14.reset_index(inplace=True, drop=True)

    # The inputfile15 DataFrame stores the input file i.e. excel file that contains transmission outages data for the year 2015
    inputfile15 = pd.read_excel(r"S:\asset ops\GO_Group\Interns\2019\Anubha\Constraint Project\Constraint-Project\Data\Transmission Outages\ERCOT_TransmissionOutage_2015-01-01_to_2015-12-31.xlsx",sheet_name="2015", index=False)
    # Formatting the column names of excel sheet into one form to make it easier to access column names (all lower case letters with words separated using underscore)
    inputfile15.columns = inputfile15.columns.str.strip().str.lower().str.replace(' ', '_').str.replace('(','').str.replace(')', '')
    # preserves index and stores into a column
    inputfile15.reset_index(inplace=True, drop=True)

    # The inputfile16 DataFrame stores the input file i.e. excel file that contains transmission outages data for the year 2016
    inputfile16 = pd.read_excel(r"S:\asset ops\GO_Group\Interns\2019\Anubha\Constraint Project\Constraint-Project\Data\Transmission Outages\ERCOT_TransmissionOutage_2015-01-01_to_2015-12-31.xlsx",sheet_name="2016", index=False)
    # Formatting the column names of excel sheet into one form to make it easier to access column names (all lower case letters with words separated using underscore)
    inputfile16.columns = inputfile16.columns.str.strip().str.lower().str.replace(' ', '_').str.replace('(','').str.replace(')', '')
    # preserves index and stores into a column
    inputfile16.reset_index(inplace=True, drop=True)

    # The inputfile17 DataFrame stores the input file i.e. excel file that contains transmission outages data for the year 2017
    inputfile17 = pd.read_excel(r"S:\asset ops\GO_Group\Interns\2019\Anubha\Constraint Project\Constraint-Project\Data\Transmission Outages\ERCOT_TransmissionOutage_2017-01-01_to_2017-12-31.xlsx",sheet_name="2017", index=False)
    # Formatting the column names of excel sheet into one form to make it easier to access column names (all lower case letters with words separated using underscore)
    inputfile17.columns = inputfile17.columns.str.strip().str.lower().str.replace(' ', '_').str.replace('(','').str.replace(')', '')
    # preserves index and stores into a column
    inputfile17.reset_index(inplace=True, drop=True)

    # The inputfile18 DataFrame stores the input file i.e. excel file that contains transmission outages data for the year 2018
    inputfile18 = pd.read_excel(r"S:\asset ops\GO_Group\Interns\2019\Anubha\Constraint Project\Constraint-Project\Data\Transmission Outages\ERCOT_TransmissionOutage_2018-01-01_to_2018-12-31.xlsx",sheet_name="2018", index=False)
    # Formatting the column names of excel sheet into one form to make it easier to access column names (all lower case letters with words separated using underscore)
    inputfile18.columns = inputfile18.columns.str.strip().str.lower().str.replace(' ', '_').str.replace('(','').str.replace(')', '')
    # preserves index and stores into a column
    inputfile18.reset_index(inplace=True, drop=True)

    # The inputfile19 DataFrame stores the input file i.e. excel file that contains transmission outages data for the year 2019
    inputfile19 = pd.read_excel(r"S:\asset ops\GO_Group\Interns\2019\Anubha\Constraint Project\Constraint-Project\Data\Transmission Outages\ERCOT_TransmissionOutage_2019-01-01_to_2019-07-24.xlsx",sheet_name="2019", index=False)
    # Formatting the column names of excel sheet into one form to make it easier to access column names (all lower case letters with words separated using underscore)
    inputfile15.columns = inputfile15.columns.str.strip().str.lower().str.replace(' ', '_').str.replace('(','').str.replace(')', '')
    # preserves index and stores into a column
    inputfile15.reset_index(inplace=True, drop=True)

    # auctionMappingFile DataFrame stores the excel file that contains the auction mapping data (mapping file used is of September 2019)
    auctionMappingFile_autos = pd.read_excel(r"S:\asset ops\GO_Group\Interns\2019\Anubha\Constraint Project\Constraint-Project\Data\Mapping Documents\2019.SEP.Monthly.Auction.MappingDocument.xlsx",sheet_name="Autos", index=False)
    # Formatting the column names of excel sheet into one form to make it easier to access column names (all lower case letters with words separated using underscore)
    auctionMappingFile_autos.columns = auctionMappingFile_autos.columns.str.strip().str.lower().str.replace(' ', '_').str.replace(')', '').str.replace(')', '')
    # preserves index and converts into a column
    auctionMappingFile_autos.reset_index(inplace=True, drop=True)

    # parallel processing - to enable faster execution
    # this gives the number of processors available to use in the system
    cores = mp.cpu_count()
    # creating processes depending on the number of processors
    pool = Pool(cores)
    # splitting the dataframe into a number equivalent to the number of processors
    inputfile_split14 = np.array_split(inputfile14, cores)
    inputfile_split15 = np.array_split(inputfile15, cores)
    inputfile_split16 = np.array_split(inputfile16, cores)
    inputfile_split17 = np.array_split(inputfile17, cores)
    inputfile_split18 = np.array_split(inputfile18, cores)
    inputfile_split19 = np.array_split(inputfile19, cores)
    # using map function for parallel execution and concatenating the result back to a dataframe
    result14 = pd.concat(pool.map(outagesMapping, inputfile_split14))
    result15 = pd.concat(pool.map(outagesMapping, inputfile_split15))
    result16 = pd.concat(pool.map(outagesMapping, inputfile_split16))
    result17 = pd.concat(pool.map(outagesMapping, inputfile_split17))
    result18 = pd.concat(pool.map(outagesMapping, inputfile_split18))
    result19 = pd.concat(pool.map(outagesMapping, inputfile_split19))
    # closing all processes
    pool.close()
    # waiting untill all processes are finished
    pool.join()

    # class to auctionFormatting() function
    auctionMappingFile = auctionFormatting()

    # Saving the resultant Dataframe of year 2014 to an excel file along with auction mapping document
    writer = pd.ExcelWriter(r"S:\asset ops\GO_Group\Interns\2019\Anubha\Constraint Project\Constraint-Project\Data\UniqueTransmissionOutagesMapped2014.xlsx")
    result14.to_excel(writer, 'Outages')
    auctionMappingFile.to_excel(writer, 'AuctionMapping2019SEP_LINES')
    auctionMappingFile_autos.to_excel(writer, 'AuctionMapping2019SEP_AUTOS')
    writer.save()

    # Saving the resultant Dataframe of year 2015 to an excel file along with auction mapping document
    writer = pd.ExcelWriter(r"S:\asset ops\GO_Group\Interns\2019\Anubha\Constraint Project\Constraint-Project\Data\UniqueTransmissionOutagesMapped2015.xlsx")
    result15.to_excel(writer, 'Outages')
    auctionMappingFile.to_excel(writer, 'AuctionMapping2019SEP_LINES')
    auctionMappingFile_autos.to_excel(writer, 'AuctionMapping2019SEP_AUTOS')
    writer.save()

    # Saving the resultant Dataframe of year 2016 to an excel file along with auction mapping document
    writer = pd.ExcelWriter(r"S:\asset ops\GO_Group\Interns\2019\Anubha\Constraint Project\Constraint-Project\Data\UniqueTransmissionOutagesMapped2016.xlsx")
    result16.to_excel(writer, 'Outages')
    auctionMappingFile.to_excel(writer, 'AuctionMapping2019SEP_LINES')
    auctionMappingFile_autos.to_excel(writer, 'AuctionMapping2019SEP_AUTOS')
    writer.save()

    # Saving the resultant Dataframe of year 2017 to an excel file along with auction mapping document
    writer = pd.ExcelWriter(r"S:\asset ops\GO_Group\Interns\2019\Anubha\Constraint Project\Constraint-Project\Data\UniqueTransmissionOutagesMapped2017.xlsx")
    result17.to_excel(writer, 'Outages')
    auctionMappingFile.to_excel(writer, 'AuctionMapping2019SEP_LINES')
    auctionMappingFile_autos.to_excel(writer, 'AuctionMapping2019SEP_AUTOS')
    writer.save()

    # Saving the resultant Dataframe of year 2018 to an excel file along with auction mapping document
    writer = pd.ExcelWriter(r"S:\asset ops\GO_Group\Interns\2019\Anubha\Constraint Project\Constraint-Project\Data\UniqueTransmissionOutagesMapped2018.xlsx")
    result18.to_excel(writer, 'Outages')
    auctionMappingFile.to_excel(writer, 'AuctionMapping2019SEP_LINES')
    auctionMappingFile_autos.to_excel(writer, 'AuctionMapping2019SEP_AUTOS')
    writer.save()

    # Saving the resultant Dataframe of year 2019 to an excel file along with auction mapping document
    writer = pd.ExcelWriter(r"S:\asset ops\GO_Group\Interns\2019\Anubha\Constraint Project\Constraint-Project\Data\UniqueTransmissionOutagesMapped2019.xlsx")
    result19.to_excel(writer, 'Outages')
    auctionMappingFile.to_excel(writer, 'AuctionMapping2019SEP_LINES')
    auctionMappingFile_autos.to_excel(writer, 'AuctionMapping2019SEP_AUTOS')
    writer.save()

# call to main function
# -----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------


if __name__ =='__main__':
    main()
