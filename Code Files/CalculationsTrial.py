# Created on: September 23, 2019
# Created by: Anubha Barve (anubha.barve@constellation - Intern 2019)
# File name: CalculationsTrail.py
# Purpose: This file is developed to perform lodf, ptdf and tlr calculations to find relationship between a constraint and transmission outage.

# -----------------------------------------------------------------------------------------------------------------------------------------------------------
# import statements

import pandas as pd
from tqdm import tqdm
import datetime as dt
import win32com.client
from win32com.client import Dispatch
from win32com.client import VARIANT
from ConstraintContingencyInterfaceCreation import InterfaceMap
from ConstraintContingencyInterfaceDefinitionPowerWorld import interfaceDefinition
import datetime
import multiprocessing as mp
from multiprocessing import  Pool
import numpy as np
# ------------------------------------------------------------------------------------------------------------------------------------------------------------------
# Code starts
# Global variable declaration for powerworld case
simauto = None

# function create sample is used to pick approximately 200 constraints from the list of ocnstraints which
# fall in the range 2019-01-01 to 2019-07-24 as transmission outages data for 2019 is available within this
# date range only. (Latest data was not pulled from Yes Energy)


def createsample(inputfile):

    # the parameter of the function - inputfile contains list of constraints in the form of a dataframe
    # sample function creates a dataframe with number of rows equivalent to the specified value of n, here n = 200
    inputfile = inputfile.sample(n=200, replace=True)

    # "date" column of inputfile dataframe is converted to datetime format for ease of use.
    inputfile['date'] = pd.to_datetime(inputfile['date'])

    # creating an empty dataframe to store date range to make comparison with inputfile dataframe easier and faster.
    date_range = pd.DataFrame()

    # populating "strtdate" column of date_range dataframe with the start date of 2019
    date_range.at[0, 'strtdate'] = "2019-01-01"

    # populating "enddate" column of date_range dataframe with the end date of 2019
    date_range.at[0, 'enddate'] = "2019-07-24"

    # "strtdate" column of date_range dataframe is converted to datetime format for ease of use.
    date_range['strtdate'] = pd.to_datetime(date_range['strtdate'])

    # "enddate" column of date_range dataframe is converted to datetime format for ease of use.
    date_range['enddate'] = pd.to_datetime(date_range['enddate'])

    # Getting all the constraints that fall within the given range (2019-01-01 to 2019-07-24) by comparing "date" column
    # of inputfile dataframe with "strtdate" and "enddate" columns of date_range dataframe
    file = inputfile.loc[(inputfile['date'] >= date_range.at[0,'strtdate']) & (inputfile['date'] <= date_range.at[0,'enddate'])]

    # returning final dataframe
    return file

# the function getoutages is used to pull all the outages that happened on the same date as
# the provided parameter to the function i.e. "date"


def getoutages(date):

    # file variable stores the transmission outages list containing all the hourly outages for 2019 in the form of
    # a dataframe
    file = pd.read_excel(r"S:\asset ops\GO_Group\Interns\2019\Anubha\Constraint Project\Constraint-Project\Data\TransmissionOutagesList.xlsx", sheet_name="2019", index=False)

    # "startdate" column of file dataframe is converted to datetime format for ease of use.
    file['startdate'] = pd.to_datetime(file['startdate'])

    # "enddate" column of file dataframe is converted to datetime format for ease of use.
    file['enddate'] = pd.to_datetime(file['enddate'])

    # "startdate" column of file dataframe is converted to date format and stored in a new column "date"
    # to extract the date part of the datetime object.
    file['date'] = file['startdate'].dt.date

    # "enddate" column of file dataframe is converted to date format and stored in a new column "end"
    # to extract the date part of the datetime object.
    file['end'] = file['enddate'].dt.date

    # creating an empty dataframe to store date parameter to make comparison with file dataframe easier and faster.
    date_single = pd.DataFrame()

    # populating "constraint_date" column of date_single dataframe with the value of "date" parameter
    date_single.at[0, 'constraint_date'] = date

    # "constraint_date" column of file dataframe is converted to date format for easier comparison
    date_single['constraint_date'] = pd.to_datetime(date_single['constraint_date']).dt.date

    # Getting all the outages that the "constraint_date" column of date_single dataframe falls within the "date"
    # and "end" column value range of file dataframe through comparison
    finalfile = file.loc[(file['date'] >= date_single.at[0,'constraint_date']) & (file['end'] <= date_single.at[0, 'constraint_date'])]

    # returning final dataframe
    return finalfile

# the function powerworldLODF is used for calculating LODF for a given set of constraints and outages.


def powerworldLODF(outages,monitoredConstraints):

    # finalLodfDf is an empty dataframe which would be used to store the final LODF calculation result.
    finalLodfDf = pd.DataFrame()
    
    # accessing the global variable declared up top (simauto)
    global simauto

    # rfile holds the path to the powerworld Case file
    rfile = r"S:\asset ops\GO_Group\Interns\2019\Anubha\Constraint Project\Constraint-Project\Data\Trial Data\InterfaceDefinition_Updated.pwb"

    # starts PowerWorld
    simauto = win32com.client.Dispatch("pwrworld.SimulatorAuto")

    # Command to open the powerworld case
    simauto.OpenCase(rfile), 'Case open'

    # This is done to accept files with RAW extension
    # Simauto.OpenCaseType(rfile, 'PTI')
    # To calculate sensitivities, always enter RUN mode and not EDIT mode
    simauto.RunSCriptCommand('EnterMode(RUN)')

    # To solve  the powerFlow in DC
    simauto.RunSCriptCommand('SolvePowerFlow(DC)')

    # converting the data type of "from_bus_number" column of outages dataframe for ease of use
    outages['from_bus_number'] = outages['from_bus_number'].astype(int)

    # converting the data type of "to_bus_number" column of outages dataframe for ease of use
    outages['to_bus_number'] = outages['to_bus_number'].astype(int)

    # iterating through outages dataframe
    for it,r in outages.iterrows():
        # a holds the From Bus number
        a = r['from_bus_number']

        # b holds the to bus number
        b = r['to_bus_number']

        # c holds the circuit ID
        c = r['circuit_id']

        # d holds the from bus name
        d = r["from_bus_name"]

        # e holds the To bus name
        e = r["to_bus_name"]

        # Command to calculate the LODF given from bus number, to bus number and Circuit ID
        LODFCalc = 'CalculateLODF([BRANCH {} {} {}],DC)'.format(a, b, c)

        # Run the LODF command
        simauto.RunScriptCommand(LODFCalc)

        # ConstraintData is a variant list storing a list of powerWorld object field variables to retrieve from the simulator
        constraintData = ["Number", "Name", "LODF", "MW", "LODFCTGMW"]

        # In powerWorld Constraint-Contingency pairs are defined as Interfaces

        # GetParametersMultipleElement command is used to pull data from powerWorld given a list of parameters
        # The first parameter in GetParametersMultipleElement is the type of parameter you are getting the parameters for
        # The branches_output stores a set of list of lists returned by GetParametersMultipleElement containing the parameter values for the Interface device type as requested
        branches_output = simauto.GetParametersMultipleElement("Interface", constraintData, ' ')

        # df_temp is a temporarty dataframe that holds the Interface(Constraint - Contingency pair) data returned by powerworld for one monitored Constraint which will dynamically be appended to the finalLodfDf at each iteration
        df_temp = pd.DataFrame()

        # To add the outage information for Identification of the constraint data pertaining to the respective outage data
        for it1 in range(0, len(constraintData)):
            df_temp[constraintData[it1]] = branches_output[1][it1]
            df_temp["from_bus_number"] = a
            df_temp["from_bus_name"] = d
            df_temp["to_bus_number"] = b
            df_temp["to_bus_name"] = e
            df_temp["circuit_id"] = c

        # Merging the df_temp and the monitoredConstraints to filter data based on  the monitored Constraints
        df_temp = pd.merge(df_temp, monitoredConstraints, how='inner', on=["Name"])

        # Drop empty rows
        df_temp.dropna(inplace=True)

        # Append the filtered dataframe to the final LODF dataframe
        finalLodfDf = finalLodfDf.append(df_temp)

    # renaming the columns of finalLodfDf dataframe
    finalLodfDf.rename(columns={'Number': 'Interface Number', 'Name': 'Interface Name'}, inplace=True)

    # creating an excel file
    writer = pd.ExcelWriter(r"S:\asset ops\GO_Group\Interns\2019\Anubha\Constraint Project\Constraint-Project\Data\Trial Data\CalculationLODF.xlsx")

    # writing to the created excel file and storing the sheet as "LODF"
    finalLodfDf.to_excel(writer, "LODF")

    # saving the excel file
    writer.save()

# the function powerworldPTDF is used to calculate PTDF for a given set of outages and constraints


def powerworldPTDF(outages,monitoredConstraints):

    # finalLodfDf is an empty dataframe which would be used to store the final PTDF calculation result.
    finalPTDF = pd.DataFrame()

    global simauto

    # rfile holds the path to the powerworld Case file
    rfile = r"S:\asset ops\GO_Group\Interns\2019\Anubha\Constraint Project\Constraint-Project\Data\Trial Data\InterfaceDefinition_Updated.pwb"

    # starts PowerWorld
    simauto = win32com.client.Dispatch("pwrworld.SimulatorAuto")

    # Command to open the powerworld case
    simauto.OpenCase(rfile), 'Case open'

    # This is done to accept files with RAW extension
    # Simauto.OpenCaseType(rfile, 'PTI')
    # To calculate sensitivities, always enter RUN mode and not EDIT mode
    simauto.RunSCriptCommand('EnterMode(RUN)')

    # To solve  the powerFlow in DC
    simauto.RunSCriptCommand('SolvePowerFlow(DC)')

    # converting the data type of "from_bus_number" column of outages dataframe for ease of use
    outages['from_bus_number'] = outages['from_bus_number'].astype(int)

    # converting the data type of "to_bus_number" column of outages dataframe for ease of use
    outages['to_bus_number'] = outages['to_bus_number'].astype(int)

    # iterating through outages dataframe
    for it,r in outages.iterrows():
        # a holds the From Bus number
        a = r['from_bus_number']

        # b holds the to bus number
        b = r['to_bus_number']

        # c holds the circuit ID
        c = r['circuit_id']

        # d holds the from bus name
        d = r["from_bus_name"]

        # e holds the To bus name
        e = r["to_bus_name"]

        # Command to calculate the LODF given from bus number, to bus number and Circuit ID
        PTDFCalc = 'CalculatePTDF([BUS {}],[BUS {}],DC)'.format(a, b)

        # Run the LODF command
        simauto.RunScriptCommand(PTDFCalc)

        # ConstraintData is a variant list storing a list of powerWorld object field variables to retrieve from the simulator
        constraintData = ["Name", "Number", "PTDF", "MW", "HasCTG"]

        # In powerWorld Constraint-Contingency pairs are defined as Interfaces

        # GetParametersMultipleElement command is used to pull data from powerWorld given a list of parameters
        # The first parameter in GetParametersMultipleElement is the type of parameter you are getting the parameters for
        # The branches_output stores a set of list of lists returned by GetParametersMultipleElement containing the parameter values for the Interface device type as requested
        branches_output = simauto.GetParametersMultipleElement("Interface", constraintData, ' ')

        # df_temp is a temporarty dataframe that holds the Interface(Constraint - Contingency pair) data returned by powerworld for one monitored Constraint which will dynamically be appended to the finalLodfDf at each iteration
        df_temp = pd.DataFrame()

        # To add the outage information for Identification of the constraint data pertaining to the respective outage data
        for it1 in range(0, len(constraintData)):
            df_temp[constraintData[it1]] = branches_output[1][it1]
            df_temp["from_bus_number"] = a
            df_temp["from_bus_name"] = d
            df_temp["to_bus_number"] = b
            df_temp["to_bus_name"] = e
            df_temp["circuit_id"] = c

        # Merging the df_temp and the monitoredConstraints to filter data based on  the monitored Constraints
        df_temp = pd.merge(df_temp, monitoredConstraints, how='inner', on=["Name"])

        # Drop empty rows
        df_temp.dropna(inplace=True)

        # Append the filtered dataframe to the final LODF dataframe
        finalPTDF = finalPTDF.append(df_temp)

    # renaming the columns of finalLodfDf dataframe
    finalPTDF.rename(columns={'Number': 'Interface Number', 'Name': 'Interface Name'}, inplace=True)

    # creating an excel file
    writer = pd.ExcelWriter(r"S:\asset ops\GO_Group\Interns\2019\Anubha\Constraint Project\Constraint-Project\Data\Trial Data\CalculationPTDF.xlsx")

    # writing a sheet named "PTDF" to created excel file
    finalPTDF.to_excel(writer, "PTDF")

    # save excel file
    writer.save()

# the function powerworldTLR is used to calculate TLR for the given parameters


def powerworldTLR(Interfacepd, threshold, outputFile):

    # accessing global variable simauto
    global simauto

    # rfile holds the path to the powerworld Case file
    rfile = r"S:\asset ops\GO_Group\Interns\2019\Anubha\Constraint Project\Constraint-Project\Data\Trial Data\InterfaceDefinition_Updated.pwb"

    # starts powerworld
    simauto = win32com.client.Dispatch("pwrworld.SimulatorAuto")

    # Command to open the powerworld case
    simauto.OpenCase(rfile), 'Case open'

    # This is done to accept files with RAW extension
    # Simauto.OpenCaseType(rfile, 'PTI')
    # To calculate sensitivities, always enter RUN mode and not EDIT mode
    simauto.RunSCriptCommand('EnterMode(RUN)')

    # To solve  the powerFlow in DC
    simauto.RunSCriptCommand('SolvePowerFlow(DC)')

    # The finalTLR dataframe holds the final data for all the monitored Constraints
    finalTLRBus = pd.DataFrame()
    finalTLRGen = pd.DataFrame()
    finalTLRLoad = pd.DataFrame()
    finalTLRArea = pd.DataFrame()
    finalTLRInjectionGroup = pd.DataFrame()

    # TLR is a variant list storing a list of powerWorld object field variables to retrieve from the simulator
    TLR = ["Number", "Name", "AreaNumber", "AreaName", "SensdValuedPinj"]
    TLR1 = ["BusNum", "BusName", "ID", "AreaName", "AGC", "SensdValuedPinj", "MW", "MWMin", "MWMax",
            "SensdValuedVset", "VoltSet"]
    TLR2 = ["BusNum", "BusName", "ID", "AreaName", "SensdValuedPinj", "MW"]
    TLR3 = ["Number", "Name", "AGC", "GenMW", "LoadMW", "SensdValuedPinj"]

    # To add a function to check if there is an Injectiongroup or not
    # TLR4 = ["Name", "GenMW", "LoadMW", "SensdValuedPinj"]

    # Iterate over the rows in the Interfacepd to calculate the TLR given a single Constraint-Contingency pair / Interface Name
    for it,r in tqdm(Interfacepd.iterrows()):
        a = r['Name']

        Outtlr = simauto.RunScriptCommand('CalculateTLR([INTERFACE "{}"],Buyer,[SLACK],DC)'.format(a))

        branches_output1 = simauto.GetParametersMultipleElement("Bus", TLR, "")

        branches_output2 = simauto.GetParametersMultipleElement("Gen", TLR1, "")

        branches_output3 = simauto.GetParametersMultipleElement("Load", TLR2, "")

        branches_output4 = simauto.GetParametersMultipleElement("Area", TLR3, "")

        # For Injection Groups
        # branches_output5 = simauto.GetParametersMultipleElement("InjectionGroup", TLR4, "")

        df_temp = pd.DataFrame()
        df_temp1 = pd.DataFrame()
        df_temp2 = pd.DataFrame()
        df_temp3 = pd.DataFrame()
        # df_temp4 = pd.DataFrame()

        for it1 in range(0, len(TLR)):
            df_temp[TLR[it1]] = branches_output1[1][it1]
            df_temp["Interface Name"] = a

        df_temp["SensdValuedPinj"] = pd.to_numeric(df_temp["SensdValuedPinj"])

        df_temp = df_temp[abs(df_temp["SensdValuedPinj"]) > float(threshold)]

        finalTLRBus = finalTLRBus.append(df_temp)

        for it1 in range(0, len(TLR1)):
            df_temp1[TLR1[it1]] = branches_output2[1][it1]
            df_temp1["Interface Name"] = a

        df_temp1["SensdValuedPinj"] = pd.to_numeric(df_temp1["SensdValuedPinj"])

        df_temp1 = df_temp1[abs(df_temp1["SensdValuedPinj"]) > float(threshold)]

        finalTLRGen = finalTLRGen.append(df_temp1)

        for it1 in range(0, len(TLR2)):
            df_temp2[TLR2[it1]] = branches_output3[1][it1]
            df_temp2["Interface Name"] = a

        df_temp2["SensdValuedPinj"] = pd.to_numeric(df_temp2["SensdValuedPinj"])

        df_temp2 = df_temp2[abs(df_temp2["SensdValuedPinj"]) > float(threshold)]

        finalTLRLoad = finalTLRLoad.append(df_temp2)

        for it1 in range(0, len(TLR3)):
            df_temp3[TLR3[it1]] = branches_output4[1][it1]
            df_temp3["Interface Name"] = a

        df_temp3["SensdValuedPinj"] = pd.to_numeric(df_temp3["SensdValuedPinj"])

        df_temp3 = df_temp3[abs(df_temp3["SensdValuedPinj"]) > float(threshold)]

        finalTLRArea = finalTLRArea.append(df_temp3)

    # renaming columns of various dataframes
    finalTLRBus.rename(columns={'Number': 'Bus Number', 'Name': 'Bus Name'}, inplace=True)
    finalTLRGen.rename(columns={'BusNum': 'Gen Bus Number', 'BusName': 'Gen Bus Name'}, inplace=True)
    finalTLRLoad.rename(columns={'BusNum': 'Load Bus Number', 'BusName': 'Load Bus Name'}, inplace=True)
    finalTLRBus.rename(columns={'Number': 'Bus Number', 'Name': 'Bus Name'}, inplace=True)

    # creating an excel file
    writer = pd.ExcelWriter(outputFile)

    # writing to the created excel file
    finalTLRBus.to_excel(writer, "Bus")
    finalTLRGen.to_excel(writer, "Gen")
    finalTLRLoad.to_excel(writer, "Load")
    finalTLRArea.to_excel(writer, "Area")

    # saving the excel file
    writer.save()

# ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
# main function


def main():

    print("Sample Creation")

    # inputfile variable stores the list of hourly constraint-contingency for 2019 in the form a dataframe
    inputfile = pd.read_excel(r"S:\asset ops\GO_Group\Interns\2019\Anubha\Constraint Project\Constraint-Project\Data\UniqueConstraintContingencyPair\ConstraintContingencyFinalList.xlsx",sheet_name="2019", index=False)

    # converting the "datetime" column of inputfile dataframe to datetime object for ease of use
    inputfile['datetime'] = pd.to_datetime(inputfile['datetime'])

    # extracting the date from the datetime object and storing it into a new column "date"
    inputfile['date'] = inputfile['datetime'].dt.date

    # creating pool of processes based on the number of processors available in the system
    pool = Pool(mp.cpu_count())
    # splitting the inputfile dataframe into multiple chunks for parallel processing
    inputfile_split = np.array_split(inputfile, mp.cpu_count())

    # call to createsample function
    constraints = pd.concat(pool.map(createsample, inputfile_split))

    # creating an excel file
    writer = pd.ExcelWriter(r"S:\asset ops\GO_Group\Interns\2019\Anubha\Constraint Project\Constraint-Project\Data\Trial Data\Constraints.xlsx")

    # writing an excel sheet name "2019 Sample" to the created file
    constraints.to_excel(writer, "2019 Sample")

    # saving the excel file
    writer.save()

    print("Outage selection")
    # creating an empty dataframe to store the outages
    outages = pd.DataFrame()

    # dropping the duplicates from constraints dataframe keeping the unique dates
    constraints_unique = constraints.drop_duplicates(subset='date', keep='first', inplace=False)

    # iterating over constraints_unique dataframe
    for i, r in tqdm(constraints_unique.iterrows()):

        # call to getoutages function where "date" column of the constraints_unique is passed as a parameter
        outages = outages.append(getoutages(pd.to_datetime(r['date'])))

    # creating an excel file
    writer = pd.ExcelWriter(r"S:\asset ops\GO_Group\Interns\2019\Anubha\Constraint Project\Constraint-Project\Data\Trial Data\Outages.xlsx")

    # writing an excel sheet named "2019 Sample" to created excel file
    outages.to_excel(writer, "2019 Sample")

    # saving excel file
    writer.save()

    print("PowerWorld Format")
    # splitting the constraints dataframe in chunks for parallel processing
    constraints_split = np.array_split(constraints, mp.cpu_count())
    # call to InterfaceMap function to convert the constraints data in powerworld format
    powerworldFormat = pd.concat(pool.map(InterfaceMap, constraints_split))

    # creating an excel file
    writer = pd.ExcelWriter(r"S:\asset ops\GO_Group\Interns\2019\Anubha\Constraint Project\Constraint-Project\Data\Trial Data\PowerWorldFormat.xlsx")
    # writing an excel sheet named "2019" to created excel sheet
    powerworldFormat.to_excel(writer, "2019")
    # saving the excel file
    writer.save()

    print("PowerWorld Definition")
    # powerworldFile variable storing the path name of the powerworld case
    powerworldFile = r"S:\asset ops\GO_Group\Interns\2019\Anubha\Constraint Project\Constraint-Project\Data\2019.SEP.Monthly.Auction.NetworkModel_PeakWD.RAW"
    # outputPath variable storing the path name of the output
    outputPath = r"S:\asset ops\GO_Group\Interns\2019\Anubha\Constraint Project\Constraint-Project\Data\Trial Data"
    # interfaceList variable storing the path of the file containing constraint data in powerworld format
    interfaceList = r"S:\asset ops\GO_Group\Interns\2019\Anubha\Constraint Project\Constraint-Project\Data\Trial Data\PowerWorldFormat.xlsx"

    # call to interfaceDefinition function which defines the list of interfaces into powerworld
    interfaceDefinition(powerworldFile, interfaceList, "2019", outputPath)

    # outputFile variable stores the file path of the result
    outputFile = r"S:\asset ops\GO_Group\Interns\2019\Anubha\Constraint Project\Constraint-Project\Data\Trial Data\CalculationTLR.xlsx"

    # creating an empty dataframe
    constraintsData = pd.DataFrame()
    # creating column named "Name" in constraintData dataframe to use it as a function
    # parameter while calling powerworldTLR function
    constraintsData['Name'] = constraints['interface_name']

    # Prompting user to enter a value
    print("1. LODF ")
    print("2. PTDF ")
    print("3. TLR ")

    value = raw_input("Enter your choice : ")

    # if entered value is 1 call powerworldLODF function
    if value == 1:
        powerworldLODF(outages, constraintsData)
    # if entered value is 2 call powerworldPTDF function
    elif value == 2:
        powerworldPTDF(outages, constraintsData)
    # if entered value is 3 call powerworldTLR function
    else:
        powerworldTLR(constraintsData,0.00000000, outputFile)

# call to main function


if __name__ == '__main__':
    main()