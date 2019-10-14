# Created on: September 23, 2019
# Created by: Anubha Barve (anubha.barve@constellation - Intern 2019)
# File name: CalculationsTrail.py
# Purpose: This file is developed to perform lodf, ptdf and tlr calculations to find relationship between a constraint and transmission outage.

import pandas as pd
from tqdm import tqdm
import datetime as dt
import win32com.client
from win32com.client import Dispatch
from win32com.client import VARIANT
from ConstraintContingencyInterfaceCreation import InterfaceMap
from ConstraintContingencyInterfaceDefinitionPowerWorld import interfaceDefinition


simauto = None

def createsample():
    inputfile = pd.read_excel(r"S:\asset ops\GO_Group\Interns\2019\Anubha\Constraint Project\Constraint-Project\Data\UniqueConstraintContingencyPair\ConstraintContingencyFinalList.xlsx", sheet_name="2019", index=False)
    result = inputfile.sample(n=21, axis=0, replace=True)
    result['datetime'] = pd.to_datetime(result['datetime'])
    result['date'] = result['datetime'].dt.date

    return result

def getoutages(date):
    file = pd.read_excel(r"S:\asset ops\GO_Group\Interns\2019\Anubha\Constraint Project\Constraint-Project\Data\TransmissionOutagesList.xlsx", sheet_name="2019", index=False)
    file['startdate'] = pd.to_datetime(file['startdate'])
    file['date'] = file['startdate'].dt.date

    finalfile = pd.DataFrame()
    for i,r in tqdm(file.iterrows()):
        if pd.to_datetime(r['date']) == pd.to_datetime(date):
            if r['facility_type'] != "BRKR":
                finalfile = finalfile.append(r)

    return finalfile

def powerworldLODF(dataframe,monitoredConstraints):

    finalLodfDf = pd.DataFrame()
    
    global simauto

    # rfile holds the path to the powerworld Case file
    rfile = r"S:\asset ops\GO_Group\Interns\2019\Anubha\Constraint Project\Constraint-Project\Data\Trial Data\InterfaceDefinition_Updated.pwb"

    simauto = win32com.client.Dispatch("pwrworld.SimulatorAuto")

    # Command to open the powerworld case
    simauto.OpenCase(rfile), 'Case open'

    # This is done to accept files with RAW extension
    # Simauto.OpenCaseType(rfile, 'PTI')
    # To calculate sensitivities, always enter RUN mode and not EDIT mode
    simauto.RunSCriptCommand('EnterMode(RUN)')

    # To solve  the powerFlow in DC
    simauto.RunSCriptCommand('SolvePowerFlow(DC)')

    dataframe['from_bus_number'] = dataframe['from_bus_number'].astype(int)
    dataframe['to_bus_number'] = dataframe['to_bus_number'].astype(int)
    for it,r in dataframe.iterrows():
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

    finalLodfDf.rename(columns={'Number': 'Interface Number', 'Name': 'Interface Name'}, inplace=True)
    # Write the finalLodf Dataframe to excel
    writer = pd.ExcelWriter(r"S:\asset ops\GO_Group\Interns\2019\Anubha\Constraint Project\Constraint-Project\Data\Trial Data\CalculationLODF.xlsx")
    finalLodfDf.to_excel(writer, "LODF")
    writer.save()


def powerworldPTDF(dataframe,monitoredConstraints):

    finalPTDF = pd.DataFrame()

    global simauto

    # rfile holds the path to the powerworld Case file
    rfile = r"S:\asset ops\GO_Group\Interns\2019\Anubha\Constraint Project\Constraint-Project\Data\Trial Data\InterfaceDefinition_Updated.pwb"

    simauto = win32com.client.Dispatch("pwrworld.SimulatorAuto")

    # Command to open the powerworld case
    simauto.OpenCase(rfile), 'Case open'

    # This is done to accept files with RAW extension
    # Simauto.OpenCaseType(rfile, 'PTI')
    # To calculate sensitivities, always enter RUN mode and not EDIT mode
    simauto.RunSCriptCommand('EnterMode(RUN)')

    # To solve  the powerFlow in DC
    simauto.RunSCriptCommand('SolvePowerFlow(DC)')

    dataframe['from_bus_number'] = dataframe['from_bus_number'].astype(int)
    dataframe['to_bus_number'] = dataframe['to_bus_number'].astype(int)
    for it,r in dataframe.iterrows():
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

    finalPTDF.rename(columns={'Number': 'Interface Number', 'Name': 'Interface Name'}, inplace=True)
    # Write the finalLodf Dataframe to excel
    writer = pd.ExcelWriter(r"S:\asset ops\GO_Group\Interns\2019\Anubha\Constraint Project\Constraint-Project\Data\Trial Data\CalculationPTDF.xlsx")
    finalPTDF.to_excel(writer, "PTDF")
    writer.save()


def powerworldTLR(Interfacepd, threshold, outputFile):

    global simauto

    # rfile holds the path to the powerworld Case file
    rfile = r"S:\asset ops\GO_Group\Interns\2019\Anubha\Constraint Project\Constraint-Project\Data\Trial Data\InterfaceDefinition_Updated.pwb"

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

        # for it1 in range(0, len(TLR4)):
        #    df_temp4[TLR4[it1]] = branches_output5[1][it1]
        #    if df_temp4.empty:
        #        pass
        #    df_temp4["Interface Name"] = a

        # df_temp4["SensdValuedPinj"] = pd.to_numeric(df_temp4["SensdValuedPinj"])

        # df_temp4 = df_temp4[abs(df_temp4["SensdValuedPinj"]) > float(threshold)]

        # finalTLRInjectionGroup = finalTLRInjectionGroup.append(df_temp4)

    finalTLRBus.rename(columns={'Number': 'Bus Number', 'Name': 'Bus Name'}, inplace=True)
    finalTLRGen.rename(columns={'BusNum': 'Gen Bus Number', 'BusName': 'Gen Bus Name'}, inplace=True)
    finalTLRLoad.rename(columns={'BusNum': 'Load Bus Number', 'BusName': 'Load Bus Name'}, inplace=True)
    finalTLRBus.rename(columns={'Number': 'Bus Number', 'Name': 'Bus Name'}, inplace=True)
    # finalTLRBus.rename(columns={'Name': 'InjectionGroup Name'}, inplace=True)
    writer = pd.ExcelWriter(outputFile)
    finalTLRBus.to_excel(writer, "Bus")
    finalTLRGen.to_excel(writer, "Gen")
    finalTLRLoad.to_excel(writer, "Load")
    finalTLRArea.to_excel(writer, "Area")
    # finalTLRInjectionGroup.to_excel(writer, "InjectionGroup")
    writer.save()


def main():

    constraints = createsample()

    writer = pd.ExcelWriter(r"S:\asset ops\GO_Group\Interns\2019\Anubha\Constraint Project\Constraint-Project\Data\Trial Data\Constraints.xlsx")
    constraints.to_excel(writer, "2019 Sample")
    writer.save()

    powerworldFormat = InterfaceMap(constraints)

    writer = pd.ExcelWriter(r"S:\asset ops\GO_Group\Interns\2019\Anubha\Constraint Project\Constraint-Project\Data\Trial Data\PowerWorldFormat.xlsx")
    powerworldFormat.to_excel(writer, "2019")
    writer.save()

    powerworldFile = r"S:\asset ops\GO_Group\Interns\2019\Anubha\Constraint Project\Constraint-Project\Data\2019.SEP.Monthly.Auction.NetworkModel_PeakWD.RAW"
    outputPath = r"S:\asset ops\GO_Group\Interns\2019\Anubha\Constraint Project\Constraint-Project\Data\Trial Data"
    interfaceList = r"S:\asset ops\GO_Group\Interns\2019\Anubha\Constraint Project\Constraint-Project\Data\Trial Data\PowerWorldFormat.xlsx"

    interfaceDefinition(powerworldFile, interfaceList, "2019", outputPath)

    constraintsData = pd.DataFrame()
    constraintsData['Name'] = constraints['interface_name']

    outages = pd.DataFrame()
    for i,r in tqdm(constraints.iterrows()):
        outages = outages.append(getoutages(r['date']))

    writer = pd.ExcelWriter(r"S:\asset ops\GO_Group\Interns\2019\Anubha\Constraint Project\Constraint-Project\Data\Trial Data\Outages.xlsx")
    outages.to_excel(writer, "2019 Sample")
    writer.save()

    outputFile = r"S:\asset ops\GO_Group\Interns\2019\Anubha\Constraint Project\Constraint-Project\Data\Trial Data\CalculationTLR.xlsx"



    powerworldLODF(outages, constraintsData)
    powerworldPTDF(outages, constraintsData)
    powerworldTLR(constraintsData,0.00000000, outputFile)




if __name__ == '__main__':
    main()
