# Author: Sneha Bharath (sneha.bharath@constellation.com - Intern 2018)
# Modified on: October 01, 2019
# Created by: Anubha Barve (anubha.barve@constellation - Intern 2019)
# File name: CalculationsTrail.py
# Purpose: This file is developed to perform lodf, ptdf and tlr calculations to find relationship between a constraint and transmission outage.


import pandas as pd
from tqdm import tqdm
import datetime as dt
import win32com.client
from win32com.client import Dispatch
from win32com.client import VARIANT

def interfaceDefinition(powerworldFile, interfaceList, interfaceListSheetName, outputPath):

    # The PowerWorld file that contains the same cases as that of the constraints data provided for calculation of sensitivities
    # simauto is the object used each time to call a function in PowerWorld
    simauto = None
    simauto = win32com.client.Dispatch("pwrworld.SimulatorAuto")
    simauto.OpenCase(powerworldFile), 'Case open'

    # To calculate sensitivities, always enter RUN mode and not EDIT mode

    simauto.RunSCriptCommand('EnterMode(RUN)')

    simauto.RunSCriptCommand('SolvePowerFlow(DC)')


    interfaceData = pd.read_excel(interfaceList, sheet_name=interfaceListSheetName, index = False) #The xlsx file that contains the Interface data

    interfaceData = interfaceData.drop_duplicates(subset=["Interface Name", "Element"], keep='first')

    for i, r in tqdm(interfaceData.iterrows()):

        if "BRANCHOPEN" not in r['Element']:

            interfaceName = str(r['Interface Name'])

            Element = str(r['Element'])

            MeterFar = str(r['Meter Far'])

            Weight = str(r['Weight'])

            Interface = ["InterfaceName", "Element", "MeterFar", "Weight"]  #Field Array

            addInterface = 'CreateData(InterfaceElement, [''InterfaceName'', ''Element'', ''MeterFar'', ''Weight''], [{}, {}, {}, {}]);'.format(interfaceName, Element, MeterFar, Weight)

            print(addInterface)
            simauto.RunSCriptCommand(addInterface)#Simauto command to run a script

            if "BASE CASE" not in r['Contingency Name']:

                frame = interfaceData.loc[interfaceData['Interface Name'] == interfaceName]

                print(frame)

                for id,ro in frame.iterrows():

                    if "BRANCHOPEN" in ro['Element']:

                        Element = str(ro['Element'])

                        addInterface = 'CreateData(InterfaceElement, [''InterfaceName'', ''Element'', ''MeterFar'', ''Weight''], [{}, {}, {}, {}]);'.format(interfaceName, Element, MeterFar, Weight)

                        print(addInterface)

                        simauto.RunSCriptCommand(addInterface)  # Simauto command to run a script


    filename = outputPath +"\InterfaceDefinition_Updated.pwb"
    simauto.SaveCase(filename, "PWB", True)


def main():
    powerworldFile = r"S:\asset ops\GO_Group\Interns\2019\Anubha\Constraint Project\Constraint-Project\Data\2019.SEP.Monthly.Auction.NetworkModel_PeakWD.RAW"

    interfaceList = r"S:\asset ops\GO_Group\Interns\2019\Anubha\Constraint Project\Constraint-Project\Data\UniqueConstraintContingencyPair\PowerWorldFormat.xlsx"

    interfaceListSheetName = "2019"

    outputPath = r"S:\asset ops\GO_Group\Interns\2019\Anubha\Constraint Project\Constraint-Project\Data"

    powerworldFile1 = r"S:\asset ops\GO_Group\Interns\2019\Anubha\Constraint Project\Constraint-Project\Data\InterfaceDefinition_Updated.pwb"

    interfaceDefinition(powerworldFile, interfaceList, interfaceListSheetName, outputPath)

    #contingencydefinition(powerworldFile1, interfaceList, interfaceListSheetName, outputPath)


if __name__ == '__main__':
        main()