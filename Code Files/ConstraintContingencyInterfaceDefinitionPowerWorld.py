# Created by: Sneha Bharath (sneha.bharath@constellation.com - Intern 2018)
# Modified on: October 01, 2019
# Modified by: Anubha Barve (anubha.barve@constellation - Intern 2019)
# File name: CalculationsTrail.py
# Purpose: This file is developed to define the interface elements in powerworld

# ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

import pandas as pd
from tqdm import tqdm
import datetime as dt
import win32com.client
from win32com.client import Dispatch
from win32com.client import VARIANT

# ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
# code starts

# declaring global variable
simauto = None

# the function interfaceDefinition is used to define provided interface elements in powerworld


def interfaceDefinition(powerworldFile, interfaceList, interfaceListSheetName, outputPath):

    # powerworldFile - stores the path name to powerworld case file
    # interfaceList - stores the path name of excel file containing interface elements
    # interfaceListSheetName - stores the sheet name of above mentioned file which contains interface elements
    # outputPath - stores the path name where output should be stored

    # The PowerWorld file that contains the same cases as that of the constraints data
    # provided for calculation of sensitivities
    # simauto is the object used each time to call a function in PowerWorld
    # starting powerworld
    simauto = win32com.client.Dispatch("pwrworld.SimulatorAuto")

    # opening given powerworld case file
    simauto.OpenCase(powerworldFile), 'Case open'

    # To calculate sensitivities, always enter RUN mode and not EDIT mode
    simauto.RunSCriptCommand('EnterMode(RUN)')

    # powerflow calculations performed in DC
    simauto.RunSCriptCommand('SolvePowerFlow(DC)')

    # interfaceData stores the interface elements in the form of a dataframe
    interfaceData = pd.read_excel(interfaceList, sheet_name=interfaceListSheetName, index = False)

    # droppping duplicate rows in interfaceData dataframe
    interfaceData = interfaceData.drop_duplicates(subset=["Interface Name", "Element"], keep='first')

    # iterating through interfaceData dataframe
    for i, r in tqdm(interfaceData.iterrows()):

        # if a branch/ interface element is not open pull the details of that row (defining constraint in powerworld)
        if "BRANCHOPEN" not in r['Element']:

            interfaceName = str(r['Interface Name'])

            Element = str(r['Element'])

            MeterFar = str(r['Meter Far'])

            Weight = str(r['Weight'])

            # string created in powerworld format to pass it as a parameter in powerworld function
            addInterface = 'CreateData(InterfaceElement, [''InterfaceName'', ''Element'', ''MeterFar'', ''Weight''], [{}, {}, {}, {}]);'.format(interfaceName, Element, MeterFar, Weight)

            # running powerworld script method to add interface elements
            simauto.RunSCriptCommand(addInterface)

            # defining contingency in powerworld such that for the given interface
            # element branch is open and contingency is not 'BASE CASE'
            if "BASE CASE" not in r['Contingency Name']:

                # get all the rows with interface name equivalent to currently iterated row
                # i.e interfaceName = str(r['Interface Name'])
                frame = interfaceData.loc[interfaceData['Interface Name'] == interfaceName]

                # iterating through the obtained dataframe
                for id1, ro in frame.iterrows():

                    # if interface element is a branch open i.e it is a contingency define it in powerworld
                    if "BRANCHOPEN" in ro['Element']:

                        Element = str(ro['Element'])

                        # string created in powerworld format to pass it as a parameter in powerworld function
                        addInterface = 'CreateData(InterfaceElement, [''InterfaceName'', ''Element'', ''MeterFar'', ''Weight''], [{}, {}, {}, {}]);'.format(interfaceName, Element, MeterFar, Weight)

                        # running powerworld script method to add interface elements
                        simauto.RunSCriptCommand(addInterface)  # Simauto command to run a script

    # storing the path name where result needs to be stored
    filename = outputPath +"\InterfaceDefinition_Updated.pwb"

    # saving the result as a powerworld case
    simauto.SaveCase(filename, "PWB", True)

# main function


def main():

    # stores the path name to powerworld case file
    powerworldFile = r"S:\asset ops\GO_Group\Interns\2019\Anubha\Constraint Project\Constraint-Project\Data\2019.SEP.Monthly.Auction.NetworkModel_PeakWD.RAW"

    # stores the path name of excel file containing interface elements
    interfaceList = r"S:\asset ops\GO_Group\Interns\2019\Anubha\Constraint Project\Constraint-Project\Data\UniqueConstraintContingencyPair\PowerWorldFormat.xlsx"

    # stores the sheet name of above mentioned file which contains interface elements
    interfaceListSheetName = "2019"

    # stores the path name where output should be stored
    outputPath = r"S:\asset ops\GO_Group\Interns\2019\Anubha\Constraint Project\Constraint-Project\Data"

    # call to interfaceDefinition function
    interfaceDefinition(powerworldFile, interfaceList, interfaceListSheetName, outputPath)

# call to main function


if __name__ == '__main__':
        main()