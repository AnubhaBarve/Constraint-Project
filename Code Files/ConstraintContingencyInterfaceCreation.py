#Created by: Sneha Bharath (Intern 2018)
#Modified by: Anubha Barve (anubha.barve@constellation.com - Intern 2019)
#Modified on: July 30, 2019
#File name: ConstraintContigencyMapping.py
#Purpose: The purpose of this file is, given a list of constraint-contingency pair it explicitly defines the pair in powerworld format.

# import statements
import pandas as pd
from tqdm import tqdm
import multiprocessing as mp
import numpy as np
from multiprocessing import Pool
# Code starts
# ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------


# A function named InterfaceMap which stores the Mapped Contingency-Monitored Element in a PowerWorld Format in an excel file.
def InterfaceMap(uniquePairs):

    # creating a new DataFrame that is mapped and matches PowerWorld Format
    powerWorldFormatMapped = pd.DataFrame(columns=["Interface Name", "Element", "Meter Far", "Weight", "Contingency Name", "Monitored Element Name"])

    print(uniquePairs.columns)
    #Multiple conditions are implemented and accordingly data is stored in the powerWorldFormat DataFrame
    for i,r in tqdm(uniquePairs.iterrows()):

        # check if the Contingency Column for From Bus Number is empty - if yes, store the details in powerWorldFormatMapped DataFrame (Base Case)
        if(r["contingency_from_bus_number"]) == " " and (r["constraint_from_bus_number"]) != " ":
            a = uniquePairs.at[i, "interface_name"]
            b = uniquePairs.at[i, "element_b_monitored"]
            c = uniquePairs.at[i, "meter_far"]
            d = uniquePairs.at[i, "weight"]
            f = uniquePairs.at[i, "contingency"]
            g = uniquePairs.at[i, "facilityname"]
            powerWorldFormatMapped = powerWorldFormatMapped.append({"Interface Name": a, "Element": b, "Meter Far": c, "Weight": d, "Contingency Name": f, "Monitored Element Name" : g}, ignore_index=True)

        # else store the details in powerWorldFormatMapped DataFrame (Constraint-Contingency)
        else:
            a = uniquePairs.at[i, "interface_name"]
            b = uniquePairs.at[i, "element_b_monitored"]
            c = uniquePairs.at[i, "meter_far"]
            d = uniquePairs.at[i, "weight"]
            e = uniquePairs.at[i, "element_a_contingency"]
            f = uniquePairs.at[i, "contingency"]
            g = uniquePairs.at[i, "facilityname"]
            powerWorldFormatMapped = powerWorldFormatMapped.append({"Interface Name": a, "Element": b, "Meter Far": c, "Weight": d, "Monitored Element Name": g, "Contingency Name": f}, ignore_index=True)
            powerWorldFormatMapped = powerWorldFormatMapped.append({"Interface Name": a, "Element": e, "Meter Far": c, "Weight": d, "Monitored Element Name": g, "Contingency Name": f}, ignore_index=True)

    # Removing the duplicate Interface Names from the Mapped List
    powerWorldFormatMappedUnique = powerWorldFormatMapped.drop_duplicates(subset=["Interface Name","Element"], keep='first')
    powerWorldFormatMappedUnique = powerWorldFormatMappedUnique.reset_index(drop=True)

    return powerWorldFormatMappedUnique



def main():

    # The uniquePairs variable reads the excel file that contains information of unique constraint-contingency pair 2014
    uniquePairs14 = pd.read_excel(r"S:\asset ops\GO_Group\Interns\2019\Anubha\Constraint Project\Constraint-Project\Data\UniqueConstraintContingencyPair\ConstraintContingencyFinalList.xlsx", sheet_name="2014", index=False)
    # parallel processing - to enable faster execution
    # this gives the number of processors available to use in the system
    cores = mp.cpu_count()
    # creating processes depending on the number of processors
    pool = Pool(cores)
    # splitting the inputfile dataframe into parts equivalent to the number of processes created
    inputfile_split14 = np.array_split(uniquePairs14, cores)
    # call to mappedconstraint function and concatenating all resulting dataframes into one'''
    result14 = pd.concat(pool.map(InterfaceMap, inputfile_split14))
    # closing all processes
    pool.close()
    # wait until all processes are finished
    pool.join()

    # The uniquePairs variable reads the excel file that contains information of unique constraint-contingency pair 2015
    uniquePairs15 = pd.read_excel(r"S:\asset ops\GO_Group\Interns\2019\Anubha\Constraint Project\Constraint-Project\Data\UniqueConstraintContingencyPair\ConstraintContingencyFinalList.xlsx", sheet_name="2015", index=False)
    # parallel processing - to enable faster execution
    # this gives the number of processors available to use in the system
    cores = mp.cpu_count()
    # creating processes depending on the number of processors
    pool = Pool(cores)
    # splitting the inputfile dataframe into parts equivalent to the number of processes created
    inputfile_split15 = np.array_split(uniquePairs15, cores)
    # call to mappedconstraint function and concatenating all resulting dataframes into one
    result15 = pd.concat(pool.map(InterfaceMap, inputfile_split15))
    # closing all processes
    pool.close()
    # wait until all processes are finished
    pool.join()

    # The uniquePairs variable reads the excel file that contains information of unique constraint-contingency pair 2015
    uniquePairs16 = pd.read_excel(r"S:\asset ops\GO_Group\Interns\2019\Anubha\Constraint Project\Constraint-Project\Data\UniqueConstraintContingencyPair\ConstraintContingencyFinalList.xlsx", sheet_name="2016", index=False)
    # parallel processing - to enable faster execution
    # this gives the number of processors available to use in the system
    cores = mp.cpu_count()
    # creating processes depending on the number of processors
    pool = Pool(cores)
    # splitting the inputfile dataframe into parts equivalent to the number of processes created
    inputfile_split16 = np.array_split(uniquePairs16, cores)
    # call to mappedconstraint function and concatenating all resulting dataframes into one
    result16 = pd.concat(pool.map(InterfaceMap, inputfile_split16))
    # closing all processes
    pool.close()
    # wait until all processes are finished
    pool.join()

    # The uniquePairs variable reads the excel file that contains information of unique constraint-contingency pair 2015
    uniquePairs17 = pd.read_excel(r"S:\asset ops\GO_Group\Interns\2019\Anubha\Constraint Project\Constraint-Project\Data\UniqueConstraintContingencyPair\ConstraintContingencyFinalList.xlsx", sheet_name="2017", index=False)
    # parallel processing - to enable faster execution
    # this gives the number of processors available to use in the system
    cores = mp.cpu_count()
    # creating processes depending on the number of processors
    pool = Pool(cores)
    # splitting the inputfile dataframe into parts equivalent to the number of processes created
    inputfile_split17 = np.array_split(uniquePairs17, cores)
    # call to mappedconstraint function and concatenating all resulting dataframes into one
    result17 = pd.concat(pool.map(InterfaceMap, inputfile_split17))
    # closing all processes
    pool.close()
    # wait until all processes are finished
    pool.join()

    # The uniquePairs variable reads the excel file that contains information of unique constraint-contingency pair 2015
    uniquePairs18 = pd.read_excel(r"S:\asset ops\GO_Group\Interns\2019\Anubha\Constraint Project\Constraint-Project\Data\UniqueConstraintContingencyPair\ConstraintContingencyFinalList.xlsx", sheet_name="2018", index=False)
    # parallel processing - to enable faster execution
    # this gives the number of processors available to use in the system
    cores = mp.cpu_count()
    # creating processes depending on the number of processors
    pool = Pool(cores)
    # splitting the inputfile dataframe into parts equivalent to the number of processes created
    inputfile_split18 = np.array_split(uniquePairs18, cores)
    # call to mappedconstraint function and concatenating all resulting dataframes into one
    result18 = pd.concat(pool.map(InterfaceMap, inputfile_split18))
    # closing all processes
    pool.close()
    # wait until all processes are finished
    pool.join()

    # The uniquePairs variable reads the excel file that contains information of unique constraint-contingency pair 2015
    uniquePairs19 = pd.read_excel(r"S:\asset ops\GO_Group\Interns\2019\Anubha\Constraint Project\Constraint-Project\Data\UniqueConstraintContingencyPair\ConstraintContingencyFinalList.xlsx", sheet_name="2019", index=False)
    # parallel processing - to enable faster execution
    # this gives the number of processors available to use in the system
    cores = mp.cpu_count()
    # creating processes depending on the number of processors
    pool = Pool(cores)
    # splitting the inputfile dataframe into parts equivalent to the number of processes created
    inputfile_split19 = np.array_split(uniquePairs19, cores)
    # call to mappedconstraint function and concatenating all resulting dataframes into one
    result19 = pd.concat(pool.map(InterfaceMap, inputfile_split19))
    # closing all processes
    pool.close()
    # wait until all processes are finished
    pool.join()

    # Saving the Mapped and Unmapped List into excel into different sheets
    writer = pd.ExcelWriter(r"S:\asset ops\GO_Group\Interns\2019\Anubha\Constraint Project\Constraint-Project\Data\UniqueConstraintContingencyPair\PowerWorldFormat.xlsx")
    result14.to_excel(writer, '2014')
    result15.to_excel(writer, '2015')
    result16.to_excel(writer, '2016')
    result17.to_excel(writer, '2017')
    result18.to_excel(writer, '2018')
    result19.to_excel(writer, '2019')
    writer.save()




if __name__ == '__main__':
        main()
