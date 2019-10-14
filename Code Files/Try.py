import pandas as pd
from tqdm import tqdm

import pandas as pd
from tqdm import tqdm
import multiprocessing as mp
import numpy as np
from multiprocessing import Pool
import math

'''def combine(df):

    # finalOutages is an empty dataframe created to store the final output
    finalOutages_all = pd.DataFrame()
    df['from_bus_number'].fillna(" ", inplace=True)
    # Storing only those transmission outages that are mapped i.e. the ones that have a value for 'from_bus_number' column
    for i2,r2 in tqdm(df.iterrows()):
            if r2['from_bus_number'] != " ":
                finalOutages_all = finalOutages_all.append(r2)

    finalOutages_all['from_bus_number'] = finalOutages_all['from_bus_number'].astype(int)
    print(finalOutages_all['from_bus_number'])
    print(finalOutages_all['from_bus_number'].dtype)
    return finalOutages_all'''

def main():
    '''
    final_file is a string that stores the name of the final transmission outages file
    final_file = r"S:\asset ops\GO_Group\Interns\2019\Anubha\Constraint Project\Constraint-Project\Data\TransmissionOutagesFinal.xlsx"

    # dataFrame19 reads the excel file whose name is stored in year_2019 as a dataframe
    dataFrame19 = pd.read_excel(final_file, sheet_name="2019", index=False)
    # Formatting the column names of excel sheet into one form to make it easier to access column names (all lower case letters with words separated using underscore)
    dataFrame19.columns = dataFrame19.columns.str.strip().str.lower().str.replace(' ', '_').str.replace('(','').str.replace(')', '')
    # this line preserves the index and converts it into a column
    dataFrame19.reset_index(inplace=True, drop=True)
    print(dataFrame19['from_bus_number'])
    print(dataFrame19['from_bus_number'].dtype)
    cores = mp.cpu_count()
    dataFrame19_split = np.array_split(dataFrame19, cores)
    pool = Pool(cores)
    result19 = pd.concat(pool.map(combine, dataFrame19_split))
    resultmapped19 = result19.drop_duplicates(subset=['facility', 'startdate'], keep='first', inplace=False)
    pool.close()
    pool.join()'''

    interfaceList = r"S:\asset ops\GO_Group\Interns\2019\Anubha\Constraint Project\Constraint-Project\Data\Trial Data\PowerWorldFormat.xlsx"

    interfaceListSheetName = "2019"

    interfaceData = pd.read_excel(interfaceList, sheet_name=interfaceListSheetName,index=False)  # The xlsx file that contains the Interface data

    interfaceData.reset_index(inplace=True,drop=True)  # to reset the index as importing might create duplicate index values

    interfaceData.loc[interfaceData.index.dropna()]

    #print(interfaceData)

    for i, r in tqdm(interfaceData.iterrows()):
        interfaceName = r['Interface Name']

        Element = r['Element']

        MeterFar = r['Meter Far']

        Weight = r['Weight']

        frame = interfaceData.loc[interfaceData['Interface Name'] == interfaceName]

        print(frame)

        for i,r in frame.iterrows():
            if "BRANCHOPEN" in r['Element']:
                print(r)
        break



if __name__ == '__main__':
    main()