# Created on: August 27,2019
# Created by: Anubha Barve (anubha.barve@constellation.com - Intern 2019)
# Purpose - this file is created to separate the mapped and unmapped transmission outages from the Unmapped transmission Outages file. Also, verify the mapped outages by
# approximately matching the 'fromstation' and 'tostation' with the 'from bus name' and 'to bus name' of the mapping document.

import pandas as pd
from fuzzywuzzy import fuzz
from tqdm import tqdm
import multiprocessing as mp
import numpy as np
from multiprocessing import Pool


def separationmapped(inputfile):

    mapped = pd.DataFrame()

    for i, r in tqdm(inputfile.iterrows()):
        if str(r['matching_code_mapped']) != " ":
            mapped = mapped.append(r)

    mapped['from_matching'] = " "
    mapped['from_ratio'] = " "
    mapped['to_matching'] = " "
    mapped['to_ratio'] = " "

    for index,row in tqdm(mapped.iterrows()):
                if fuzz.ratio(str(row['fromstation']),str(row['from_bus_name'])) >= fuzz.ratio(str(row['tostation']),str(row['from_bus_name'])):
                    mapped.at[index, 'from_ratio'] = str(fuzz.ratio(str(row['fromstation']),str(row['from_bus_name'])))
                    mapped.at[index, 'from_matching'] = str(row['fromstation'])

                else:
                    mapped.at[index, 'from_ratio'] = str(fuzz.ratio(str(row['tostation']),str(row['from_bus_name'])))
                    mapped.at[index, 'from_matching'] = str(row['tostation'])

                if fuzz.ratio(str(row['fromstation']),str(row['to_bus_name'])) >= fuzz.ratio(str(row['tostation']),str(row['to_bus_name'])):
                    mapped.at[index, 'to_ratio'] = str(fuzz.ratio(str(row['fromstation']),str(row['to_bus_name'])))
                    mapped.at[index, 'to_matching'] = str(row['fromstation'])

                else:
                    mapped.at[index, 'to_ratio'] = str(fuzz.ratio(str(row['tostation']),str(row['to_bus_name'])))
                    mapped.at[index, 'to_matching'] = str(row['tostation'])

    return mapped

def separationunmapped(inputfile):

    unmapped = pd.DataFrame()

    for index, row in tqdm(inputfile.iterrows()):
        if str(row['matching_code_mapped']) == " ":
            unmapped = unmapped.append(row)

    return unmapped


def main():
    inputfile = pd.read_excel(r"S:\asset ops\GO_Group\Interns\2019\Anubha\Constraint Project\Constraint-Project\Data\UnmappedTransmissionOutagesAprroximateStringMatch1.xlsx",sheet_name="unmappedAll", index=False)
    inputfile.columns = inputfile.columns.str.strip().str.lower().str.replace(' ', '_').str.replace('(', '').str.replace(')', '')
    inputfile.reset_index(inplace=True, drop=True)

    cores = mp.cpu_count()
    pool = Pool(cores)
    inputfile_split = np.array_split(inputfile, cores)
    result = pd.concat(pool.map(separationmapped, inputfile_split))
    result1 = pd.concat(pool.map(separationunmapped, inputfile_split))
    pool.close()
    pool.join()

    writer = pd.ExcelWriter(r"S:\asset ops\GO_Group\Interns\2019\Anubha\Constraint Project\Constraint-Project\Data\UnmappedTransmissionOutagesAprroximateSeparation.xlsx")
    result.to_excel(writer, 'Sheet1')
    result1.to_excel(writer, 'Sheet2')
    writer.save()



if __name__ == '__main__':
    main()


