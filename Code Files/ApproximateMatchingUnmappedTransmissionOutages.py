# Created on: August 20,2019
# Created by: Anubha Barve (anubha.barve@constellation.com - Intern 2019)
# Purpose - this file is created to try and match the 'fromstation' and 'from_bus_name', 'tostation' and 'to_bus_name' approximately to check whether mapping is correct or not.

import pandas as pd
from fuzzywuzzy import fuzz
from tqdm import tqdm
import multiprocessing as mp
import numpy as np
from multiprocessing import Pool


def approximatematching(inputfile):


    inputfile1 = pd.read_excel(r"S:\asset ops\GO_Group\Interns\2019\Anubha\Constraint Project\Constraint-Project\Data\UniqueTransmissionOutagesMapped2014-2019.xlsx", sheet_name="AuctionMapping2019SEP_LINES", index=False)
    inputfile1.columns = inputfile1.columns.str.strip().str.lower().str.replace(' ', '_').str.replace('(','').str.replace(')', '')
    inputfile1.reset_index(inplace=True, drop=True)

    inputfile2 = pd.read_excel(r"S:\asset ops\GO_Group\Interns\2019\Anubha\Constraint Project\Constraint-Project\Data\UniqueTransmissionOutagesMapped2014-2019.xlsx", sheet_name="AuctionMapping2019SEP_AUTOS", index=False)
    inputfile2.columns = inputfile2.columns.str.strip().str.lower().str.replace(' ', '_').str.replace('(','').str.replace(')', '')
    inputfile2.reset_index(inplace=True, drop=True)

    inputfile['from_matching_code'] = " "
    inputfile['to_matching_code'] = " "
    inputfile['to_ratio'] = " "
    inputfile['to_partialratio'] = " "
    inputfile['to_sortratio'] = " "
    inputfile['to_setratio'] = " "
    inputfile['from_ratio'] = " "
    inputfile['from_partialratio'] = " "
    inputfile['from_sortratio'] = " "
    inputfile['from_setratio'] = " "

    for index, row in tqdm(inputfile.iterrows()):
        c1 = 0
        c2 = 0
        c3 = 0
        c4 = 0

        if str(row['facility_type']) == 'LINE':
            for i1, r1 in inputfile1.iterrows():
                if fuzz.ratio(str(row['tostation']),str(r1['to_name'])) >= 80:
                    c1 = c1 + 1
                    if c1 <= 5:
                        if str(r1['to_name']) not in str(inputfile.at[index, 'to_matching_code']):
                            inputfile.at[index,'to_ratio'] = str(inputfile.at[index, 'to_ratio']) + str(fuzz.ratio(str(row['tostation']),str(r1['to_name']))) + " * "
                            inputfile.at[index, 'to_partialratio'] = str(inputfile.at[index, 'to_partialratio']) + str(fuzz.partial_ratio(str(row['tostation']), str(r1['to_name']))) + " * "
                            inputfile.at[index, 'to_sortratio'] = str(inputfile.at[index, 'to_sortratio']) + str(fuzz.token_sort_ratio(str(row['tostation']), str(r1['to_name']))) + " * "
                            inputfile.at[index, 'to_setratio'] = str(inputfile.at[index, 'to_setratio']) + str(fuzz.token_set_ratio(str(row['tostation']), str(r1['to_name']))) + " * "
                            inputfile.at[index, 'to_matching_code'] = str(inputfile.at[index, 'to_matching_code']) + str(r1['to_name']) + " * "

                if fuzz.ratio(str(row['fromstation']),str(r1['from_name'])) >= 80:
                    c3 = c3 + 1
                    if c3 <= 5:
                        if str(r1['from_name']) not in str(inputfile.at[index, 'from_matching_code']):
                            inputfile.at[index,'from_ratio'] = str(inputfile.at[index, 'from_ratio']) + str(fuzz.ratio(str(row['fromstation']),str(r1['from_name']))) + " * "
                            inputfile.at[index, 'from_partialratio'] = str(inputfile.at[index, 'from_partialratio']) + str(fuzz.partial_ratio(str(row['fromstation']), str(r1['from_name']))) + " * "
                            inputfile.at[index, 'from_sortratio'] = str(inputfile.at[index, 'from_sortratio']) + str(fuzz.token_sort_ratio(str(row['fromstation']), str(r1['from_name']))) + " * "
                            inputfile.at[index, 'from_setratio'] = str(inputfile.at[index, 'from_setratio']) + str(fuzz.token_set_ratio(str(row['fromstation']), str(r1['from_name']))) + " * "
                            inputfile.at[index, 'from_matching_code'] = str(inputfile.at[index, 'from_matching_code']) + str(r1['from_name']) + " * "

        if str(row['facility_type']) == 'XFMR':
            for i1, r1 in inputfile2.iterrows():
                if fuzz.ratio(str(row['tostation']), str(r1['to_name'])) >= 80:
                    c2 = c2 + 1
                    if c2 <= 5:
                        if str(r1['to_name']) not in str(inputfile.at[index, 'to_matching_code']):
                            inputfile.at[index, 'to_ratio'] = str(inputfile.at[index, 'to_ratio']) + str(fuzz.ratio(str(row['tostation']), str(r1['to_name']))) + " * "
                            inputfile.at[index, 'to_partialratio'] = str(inputfile.at[index, 'to_partialratio']) + str(fuzz.partial_ratio(str(row['tostation']), str(r1['to_name']))) + " * "
                            inputfile.at[index, 'to_sortratio'] = str(inputfile.at[index, 'to_sortratio']) + str(fuzz.token_sort_ratio(str(row['tostation']), str(r1['to_name']))) + " * "
                            inputfile.at[index, 'to_setratio'] = str(inputfile.at[index, 'to_setratio']) + str(fuzz.token_set_ratio(str(row['tostation']), str(r1['to_name']))) + " * "
                            inputfile.at[index, 'to_matching_code'] = str(inputfile.at[index, 'to_matching_code']) + str(r1['to_name']) + " * "

                if fuzz.ratio(str(row['fromstation']), str(r1['from_name'])) >= 80:
                    c4 = c4 + 1
                    if c4 <= 5:
                        if str(r1['from_name']) not in str(inputfile.at[index, 'from_matching_code']):
                            inputfile.at[index, 'from_ratio'] = str(inputfile.at[index, 'from_ratio']) + str(fuzz.ratio(str(row['fromstation']), str(r1['from_name']))) + " * "
                            inputfile.at[index, 'from_partialratio'] = str(inputfile.at[index, 'from_partialratio']) + str(fuzz.partial_ratio(str(row['fromstation']), str(r1['from_name']))) + " * "
                            inputfile.at[index, 'from_sortratio'] = str(inputfile.at[index, 'from_sortratio']) + str(fuzz.token_sort_ratio(str(row['fromstation']), str(r1['from_name']))) + " * "
                            inputfile.at[index, 'from_setratio'] = str(inputfile.at[index, 'from_setratio']) + str(fuzz.token_set_ratio(str(row['fromstation']), str(r1['from_name']))) + " * "
                            inputfile.at[index, 'from_matching_code'] = str(inputfile.at[index, 'from_matching_code']) + str(r1['from_name']) + " * "

    return inputfile


def main():
    inputfile = pd.read_excel(r"S:\asset ops\GO_Group\Interns\2019\Anubha\Constraint Project\Constraint-Project\Data\UniqueTransmissionOutagesMapped2014-2019.xlsx",sheet_name="Unmapped", index=False)
    inputfile.columns = inputfile.columns.str.strip().str.lower().str.replace(' ', '_').str.replace('(','').str.replace(')','')
    inputfile.reset_index(inplace=True, drop=True)

    cores = mp.cpu_count()
    pool = Pool(cores)
    inputfile_split = np.array_split(inputfile, cores)
    result = pd.concat(pool.map(approximatematching, inputfile_split))
    pool.close()
    pool.join()

    writer = pd.ExcelWriter(r"S:\asset ops\GO_Group\Interns\2019\Anubha\Constraint Project\Constraint-Project\Data\UnmappedTransmissionOutagesAprroximateStringMatch1.xlsx")
    result.to_excel(writer, 'Sheet1')
    writer.save()



if __name__ == '__main__':
    main()


