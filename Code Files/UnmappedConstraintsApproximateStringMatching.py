# Created on: August 29,2019
# Created by: Anubha Barve (anubha.barve@constellation.com - Intern 2019)
# Purpose - this file is created to try and compare the columns of uniquePairList2014-2019Separation file's unmapped Constraints sheet with mapping document of september 2019 using approximate
#           string matching to check if constraints can be mapped.

import pandas as pd
from fuzzywuzzy import fuzz
from tqdm import tqdm
import multiprocessing as mp
import numpy as np
from multiprocessing import Pool


def approximatematching(inputfile):


    inputfile1 = pd.read_excel(r"S:\asset ops\GO_Group\Interns\2019\Anubha\Constraint Project\Constraint-Project\Data\Mapping Documents\2019.SEP.Monthly.Auction.MappingDocument.xlsx", sheet_name="Lines", index=False)
    inputfile1.columns = inputfile1.columns.str.strip().str.lower().str.replace(' ', '_').str.replace('(','').str.replace(')', '')
    inputfile1.reset_index(inplace=True, drop=True)

    inputfile['matching_code'] = " "
    inputfile['match_ratio'] = " "
    inputfile['match_partialratio'] = " "
    inputfile['match_sortratio'] = " "
    inputfile['match_setratio'] = " "

    for index, row in tqdm(inputfile.iterrows()):
        c1 = 0
        for i1, r1 in inputfile1.iterrows():
            if fuzz.ratio(str(row['match']),str(r1['op_eqcode'])) >= 80:
                c1 = c1 + 1
                if c1 <= 5:
                    if str(r1['op_eqcode']) not in str(inputfile.at[index, 'matching_code']):
                        inputfile.at[index,'match_ratio'] = str(inputfile.at[index, 'match_ratio']) + str(fuzz.ratio(str(row['match']),str(r1['op_eqcode']))) + " * "
                        inputfile.at[index, 'match_partialratio'] = str(inputfile.at[index, 'match_partialratio']) + str(fuzz.partial_ratio(str(row['match']), str(r1['op_eqcode']))) + " * "
                        inputfile.at[index, 'match_sortratio'] = str(inputfile.at[index, 'match_sortratio']) + str(fuzz.token_sort_ratio(str(row['match']), str(r1['op_eqcode']))) + " * "
                        inputfile.at[index, 'match_setratio'] = str(inputfile.at[index, 'match_setratio']) + str(fuzz.token_set_ratio(str(row['match']), str(r1['op_eqcode']))) + " * "
                        inputfile.at[index, 'matching_code'] = str(inputfile.at[index, 'matching_code']) + str(r1['op_eqcode']) + " * "
    return inputfile


def main():
    inputfile = pd.read_excel(r"S:\asset ops\GO_Group\Interns\2019\Anubha\Constraint Project\Constraint-Project\Data\UniqueConstraintContingencyPair\uniquePairList2014-2019Separation.xlsx",sheet_name="unmappedConstraints", index=False)
    inputfile.columns = inputfile.columns.str.strip().str.lower().str.replace(' ', '_').str.replace('(', '').str.replace(')', '')
    inputfile.reset_index(inplace=True, drop=True)

    cores = mp.cpu_count()
    pool = Pool(cores)
    inputfile_split = np.array_split(inputfile, cores)
    result = pd.concat(pool.map(approximatematching, inputfile_split))
    pool.close()
    pool.join()

    writer = pd.ExcelWriter(r"S:\asset ops\GO_Group\Interns\2019\Anubha\Constraint Project\Constraint-Project\Data\UniqueConstraintContingencyPair\UnmappedConstraintsApproximateStringMatching.xlsx")
    result.to_excel(writer, 'All')
    writer.save()



if __name__ == '__main__':
    main()


