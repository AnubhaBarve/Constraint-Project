# Created on: August 09,2019
# Created by: Anubha Barve (anubha.barve@constellation.com - Intern 2019)
# Purpose - this file is created to try and aprroximately match the 'Contingency name' and 'Contingency' of uniquePairList2015-2018 sheet and AuctionContingency2019JUL sheet respectively.

import pandas as pd
from fuzzywuzzy import fuzz
from tqdm import tqdm
import multiprocessing as mp
import numpy as np
from multiprocessing import Pool


#class StringMatching():
def approximatematching(inputFile1):

    inputFile2 = pd.read_excel(r"S:\asset ops\GO_Group\Interns\2019\Anubha\Constraint Project\Constraint-Project\Data\UniqueConstraintContingencyPair\uniquePairList2015-2018Copy.xlsx",sheet_name="AuctionContingency2019JUL", index=False)
    inputFile2.columns = inputFile2.columns.str.strip().str.lower().str.replace(' ', '_').str.replace('(','').str.replace(')', '')
    inputFile2.reset_index(inplace=True, drop=True)

    inputFile3 = pd.read_excel(r"S:\asset ops\GO_Group\Interns\2019\Anubha\Constraint Project\Constraint-Project\Data\UniqueConstraintContingencyPair\uniquePairList2015-2018Copy.xlsx",sheet_name="AuctionMapping2019JUL", index=False)
    inputFile3.columns = inputFile3.columns.str.strip().str.lower().str.replace(' ', '_').str.replace('(','').str.replace(')', '')
    inputFile3.reset_index(inplace=True, drop=True)

    inputFile1['auction_contingency'] = " "
    inputFile1['auction_monitored'] = " "
    inputFile1['contingency_matching_ratio'] = " "
    inputFile1['contingency_matching_partialratio'] = " "
    inputFile1['contingency_matching_tokensetratio'] = " "
    inputFile1['contingency_matching_tokensortratio'] = " "
    inputFile1['monitored_matching_ratio'] = " "
    inputFile1['monitored_matching_partialratio'] = " "
    inputFile1['monitored_matching_tokensetratio'] = " "
    inputFile1['monitored_matching_tokensortratio'] = " "

    for index,row in tqdm(inputFile1.iterrows()):
        c1 = 0
        c2 = 0
        for i1,r1 in tqdm(inputFile2.iterrows()):
            if fuzz.ratio(str(row['contingency_name']),str(r1['contingency'])) >= 50:
                c1 = c1 + 1
                if c1 <= 5:
                    if str(r1['contingency']) not in str(inputFile1.at[index,'auction_contingency']):
                        inputFile1.at[index, 'contingency_matching_ratio'] = str(inputFile1.at[index,'contingency_matching_ratio']) + "*" + str(fuzz.ratio(str(row['contingency_name']),str(r1['contingency'])))
                        inputFile1.at[index, 'contingency_matching_partialratio'] = str(inputFile1.at[index,'contingency_matching_partialratio']) + "*" + str(fuzz.partial_ratio(str(row['contingency_name']),str(r1['contingency'])))
                        inputFile1.at[index, 'contingency_matching_tokensetratio'] = str(inputFile1.at[index,'contingency_matching_tokensetratio']) + "*" + str(fuzz.token_set_ratio(str(row['contingency_name']),str(r1['contingency'])))
                        inputFile1.at[index, 'contingency_matching_tokensortratio'] = str(inputFile1.at[index,'contingency_matching_tokensortratio']) + "*" + str(fuzz.token_sort_ratio(str(row['contingency_name']),str(r1['contingency'])))
                        inputFile1.at[index,'auction_contingency'] = str(inputFile1.at[index,'auction_contingency']) + "*" + str(r1['contingency'])
                else:
                    break
        for i1,r1 in tqdm(inputFile3.iterrows()):
            if fuzz.ratio(str(row['monitored_element_name']),str(r1['op_eqcode'])) >= 50:
                c2 = c2 + 1
                if c2 <= 5:
                    if str(r1['op_eqcode']) not in str(inputFile1.at[index, 'auction_monitored']):
                        inputFile1.at[index, 'monitored_matching_ratio'] = str(inputFile1.at[index,'monitored_matching_ratio']) + "*" + str(fuzz.ratio(str(row['monitored_element_name']),str(r1['op_eqcode'])))
                        inputFile1.at[index, 'monitored_matching_partialratio'] = str(inputFile1.at[index,'monitored_matching_partialratio']) + "*" + str(fuzz.partial_ratio(str(row['monitored_element_name']),str(r1['op_eqcode'])))
                        inputFile1.at[index, 'monitored_matching_tokensetratio'] = str(inputFile1.at[index,'monitored_matching_tokensetratio']) + "*" + str(fuzz.token_set_ratio(str(row['monitored_element_name']),str(r1['op_eqcode'])))
                        inputFile1.at[index, 'monitored_matching_tokensortratio'] = str(inputFile1.at[index,'monitored_matching_tokensortratio']) + "*" + str(fuzz.token_sort_ratio(str(row['monitored_element_name']),str(r1['op_eqcode'])))
                        inputFile1.at[index,'auction_monitored'] = str(inputFile1.at[index,'auction_monitored']) + "*" + str(r1['op_eqcode'])
                else:
                    break
    return inputFile1


def main():
    #match = StringMatching()
    inputFile1 = pd.read_excel(r"S:\asset ops\GO_Group\Interns\2019\Anubha\Constraint Project\Constraint-Project\Data\UniqueConstraintContingencyPair\uniquePairList2015-2018Copy.xlsx",sheet_name="uniquePairList2015-2018", index=False)
    inputFile1.columns = inputFile1.columns.str.strip().str.lower().str.replace(' ', '_').str.replace('(','').str.replace(')', '')
    inputFile1.reset_index(inplace=True, drop=True)
    cores = mp.cpu_count()
    inputFile1_split = np.array_split(inputFile1, cores)
    pool = Pool(cores)
    result = pd.concat(pool.map(approximatematching, inputFile1_split))
    pool.close()
    pool.join()
    writer = pd.ExcelWriter(r"S:\asset ops\GO_Group\Interns\2019\Anubha\Constraint Project\Constraint-Project\Data\UniqueConstraintContingencyPair\uniquePairList2015-2018CopyStringMatching3.xlsx")
    result.to_excel(writer, 'Sheet1')
    writer.save()


if __name__ == '__main__':
    main()


