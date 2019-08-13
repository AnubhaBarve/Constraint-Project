# Created on: August 09,2019
# Created by: Anubha Barve (anubha.barve@constellation.com - Intern 2019)
# Purpose - this file is created to try and aprroximately match the 'Contingency name' and 'Contingency' of uniquePairList2015-2018 sheet and AuctionContingency2019JUL sheet respectively.

import pandas as pd
from fuzzywuzzy import fuzz
from tqdm import tqdm


class StringMatching():
    def approximatematching(self):

        inputFile1 = pd.read_excel(r"S:\asset ops\GO_Group\Interns\2019\Anubha\Constraint Project\Data\UniqueConstraintContingencyPair\uniquePairList2015-2018Copy.xlsx", sheet_name="uniquePairList2015-2018", index=False)
        inputFile1.columns = inputFile1.columns.str.strip().str.lower().str.replace(' ', '_').str.replace('(','').str.replace(')', '')
        inputFile1.reset_index(inplace=True, drop=True)

        inputFile2 = pd.read_excel(r"S:\asset ops\GO_Group\Interns\2019\Anubha\Constraint Project\Data\UniqueConstraintContingencyPair\uniquePairList2015-2018Copy.xlsx",sheet_name="AuctionContingency2019JUL", index=False)
        inputFile2.columns = inputFile2.columns.str.strip().str.lower().str.replace(' ', '_').str.replace('(','').str.replace(')', '')
        inputFile2.reset_index(inplace=True, drop=True)

        for index,row in tqdm(inputFile1.iterrows()):
            for i1,r1 in tqdm(inputFile2.iterrows()):
                if fuzz.ratio(str(row['contingency_name']),str(r1['contingency'])) >= 70:
                    inputFile1.at[index, 'contingency_matching_ratio'] = fuzz.ratio(str(row['contingency_name']),str(r1['contingency']))
                    inputFile1.at[index, 'contingency_matching_partialratio'] = fuzz.partial_ratio(str(row['contingency_name']),str(r1['contingency']))
                    inputFile1.at[index, 'contingency_matching_tokenratio'] = fuzz.token_set_ratio(str(row['contingency_name']),str(r1['contingency']))
                    inputFile1.at[index, 'contingency_matching_tokenpartialratio'] = fuzz.partial_token_sort_ratio(str(row['contingency_name']),str(r1['contingency']))
                    inputFile1.at[index,'auction_contingency'] = str(r1['contingency'])
                    break


        writer = pd.ExcelWriter(r"S:\asset ops\GO_Group\Interns\2019\Anubha\Constraint Project\Data\UniqueConstraintContingencyPair\uniquePairList2015-2018CopyStringMatching1.xlsx")
        inputFile1.to_excel(writer,'Sheet1')
        inputFile2.to_excel(writer,'Sheet2')
        writer.save()


def main():
    match = StringMatching()
    match.approximatematching()


if __name__ == '__main__':
    main()


