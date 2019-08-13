# Created on: August 12,2019
# Created by: Anubha Barve (anubha.barve@constellation.com - Intern 2019)
# Purpose - this file is created to try and aprroximately match the 'to' and 'op_eqcode' of Outages sheet and AuctionMapping2019JUL_LINES sheet respectively where facility_type = 'LINE'.
#           Also, approximately match the 'combine' and 'combine' columns of Outages sheet and AuctionMapping2019JUL_AUTOS sheet repectively where facility_type='XFMR'.

import pandas as pd
from fuzzywuzzy import fuzz
from tqdm import tqdm


class StringMatching():
    def approximatematching(self):

        inputFile1 = pd.read_excel(r"S:\asset ops\GO_Group\Interns\2019\Anubha\Constraint Project\Data\UniqueTransmissionOutagesMapped2015.xlsx", sheet_name="Outages", index=False)
        inputFile1.columns = inputFile1.columns.str.strip().str.lower().str.replace(' ', '_').str.replace('(','').str.replace(')', '')
        inputFile1.reset_index(inplace=True, drop=True)

        inputFile2 = pd.read_excel(r"S:\asset ops\GO_Group\Interns\2019\Anubha\Constraint Project\Data\UniqueTransmissionOutagesMapped2015.xlsx",sheet_name="AuctionMapping2019JUL_LINES", index=False)
        inputFile2.columns = inputFile2.columns.str.strip().str.lower().str.replace(' ', '_').str.replace('(','').str.replace(')', '')
        inputFile2.reset_index(inplace=True, drop=True)

        inputFile3 = pd.read_excel(r"S:\asset ops\GO_Group\Interns\2019\Anubha\Constraint Project\Data\UniqueTransmissionOutagesMapped2015.xlsx",sheet_name="AuctionMapping2019JUL_AUTOS", index=False)
        inputFile3.columns = inputFile3.columns.str.strip().str.lower().str.replace(' ', '_').str.replace('(','').str.replace(')', '')
        inputFile3.reset_index(inplace=True, drop=True)

        for index,row in tqdm(inputFile1.iterrows()):
            if str(row['facility_type']) == 'LINE':
                for i1,r1 in tqdm(inputFile2.iterrows()):
                    if fuzz.ratio(str(row['to']),str(r1['op_eqcode'])) >= 70:
                        inputFile1.at[index, 'from_matching_ratio'] = fuzz.ratio(str(row['to']),str(r1['op_eqcode']))
                        inputFile1.at[index, 'from_matching_partialratio'] = fuzz.partial_ratio(str(row['to']),str(r1['op_eqcode']))
                        inputFile1.at[index, 'from_matching_tokenratio'] = fuzz.token_set_ratio(str(row['to']),str(r1['op_eqcode']))
                        inputFile1.at[index, 'from_matching_tokenpartialratio'] = fuzz.partial_token_sort_ratio(str(row['to']),str(r1['op_eqcode']))
                        inputFile1.at[index,'auction_mapping'] = str(r1['from_#'])
                        break
            if str(row['facility_type']) == 'XFMR':
                for i1,r1 in tqdm(inputFile3.iterrows()):
                    if fuzz.ratio(str(row['combine']),str(r1['combine'])) >= 70:
                        inputFile1.at[index, 'from_matching_ratio'] = fuzz.ratio(str(row['combine']),str(r1['combine']))
                        inputFile1.at[index, 'from_matching_partialratio'] = fuzz.partial_ratio(str(row['combine']),str(r1['combine']))
                        inputFile1.at[index, 'from_matching_tokenratio'] = fuzz.token_set_ratio(str(row['combine']),str(r1['combine']))
                        inputFile1.at[index, 'from_matching_tokenpartialratio'] = fuzz.partial_token_sort_ratio(str(row['combine']),str(r1['combine']))
                        inputFile1.at[index,'auction_mapping'] = str(r1['from_#'])
                        break


        writer = pd.ExcelWriter(r"S:\asset ops\GO_Group\Interns\2019\Anubha\Constraint Project\Data\Transmission Outages Approximate String Matching\OutagesAprroximateMatching2015.xlsx")
        inputFile1.to_excel(writer,'Sheet1')
        inputFile2.to_excel(writer,'Sheet2')
        inputFile3.to_excel(writer, 'Sheet3')
        writer.save()


def main():
    match = StringMatching()
    match.approximatematching()


if __name__ == '__main__':
    main()

