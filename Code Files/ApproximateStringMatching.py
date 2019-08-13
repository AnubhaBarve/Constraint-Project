# Created on: August 07,2019
# Created by: Anubha Barve (anubha.barve@constellation.com - Intern 2019)
# Purpose - this file is created to try and match the 'fromstation' and 'from_bus_name', 'tostation' and 'to_bus_name' approximately to check whether mapping is correct or not.

import pandas as pd
from fuzzywuzzy import fuzz


class StringMatching():
    def approximatematching(self):

        inputFile = pd.read_excel(r"S:\asset ops\GO_Group\Interns\2019\Anubha\Constraint Project\Data\Transmission Outages Verification\AprroximateStringMatchingTrial.xlsx", sheet_name="Outages", index=False)
        inputFile.reset_index(inplace=True, drop=True)

        for index,row in inputFile.iterrows():
            if str(row['facility_type']) == 'LINE':
                inputFile.at[index,'from_ratio'] = fuzz.ratio(str(row['from_split_1']),str(row['from_bus_name']))
                inputFile.at[index, 'fromstation_ratio'] = fuzz.ratio(str(row['fromstation']), str(row['from_bus_name']))
                inputFile.at[index, 'to_ratio'] = fuzz.ratio(str(row['from_split_2']), str(row['to_bus_name']))
                inputFile.at[index, 'tostation_ratio'] = fuzz.ratio(str(row['tostation']), str(row['to_bus_name']))
                inputFile.at[index, 'from_partialratio'] = fuzz.partial_ratio(str(row['from_split_1']), str(row['from_bus_name']))
                inputFile.at[index, 'fromstation_partialratio'] = fuzz.partial_ratio(str(row['fromstation']), str(row['from_bus_name']))
                inputFile.at[index, 'to_partialratio'] = fuzz.partial_ratio(str(row['from_split_2']), str(row['to_bus_name']))
                inputFile.at[index, 'tostation_partialratio'] = fuzz.partial_ratio(str(row['tostation']), str(row['to_bus_name']))

            if str(row['facility_type']) == 'XFMR':
                inputFile.at[index,'from_ratio'] = fuzz.ratio(str(row['from']),str(row['from_bus_name']))
                inputFile.at[index, 'fromstation_ratio'] = fuzz.ratio(str(row['fromstation']), str(row['from_bus_name']))
                inputFile.at[index, 'to_ratio'] = fuzz.ratio(str(row['from']), str(row['to_bus_name']))
                inputFile.at[index, 'tostation_ratio'] = fuzz.ratio(str(row['tostation']), str(row['to_bus_name']))
                inputFile.at[index, 'from_partialratio'] = fuzz.partial_ratio(str(row['from']), str(row['from_bus_name']))
                inputFile.at[index, 'fromstation_partialratio'] = fuzz.partial_ratio(str(row['fromstation']), str(row['from_bus_name']))
                inputFile.at[index, 'to_partialratio'] = fuzz.partial_ratio(str(row['from']), str(row['to_bus_name']))
                inputFile.at[index, 'tostation_partialratio'] = fuzz.partial_ratio(str(row['tostation']), str(row['to_bus_name']))

        for i1,r1 in inputFile.iterrows():
            if r1['from_ratio'] > r1['fromstation_ratio']:
                if r1['from_ratio'] > r1['from_partialratio']:
                    inputFile.at[i1,'from_matching'] = r1['from_ratio']
                else:
                    inputFile.at[i1,'from_matching'] = r1['from_partialratio']
            else:
                if r1['fromstation_ratio'] > r1['fromstation_partialratio']:
                    inputFile.at[i1,'from_matching'] = r1['fromstation_ratio']
                else:
                    inputFile.at[i1,'from_matching'] = r1['fromstation_partialratio']

        for i1, r1 in inputFile.iterrows():
            if r1['to_ratio'] > r1['tostation_ratio']:
                if r1['to_ratio'] > r1['to_partialratio']:
                    inputFile.at[i1, 'to_matching'] = r1['to_ratio']
                else:
                    inputFile.at[i1, 'to_matching'] = r1['to_partialratio']
            else:
                if r1['tostation_ratio'] > r1['tostation_partialratio']:
                    inputFile.at[i1, 'to_matching'] = r1['tostation_ratio']
                else:
                    inputFile.at[i1, 'to_matching'] = r1['tostation_partialratio']

        writer = pd.ExcelWriter(r"S:\asset ops\GO_Group\Interns\2019\Anubha\Constraint Project\Data\Transmission Outages Verification\TransmissionOutagesAprroximateStringMatch.xlsx")
        inputFile.to_excel(writer,'Sheet1')
        writer.save()


def main():
    match = StringMatching()
    match.approximatematching()


if __name__ == '__main__':
    main()


