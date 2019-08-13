#Created by: Sneha Bharath (Intern 2018)
#Modified by: Anubha Barve (anubha.barve@constellation.com - Intern 2019)
#Modified on: July 30, 2019
#File name: ConstraintContigencyMapping.py
#Purpose: The purpose of this file is, given a list of constraint-contingency pair it explicitly defines the pair in powerworld format.

import pandas as pd

# Class that defines a function named InterfaceMap which separates the Mapped and Unmapped Contingency-Monitored Element stores them in a PowerWorld Format in an excel file.
class ConstraintContingencyMapping():
    def InterfaceMap(self):
        # The uniquePairs variable reads the excel file that contains information of unique constraint-contingency pair which is created using ConstraintContingencyUniquePairCreation.py file.
        uniquePairs = pd.read_excel(r"S:\asset ops\GO_Group\Interns\2019\Anubha\Constraint Project\Data\UniqueConstraintContingencyPair\uniquePairList2015-2018.xlsx", sheet_name="uniquePairList2015-2018", index=False)

        # Formatting the column names of excel sheet into one form to make it easier to access column names (all lower case letters with words separated using underscore)
        uniquePairs.columns = uniquePairs.columns.str.strip().str.lower().str.replace(' ', '_').str.replace('(', '').str.replace(')', '')

        # creating a new DataFrame that is mapped and matches PowerWorld Format
        powerWorldFormatMapped = pd.DataFrame(columns=["Interface Name", "Element", "Meter Far", "Weight", "Contingency Name", "Monitored Element Name"])

        # creating a new DataFrame that is not mapped but has a PowerWorld Format
        powerWorldFormatUnMapped = pd.DataFrame(columns=["Interface Name", "Element", "Meter Far", "Weight", "Contingency Name", "Monitored Element Name", "Absence Of"])

        #Multiple conditions are implemented and accordingly data is stored in the powerWorldFormat DataFrame
        for i in range(0, len(uniquePairs)):

            # check if the Contingency Column for From Bus Number is empty - if yes, store the details in powerWorldFormatMapped DataFrame (Base Case)
            if(uniquePairs.at[i, "contingency_mapped_from_bus_number"]) == " " and (uniquePairs.at[i, "monitored_line_from_bus_number"]) != " ":
                a = uniquePairs.at[i, "interface_name"]
                b = uniquePairs.at[i, "element_b_monitored"]
                c = uniquePairs.at[i, "meter_far"]
                d = uniquePairs.at[i, "weight"]
                f = uniquePairs.at[i, "contingency_name"]
                g = uniquePairs.at[i, "monitored_element_name"]
                powerWorldFormatMapped = powerWorldFormatMapped.append({"Interface Name": a, "Element": b, "Meter Far": c,"Weight": d, "Contingency Name": f, "Monitored Element Name" : g}, ignore_index=True)

            # check if the Monitored Line Column for From Bus Number is empty - if yes, store the details in powerWorldFormatUnMapped DataFrame
            elif(uniquePairs.at[i, "monitored_line_from_bus_number"]) == " " and (uniquePairs.at[i, "contingency_mapped_from_bus_number"]) != " ":
                a = uniquePairs.at[i, "contingency_name"]
                b = uniquePairs.at[i, "monitored_element_name"]
                powerWorldFormatUnMapped = powerWorldFormatUnMapped.append({"Interface Name": "NaN", "Element": "NaN", "Meter Far": "NaN", "Weight": "NaN", "Contingency Name": a, "Monitored Element Name": b, "Absence Of": "Monitored Element"}, ignore_index=True)

            # check if the Monitored Line and Contingency Columns for From Bus Number are empty - if yes, store the details in powerWorldFormatUnMapped DataFrame
            elif(uniquePairs.at[i, "contingency_mapped_from_bus_number"]) == " " and (uniquePairs.at[i, "monitored_line_from_bus_number"]) == " ":
                a = uniquePairs.at[i, "contingency_name"]
                b = uniquePairs.at[i, "monitored_element_name"]
                powerWorldFormatUnMapped = powerWorldFormatUnMapped.append({"Interface Name": "NaN", "Element": "NaN", "Meter Far": "NaN", "Weight": "NaN", "Contingency Name": a, "Monitored Element Name": b, "Absence Of": "Both"}, ignore_index=True)

            # else store the details in powerWorldFormatMapped DataFrame (Constraint-Contingency)
            else:
                a = uniquePairs.at[i, "interface_name"]
                b = uniquePairs.at[i, "element_b_monitored"]
                c = uniquePairs.at[i, "meter_far"]
                d = uniquePairs.at[i, "weight"]
                e = uniquePairs.at[i, "element_a_contingency"]
                f = uniquePairs.at[i, "contingency_name"]
                g = uniquePairs.at[i, "monitored_element_name"]
                powerWorldFormatMapped = powerWorldFormatMapped.append({"Interface Name": a, "Element": b, "Meter Far": c, "Weight": d, "Monitored Element Name": g, "Contingency Name": f}, ignore_index=True)
                powerWorldFormatMapped = powerWorldFormatMapped.append({"Interface Name": a, "Element": e, "Meter Far": c, "Weight": d, "Monitored Element Name": g, "Contingency Name": f}, ignore_index=True)

        # Removing the duplicate Interface Names from the Mapped List
        powerWorldFormatMappedUnique = powerWorldFormatMapped.drop_duplicates(subset=["Interface Name","Element"], keep='first')
        powerWorldFormatMappedUnique = powerWorldFormatMappedUnique.reset_index(drop=True)

        # Removing the duplicate Monitored Element and Contingency Name from the Unmapped List
        powerWorldFormatUnMappedUnique = powerWorldFormatUnMapped.drop_duplicates(subset=["Monitored Element Name", "Contingency Name"], keep='first')
        powerWorldFormatUnMappedUnique = powerWorldFormatUnMappedUnique.reset_index(drop=True)

        # Saving the Mapped and Unmapped List into excel into different sheets
        writer = pd.ExcelWriter(r"S:\asset ops\GO_Group\Interns\2019\Anubha\Constraint Project\Data\PowerWorldFormat2015-2018.xlsx")
        powerWorldFormatMappedUnique.to_excel(writer, 'Sheet1')
        powerWorldFormatUnMappedUnique.to_excel(writer, 'Sheet2')
        writer.save()


def main():
    mapping = ConstraintContingencyMapping()
    mapping.InterfaceMap()


if __name__ == '__main__':
        main()
