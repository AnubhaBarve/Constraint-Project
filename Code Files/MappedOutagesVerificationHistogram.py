# Created on: August 12,2019
# Created by: Anubha Barve (anubha.barve@constellation.com - Intern 2019)
# Purpose - this file is created to plot the results of verification of mapped transmission
#           outages in the form of a histogram to get a better visualization

# import statements
# ---------------------------------------------------------------------------------------------------------------------------------------------------------------
import pandas as pd
from matplotlib import pyplot as plt
import seaborn as sb

# Code starts
# ----------------------------------------------------------------------------------------------------------------------------------------------------------------

# TransmissionHistogram class containing a function named 'plotMatching' which plots a histogram of the results obtained


class TransmissionHistogram():

    def plotMatching(self):

        # inputFile stores the results of verification of mapped transmission outages in the form of dataframe
        inputFile = pd.read_excel(r"S:\asset ops\GO_Group\Interns\2019\Anubha\Constraint Project\Constraint-Project\Data\Transmission Outages Verification\MappedOutagesVerificationResult.xlsx", sheet_name="Sheet1", index=False)
        # Formatting the column names of excel sheet into one form to make it easier to access column names (all lower case letters with words separated using underscore)
        inputFile.columns = inputFile.columns.str.strip().str.lower().str.replace(' ', '_').str.replace('(','').str.replace(')', '')
        # this line preserves the index and converts it into a column
        inputFile.reset_index(inplace=True, drop=True)

        # plotting histogram for 'from_matching' percentage
        sb.distplot(inputFile['from_matching'], kde=False)
        plt.xlabel('Percentage Range')
        plt.ylabel('Count')
        plt.title('from bus number mapping')
        plt.savefig(r"S:\asset ops\GO_Group\Interns\2019\Anubha\Constraint Project\Constraint-Project\Data\Transmission Outages Verification\from_matching.png")
        plt.show()

        # plotting histogram for 'to_matching' percentage
        sb.distplot(inputFile['to_matching'], kde=False)
        plt.xlabel('Percentage Range')
        plt.ylabel('Count')
        plt.title('to bus number mapping')
        plt.savefig(r"S:\asset ops\GO_Group\Interns\2019\Anubha\Constraint Project\Constraint-Project\Data\Transmission Outages Verification\to_matching.png")
        plt.show()


# main function
# --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
def main():

    # creating object of TransmissionHistogram class
    outages = TransmissionHistogram()
    # call to plotMatching() function
    outages.plotMatching()

# call to main function
# ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
if __name__ == '__main__':
    main()



