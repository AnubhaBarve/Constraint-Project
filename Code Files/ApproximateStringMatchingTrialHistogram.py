# Created on: August 12,2019
# Created by: Anubha Barve (anubha.barve@constellation.com - Intern 2019)
# Purpose - this file is created to plot the the verified transmission outages in the form of a histogram to get a better visualization

import pandas as pd
from matplotlib import pyplot as plt
import  seaborn as sb

class TransmissionHistogram():
    def plotMatching(self):

        inputFile = pd.read_excel(r"S:\asset ops\GO_Group\Interns\2019\Anubha\Constraint Project\Data\Transmission Outages Verification\TransmissionOutagesAprroximateStringMatch.xlsx", sheet_name="Sheet1", index=False)
        # Formatting the column names of excel sheet into one form to make it easier to access column names (all lower case letters with words separated using underscore)
        inputFile.columns = inputFile.columns.str.strip().str.lower().str.replace(' ', '_').str.replace('(','').str.replace(')', '')
        # this line preserves the index and converts it into a column
        inputFile.reset_index(inplace=True, drop=True)


        bins = [0,10,20,30,40,50,60,70,80,90,100]
        #plt.hist(inputFile['from_matching'], bins= bins, histtype='bar', rwidth= 0.8, label='from', color='r',stacked=False)

        #sb.axes_style('white')
        #sb.palplot(sb.color_palette("BrBG", 50))
        #sb.distplot(inputFile['from_matching'], kde=False)
        #plt.hist(inputFile['to_matching'], bins= bins, histtype='bar', rwidth=0.8, label='to', color='b',stacked=False)
        sb.distplot(inputFile['from_matching'], kde=False)
        plt.xlabel('Percentage Range')
        plt.ylabel('Count')
        plt.title('from bus number mapping')

        plt.savefig(r"S:\asset ops\GO_Group\Interns\2019\Anubha\Constraint Project\Data\Transmission Outages Verification\from2.png")

        plt.show()




# main function which calls the class's function named 'concatenate()'
def main():
    outages = TransmissionHistogram()
    outages.plotMatching()


if __name__ == '__main__':
    main()



