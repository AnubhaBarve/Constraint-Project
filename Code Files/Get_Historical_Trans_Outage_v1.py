'''
-----------------------------------------------------------------------------
This file is to obtain transmission outages from YesEenergy for ISO-NE
-----------------------------------------------------------------------------
Author: Zisheng Chen, Nan Li
Email: nan.li@constellation.com, nanli4@asu.edu 
Creat Date: 12/10/2016
-----------------------------------------------------------------------------
Log:
-----------------------------------------------------------------------------
'''

#packages for connecting to DSAPI
import requests
from io import StringIO
#packages for working with dates and time
from datetime import date,datetime
import datetime as dd
import calendar
import time
#packages for analyzing and manipulating data
import pandas as pd
import numpy as np
#packages for visualizing data

import matplotlib.pyplot as plt

#matplotlibinline
import seaborn as sns
import xlsxwriter
import datetime as dt

my_auth = ('neeraja.dharme@constellation.com','Baltimore@2018')

## a dictionary for input parameterss
param={}

## Regions to get transmission outages
param['regions'] = ['ERCOT']
# regions = ['PJMISO', 'ERCOT', 'MISO', 'NEISO', 'SPPISO', 'CAISO','NYISO']

## Start year
param['styear'] = 2014
## end year
param['endyear'] = dt.datetime.today().year

for reg in param['regions']:

    ## Loop through all the years
    for year in range (param['styear'], param['endyear'] + 1):

        ## Time period to pull transmission outages
        stDate = pd.to_datetime((str(year) + '-01-01')) ## start date
        ## end date
        if year < param['endyear']:        
            enDate = pd.to_datetime((str(year) + '-12-31'))
        else:
            enDate = dt.datetime.today()

        ## Output directory
        ## path of the output file
        param['outputDir'] = 'S:\\asset ops\\GO_Group\\Interns\\2019\\Anubha\\Constraint Project\\Data\\' +\
        reg + '\\'

        outputFile = reg + '_TransmissionOutage_' + \
        str(year) + '-01-01' + '_to_' + \
        enDate.strftime('%Y-%m-%d') + '.xlsx'
        workbook = xlsxwriter.Workbook(param['outputDir'] + outputFile)

        ## URL address for the data
        address = 'https://services.yesenergy.com/PS/rest/transoutage/' + reg + \
        '?facility_type=xfmr,line,BRKR&startDate=' + stDate.strftime('%m/%d/%Y') + \
        '&endDate=' + enDate.strftime('%m/%d/%Y') + \
        '&datetype=start&outagetype=Actual&showrevisions=Y'

        print address
        call_one = requests.get(address, auth=my_auth)
        print type(call_one)
        print type(call_one.text)
        # print call_one.text
        data_one = pd.read_csv(StringIO(call_one.text))
        print data_one.head(5)

        worksheet = workbook.add_worksheet(str(year))

        rawData = data_one.values
        newData = []
        j = 0
        for i in range(rawData.size - 1):

            curRow = np.array_str(rawData[i])
            curRow = curRow[11:-12]
            if i == 0:
                curRow = curRow.split('</th><th>')
                newData.append(curRow)
            else:
                curRow = curRow.split('</td><td>')
                #newData.append(curRow)
                # print curRow

            if curRow[11] != 'Canceled' and curRow[11] != 'Denied':
                for col, value in enumerate(curRow):
                    worksheet.write(j, col, value)
                j += 1

        workbook.close()
